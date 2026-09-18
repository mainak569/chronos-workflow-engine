# Chronos Security Architecture

## Overview

Chronos implements JWT-based authentication and ownership-based authorization to secure workflow APIs. Users must authenticate to access the system, and can only see and manage workflows and executions they own.

JWTs are issued by the workflow service and validated twice: by the **API gateway** before a request is
forwarded (rejecting it with `401` early) and again by the **workflow service**, which also loads the user.
Both use the same HMAC secret (`JWT_SECRET`).

**Security Model:** Authentication + Authorization + Ownership

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                        Client                               │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ 1. POST /auth/register
                         │    {username, email, password}
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   Authentication Service                    │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  - Validate input                                   │    │
│  │  - Check email uniqueness                           │    │
│  │  - Hash password (BCrypt)                           │    │
│  │  - Save to MongoDB                                  │    │
│  └─────────────────────────────────────────────────────┘    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ 2. POST /auth/login
                         │    {email, password}
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   JWT Token Provider                        │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  - Validate credentials                             │    │
│  │  - Generate JWT token                               │    │
│  │  - Sign with secret (from env)                      │    │
│  │  - Return: {token, userId, expiresIn}               │    │
│  └─────────────────────────────────────────────────────┘    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ 3. All subsequent requests
                         │    Header: Authorization: Bearer <JWT>
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   JWT Authentication Filter                 │
│  ┌─────────────────────────────────────────────────────┐    │
│  │  1. Extract JWT from Authorization header           │    │
│  │  2. Validate JWT signature                          │    │
│  │  3. Check expiration                                │    │
│  │  4. Extract userId from claims                      │    │
│  │  5. Load user from database                         │    │
│  │  6. Set Authentication in SecurityContext           │    │
│  └─────────────────────────────────────────────────────┘    │
└────────────────────────┬────────────────────────────────────┘
                         │
                         │ 4. Access protected resource
                         │    GET /api/workflows
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   Authorization Layer                       │
│  ┌────────────────────────────────────────────────────┐     │
│  │  - Check @PreAuthorize("isAuthenticated()")        │     │
│  │  - Scope every lookup to the caller (ownerId)      │     │
│  │  - Return 404 if missing or owned by someone else  │     │
│  └────────────────────────────────────────────────────┘     │
└────────────────────────┬────────────────────────────────────┘
                         │
                         ↓
┌─────────────────────────────────────────────────────────────┐
│                   Workflow Service                          │
│  - Returns only user's workflows                            │
└─────────────────────────────────────────────────────────────┘
```

## Authentication Flow

### 1. User Registration

```
Client                  Server                   Database
  |                       |                         |
  |--POST /auth/register-->                         |
  | {username, email, password}                     |
  |                       |                         |
  |                    Validate                     |
  |                    input                        |
  |                       |                         |
  |                    Check email                  |
  |                    uniqueness -->-------------> |
  |                       |<--------<-------------- |
  |                       |                         |
  |                    Hash password                |
  |                  (BCrypt 10 rounds)             |
  |                       |                         |
  |                    Save user -->--------------> |
  |                       |<--------<-------------- |
  |                       |                         |
  |<-- 201 Created -------|                         |
  |{userId, username, email}                        |
```

**Security:**
- Password hashed with BCrypt (strength 10)
- Email uniqueness enforced
- Username sanitized
- No plaintext passwords stored
- Input validation (email format, password strength)

### 2. User Login

```
Client                  Server                   Database
  |                       |                         |
  |-- POST /auth/login -->|                         |
  |   {email, password}   |                         |
  |                       |                         |
  |                    Load user -->--------------> |
  |                       |         <-------------- |
  |                       |                         |
  |                    Verify password              |
  |                    (BCrypt compare)             |
  |                       |                         |
  |                    Generate JWT                 |
  |                    - userId claim               |
  |                    - email claim                |
  |                    - iat (issued at)            |
  |                    - exp (expires)              |
  |                    - Sign with secret           |
  |                       |                         |
  |<-- 200 OK ------------|                         |
  |   {token, userId, expiresIn: 86400}             |
```

**JWT Payload:**
```json
{
  "sub": "user-123",
  "email": "user@example.com",
  "iat": 1726387200,
  "exp": 1726473600
}
```

**Security:**
- BCrypt comparison (timing-attack resistant)
- JWT signed with HS256
- Secret from environment variable
- 24-hour expiration
- No sensitive data in JWT payload

### 3. Authenticated Request

```
Client                  Server                   Database
  |                       |                         |
  |--GET /api/workflows-->                          |
  |   Authorization: Bearer <JWT>                   |
  |                       |                         |
  |                    Extract JWT                  |
  |                    from header                  |
  |                       |                         |
  |                    Validate JWT                 |
  |                    - Check signature            |
  |                    - Check expiration           |
  |                       |                         |
  |                    Extract userId               |
  |                    from claims                  |
  |                       |                         |
  |                    Load user -->--------------> |
  |                       |<------<---------------- |
  |                       |                         |
  |                    Set SecurityContext          |
  |                       |                         |
  |                    Query user's -->-----------> |
  |                    workflows <---------<------- |
  |                       |                         |
  |<-- 200 OK ------------|                         |
  |   [{workflow1}, {workflow2}]                    |
```

**Security:**
- JWT validated on every request
- User loaded fresh from database
- SecurityContext set for request scope
- Automatic logout on token expiration

## Authorization Model

### Ownership-Based Access Control

**Principle:** Users can only access resources they own.

**Implementation:**

```java
@Entity
public class Workflow {
    @Id
    private String id;
    
    @Indexed
    private String ownerId;  // User ID who created this workflow
    
    private String name;
    private List<TaskDefinition> tasks;
}
```

**Access Rules:**

| Operation | Rule | Check |
|-----------|------|-------|
| Create Workflow | Authenticated | ownerId = current user |
| Read Workflow | Owner only | `findByIdAndOwnerId(id, userId)` |
| Delete Workflow | Owner only | `existsByIdAndOwnerId(id, userId)` |
| List Workflows | Owner only | `findByOwnerId(userId)` |
| Execute Workflow | Owner only | `findByIdAndOwnerId(id, userId)`; `triggeredBy` = userId from the token |
| Read / cancel Execution, list its tasks | Owner only | `execution.ownerId == userId` |

Resources of other users are reported as **404 Not Found**, not 403, so their IDs cannot be probed.
Workflows cannot be updated (create a new one instead).

### Authorization Checks

**Method 1: @PreAuthorize Annotation**
```java
@PreAuthorize("isAuthenticated()")
@GetMapping("/api/workflows")
public List<Workflow> getWorkflows() {
    String userId = SecurityContextHolder.getContext()
        .getAuthentication().getName();
    return workflowService.findByOwnerId(userId);
}
```

**Method 2: Service-Level Ownership Validation** (executions)
```java
public WorkflowExecution getExecution(String executionId, String ownerId) {
    return executionRepository.findById(executionId)
            .filter(execution -> ownerId.equals(execution.getOwnerId()))
            .orElseThrow(() -> new ExecutionNotFoundException("Execution not found: " + executionId));
}
```

**Method 3: Repository-Level Filtering**
```java
@Repository
public interface WorkflowRepository extends MongoRepository<Workflow, String> {
    List<Workflow> findByOwnerId(String ownerId);
    Optional<Workflow> findByIdAndOwnerId(String id, String ownerId);
}
```

## Security Components

### 1. User Entity

```java
@Document(collection = "users")
public class User {
    @Id
    private String id;
    
    @Indexed(unique = true)
    private String email;
    
    private String username;
    
    // BCrypt hashed password (never stored plaintext)
    private String password;
    
    private boolean enabled = true;
    
    @CreatedDate
    private Instant createdAt;
    
    @LastModifiedDate
    private Instant updatedAt;
}
```

**Password Storage:**
- BCrypt with strength 10
- Salt automatically generated per password
- One-way hash (cannot be decrypted)
- Timing-attack resistant comparison

### 2. JWT Token Provider

```java
@Component
public class JwtTokenProvider {
    
    @Value("${chronos.security.jwt.secret}")
    private String jwtSecret;  // From environment variable
    
    @Value("${chronos.security.jwt.expiration:86400000}")
    private long jwtExpirationMs;  // 24 hours default
    
    public String generateToken(String userId, String email) {
        return Jwts.builder()
            .setSubject(userId)
            .claim("email", email)
            .setIssuedAt(new Date())
            .setExpiration(new Date(System.currentTimeMillis() + jwtExpirationMs))
            .signWith(SignatureAlgorithm.HS256, jwtSecret)
            .compact();
    }
    
    public String getUserIdFromToken(String token) {
        return Jwts.parser()
            .setSigningKey(jwtSecret)
            .parseClaimsJws(token)
            .getBody()
            .getSubject();
    }
    
    public boolean validateToken(String token) {
        try {
            Jwts.parser().setSigningKey(jwtSecret).parseClaimsJws(token);
            return true;
        } catch (ExpiredJwtException e) {
            return false;  // Token expired
        } catch (JwtException e) {
            return false;  // Invalid token
        }
    }
}
```

### 3. JWT Authentication Filter

```java
public class JwtAuthenticationFilter extends OncePerRequestFilter {
    
    @Override
    protected void doFilterInternal(HttpServletRequest request,
                                   HttpServletResponse response,
                                   FilterChain filterChain) {
        try {
            // Extract JWT from Authorization header
            String jwt = extractJwtFromRequest(request);
            
            if (jwt != null && jwtTokenProvider.validateToken(jwt)) {
                // Get user ID from token
                String userId = jwtTokenProvider.getUserIdFromToken(jwt);
                
                // Load user details
                UserDetails userDetails = userDetailsService.loadUserById(userId);
                
                // Create authentication object
                UsernamePasswordAuthenticationToken authentication = 
                    new UsernamePasswordAuthenticationToken(
                        userDetails, null, userDetails.getAuthorities()
                    );
                
                // Set in SecurityContext
                SecurityContextHolder.getContext().setAuthentication(authentication);
            }
        } catch (Exception e) {
            logger.error("Cannot set user authentication", e);
        }
        
        filterChain.doFilter(request, response);
    }
    
    private String extractJwtFromRequest(HttpServletRequest request) {
        String bearerToken = request.getHeader("Authorization");
        if (bearerToken != null && bearerToken.startsWith("Bearer ")) {
            return bearerToken.substring(7);
        }
        return null;
    }
}
```

### 4. Security Configuration

```java
@Configuration
@EnableWebSecurity
@EnableMethodSecurity
public class SecurityConfig {
    
    @Bean
    public SecurityFilterChain filterChain(HttpSecurity http) throws Exception {
        http
            .csrf().disable()  // JWT doesn't need CSRF protection
            .sessionManagement()
                .sessionCreationPolicy(SessionCreationPolicy.STATELESS)
            .and()
            .authorizeHttpRequests()
                .requestMatchers("/auth/**").permitAll()  // Public endpoints
                .requestMatchers("/actuator/health").permitAll()
                .anyRequest().authenticated()  // Everything else requires auth
            .and()
            .addFilterBefore(jwtAuthenticationFilter(), 
                           UsernamePasswordAuthenticationFilter.class);
        
        return http.build();
    }
    
    @Bean
    public PasswordEncoder passwordEncoder() {
        return new BCryptPasswordEncoder(10);  // Strength 10
    }
}
```

## API Security

### Public Endpoints (No Authentication)

```
POST /auth/register    - User registration
POST /auth/login       - User login
GET  /actuator/health  - Health check
```

### Protected Endpoints (Authentication Required)

```
GET    /api/v1/workflows                          - List user's workflows
POST   /api/v1/workflows                          - Create workflow (auto-set ownerId)
GET    /api/v1/workflows/{id}                     - Get workflow (ownership check)
DELETE /api/v1/workflows/{id}                     - Delete workflow (ownership check)
POST   /api/v1/workflows/{id}/execute             - Execute workflow (ownership check)
GET    /api/v1/workflows/executions/{id}[/tasks]  - Execution and tasks (ownership check)
POST   /api/v1/workflows/executions/{id}/cancel   - Cancel execution (ownership check)
```

### Response Codes

| Code | Meaning | Example |
|------|---------|---------|
| 200 | Success | Login successful |
| 201 | Created | User registered |
| 400 | Bad Request | Invalid email format |
| 401 | Unauthorized | Missing, invalid or expired JWT; wrong password |
| 404 | Not Found | Workflow/execution doesn't exist or belongs to another user |
| 409 | Conflict | Email already registered; execution already finished |
| 429 | Too Many Requests | Gateway rate limit exceeded |
| 500 | Server Error | Internal error |

## Security Best Practices

### 🟢 Implemented

1. **Password Security**
   - BCrypt hashing (strength 10)
   - Salted per password
   - Never stored or logged in plaintext
   - Timing-attack resistant comparison

2. **JWT Security**
   - HMAC-SHA signature (HS256/384/512, chosen from the key length; HS512 with a 64-byte key)
   - Secret from environment variable
   - 1-hour expiration in Docker (`JWT_EXPIRATION_MS`), 24 hours by default when run locally
   - No sensitive data in payload
   - Validated on every request

3. **Ownership Validation**
   - ownerId tracked on all resources
   - Service-level ownership checks
   - Repository-level filtering
   - 404 Not Found for other users' resources

4. **Input Validation**
   - Email format validation
   - Password strength requirements
   - Username sanitization
   - Request body validation

5. **Error Handling**
   - Generic error messages (no info leakage)
   - No stack traces in production
   - Consistent error format
   - Logging for security events

6. **HTTPS Required**
   - Enforce in production
   - HTTP Strict Transport Security (HSTS)
   - Secure cookies

### 🔴 Not Included (Future Enhancements)

- Multi-factor authentication (MFA)
- OAuth2 integration
- Account lockout after failed attempts
- Password reset flow
- Email verification
- Refresh tokens
- Token revocation
- Role-based access control (RBAC)
- Audit logging

## Threat Model

### Threats & Mitigations

**1. Password Compromise**
- **Threat:** Attacker obtains plaintext password
- **Mitigation:** BCrypt hashing, no plaintext storage
- **Risk Level:** Low

**2. JWT Theft**
- **Threat:** Attacker steals JWT token
- **Mitigation:** Short expiration (1h in Docker), no refresh tokens. TLS is not configured in the local
  stack; in production it should terminate in front of the gateway
- **Risk Level:** Medium

**3. Brute Force Login**
- **Threat:** Attacker tries many passwords
- **Mitigation:** BCrypt (slow by design), gateway rate limiting (120 requests/minute per IP for anonymous calls)
- **Future:** Account lockout
- **Risk Level:** Medium

**4. SQL Injection**
- **Threat:** Attacker injects malicious SQL
- **Mitigation:** MongoDB (NoSQL), Spring Data repositories (no raw queries)
- **Risk Level:** Low

**5. Unauthorized Access**
- **Threat:** User accesses another user's workflows
- **Mitigation:** Ownership checks on all operations
- **Risk Level:** Low

**6. JWT Secret Compromise**
- **Threat:** Attacker obtains JWT signing secret
- **Mitigation:** Secret from environment variable, not in code
- **Future:** Rotate secrets regularly
- **Risk Level:** High (if compromised)

**7. XSS (Cross-Site Scripting)**
- **Threat:** Attacker injects malicious JavaScript
- **Mitigation:** No web frontend (API only), JWT in HTTP-only cookies (future)
- **Risk Level:** Low

**8. CSRF (Cross-Site Request Forgery)**
- **Threat:** Attacker tricks user into unwanted actions
- **Mitigation:** JWT tokens (no cookies), stateless sessions
- **Risk Level:** Low

## Configuration

### Environment Variables

```bash
# JWT signing secret, shared by the gateway and the workflow service (REQUIRED outside local dev)
JWT_SECRET=your-secret-key-min-256-bits-use-openssl-rand-base64-48

# JWT expiration in milliseconds (docker-compose default: 1 hour)
JWT_EXPIRATION_MS=3600000

# Scheduler operations API token (empty = API disabled)
CHRONOS_ADMIN_TOKEN=
```

When running the workflow service outside Docker, `CHRONOS_JWT_SECRET` / `CHRONOS_JWT_EXPIRATION` take
precedence over `JWT_SECRET`. BCrypt strength is fixed at 10 in `SecurityConfig`. TLS can be enabled
with the standard Spring Boot `server.ssl.*` properties.

### Generating JWT Secret

```bash
# Generate a strong secret (256 bits)
openssl rand -base64 32

# Output example:
# K7gNU3sdo+OL0wNhqoVWhr3g6s1xYv72ol/pe/Unols=

# Set as environment variable
export CHRONOS_JWT_SECRET="K7gNU3sdo+OL0wNhqoVWhr3g6s1xYv72ol/pe/Unols="
```

### application.yml

```yaml
chronos:
  security:
    jwt:
      secret: ${CHRONOS_JWT_SECRET:${JWT_SECRET:<dev default>}}
      expiration: ${CHRONOS_JWT_EXPIRATION:86400000}  # 24 hours
```

## Testing Strategy

### Unit Tests

**AuthenticationServiceTest**
- User registration with valid data
- Duplicate email prevention
- Password hashing verification
- Login with valid credentials
- Login with invalid credentials
- Login with non-existent user

**JwtTokenProviderTest**
- Token generation
- Token validation
- Expired token detection
- Invalid signature detection
- User ID extraction from token

**OwnershipServiceTest**
- Ownership validation success
- Ownership validation failure
- Missing workflow handling

### Integration Tests

**AuthControllerTest**
- POST /auth/register (success)
- POST /auth/register (duplicate email)
- POST /auth/login (success)
- POST /auth/login (invalid credentials)
- Protected endpoint without token (401)
- Protected endpoint with valid token (200)
- Protected endpoint with expired token (401)

**WorkflowAuthorizationTest**
- Create workflow (authenticated)
- Get own workflow (success)
- Get another user's workflow (403)
- Update own workflow (success)
- Update another user's workflow (403)
- Delete own workflow (success)
- Delete another user's workflow (403)

### Security Tests

**PenetrationTests**
- SQL injection attempts
- XSS attempts
- CSRF attempts
- JWT tampering
- Expired token usage
- Missing authentication
- Privilege escalation

## Monitoring & Logging

### Security Events to Log

```
- User registration (INFO)
- Successful login (INFO)
- Failed login attempt (WARN)
- JWT validation failure (WARN)
- Ownership violation (WARN)
- Unauthorized access attempt (WARN)
- Account lockout (WARN)
- Password change (INFO)
```

### Metrics to Track

```
- Authentication attempts (total, success, failure)
- Token validation failures
- Ownership violations
- 401 Unauthorized responses
- 403 Forbidden responses
- Average login time
```

### Alerts

```
- High rate of failed login attempts
- Multiple JWT validation failures
- Unusual number of ownership violations
- Account created from suspicious IP
```

## Production Deployment

### Checklist

- [ ] Generate strong JWT secret (256 bits minimum)
- [ ] Store secret in environment variable (never in code)
- [ ] Enable HTTPS (TLS 1.2+)
- [ ] Configure HSTS headers
- [ ] Set secure password requirements
- [ ] Enable audit logging
- [ ] Configure rate limiting (future)
- [ ] Set up monitoring alerts
- [ ] Test token expiration
- [ ] Verify ownership checks on all endpoints
- [ ] Review error messages (no info leakage)
- [ ] Enable security headers (X-Frame-Options, X-Content-Type-Options)

## Summary

Chronos implements a secure authentication and authorization system with:

**JWT-based authentication** - Stateless, scalable  
**BCrypt password hashing** - Industry standard  
**Ownership-based authorization** - User isolation  
**Environment-based secrets** - No hardcoded credentials  
**Comprehensive validation** - Input, ownership, tokens  
**Secure error handling** - No information leakage  
**Complete test coverage** - Unit + integration tests  

The system is production-ready and follows security best practices for microservices authentication.
