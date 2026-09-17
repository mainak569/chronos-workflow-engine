# ============================================================================
# Multi-Stage Dockerfile for Chronos Workflow Service
# ============================================================================
# Stage 1: Build stage with Maven
# Stage 2: Runtime stage with JRE only
# Features: Non-root user, health checks, optimized layers
# ============================================================================

# ============================================================================
# Stage 1: Build
# ============================================================================
FROM maven:3.9.6-eclipse-temurin-21-alpine AS builder

# Set working directory
WORKDIR /build

# Copy pom.xml first to leverage Docker layer caching
COPY pom.xml .

# Download dependencies (cached if pom.xml hasn't changed)
RUN mvn dependency:go-offline -B

# Copy source code
COPY src ./src

# Build the application (skip tests for faster builds - run tests in CI/CD)
RUN mvn clean package -DskipTests -B && \
    # Extract the built JAR name
    mkdir -p target/dependency && \
    cd target/dependency && \
    jar -xf ../*.jar

# ============================================================================
# Stage 2: Runtime
# ============================================================================
FROM eclipse-temurin:21-jre-alpine

# Metadata
LABEL maintainer="chronos-team@example.com"
LABEL description="Chronos Workflow Service - Workflow management and orchestration"
LABEL version="1.0.0"

# Install runtime dependencies
RUN apk add --no-cache \
    curl \
    wget \
    tzdata \
    && rm -rf /var/cache/apk/*

# Create non-root user and group
RUN addgroup -g 1001 chronos && \
    adduser -D -u 1001 -G chronos chronos

# Set working directory
WORKDIR /app

# Copy application from builder stage
COPY --from=builder /build/target/*.jar app.jar

# Create directories for logs and temp files
RUN mkdir -p /app/logs /app/tmp && \
    chown -R chronos:chronos /app

# Switch to non-root user
USER chronos:chronos

# Expose application port and management port
EXPOSE 8081 9081

# Environment variables with sensible defaults
ENV JAVA_OPTS="-Xms256m -Xmx512m -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+UseContainerSupport" \
    SERVER_PORT=8081 \
    MANAGEMENT_PORT=9081 \
    SPRING_PROFILES_ACTIVE=docker \
    TZ=UTC

# Health check configuration
# Checks the Spring Boot Actuator health endpoint
HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD wget --quiet --tries=1 --spider http://localhost:${SERVER_PORT}/actuator/health || exit 1

# Entrypoint with optimized JVM settings
ENTRYPOINT ["sh", "-c", "exec java ${JAVA_OPTS} \
    -Djava.security.egd=file:/dev/./urandom \
    -Dserver.port=${SERVER_PORT} \
    -Dmanagement.server.port=${MANAGEMENT_PORT} \
    -jar app.jar"]

# Documentation
# Build: docker build -t chronos-workflow-service:latest .
# Run: docker run -p 8081:8081 -p 9081:9081 chronos-workflow-service:latest
