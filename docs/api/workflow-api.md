# Workflow Management API

## Overview

The Workflow Management API provides endpoints for creating, retrieving, and managing workflow definitions in the Chronos orchestration engine.

**Base URL:** `http://localhost:8081/api/v1`

**Authentication:** JWT Bearer Token (to be implemented)

---

## Endpoints

### 1. Create Workflow

Creates a new workflow definition.

**Endpoint:** `POST /workflows`

**Request Headers:**
```
Content-Type: application/json
Authorization: Bearer <jwt_token>  (to be implemented)
```

**Request Body:**
```json
{
  "name": "Image Processing Pipeline",
  "description": "Workflow for resizing and optimizing images",
  "tasks": [
    {
      "taskId": "download-image",
      "name": "Download Image",
      "taskType": "HTTP_DOWNLOAD",
      "dependencies": [],
      "configuration": {
        "url": "${input.imageUrl}",
        "destination": "/tmp/original.jpg"
      },
      "retryConfig": {
        "maxAttempts": 3,
        "initialDelayMs": 5000,
        "backoffMultiplier": 2.0,
        "maxDelayMs": 300000
      },
      "timeoutMs": 60000,
      "description": "Downloads the source image"
    },
    {
      "taskId": "resize-image",
      "name": "Resize Image",
      "taskType": "IMAGE_RESIZE",
      "dependencies": ["download-image"],
      "configuration": {
        "width": 800,
        "height": 600,
        "maintainAspectRatio": true
      },
      "retryConfig": {
        "maxAttempts": 3,
        "initialDelayMs": 5000,
        "backoffMultiplier": 2.0,
        "maxDelayMs": 300000
      },
      "timeoutMs": 120000,
      "description": "Resizes the image to specified dimensions"
    },
    {
      "taskId": "upload-result",
      "name": "Upload Result",
      "taskType": "S3_UPLOAD",
      "dependencies": ["resize-image"],
      "configuration": {
        "bucket": "processed-images",
        "key": "${input.outputKey}"
      },
      "retryConfig": {
        "maxAttempts": 5,
        "initialDelayMs": 5000,
        "backoffMultiplier": 2.0,
        "maxDelayMs": 300000
      },
      "timeoutMs": 60000,
      "description": "Uploads the processed image to S3"
    }
  ]
}
```

**Response:** `201 Created`
```json
{
  "workflowId": "65f1b2c3d4e5f6a7b8c9d0e1",
  "ownerId": "user-123",
  "name": "Image Processing Pipeline",
  "description": "Workflow for resizing and optimizing images",
  "tasks": [ /* same as request */ ],
  "createdAt": "2026-09-15T13:30:00Z",
  "updatedAt": "2026-09-15T13:30:00Z"
}
```

**Validation Rules:**
- `name`: Required, non-blank
- `tasks`: Required, at least one task
- `taskId`: Required, unique within workflow
- `name` (task): Required, non-blank
- `taskType`: Required, non-blank
- `dependencies`: Must reference existing task IDs
- No cyclic dependencies allowed
- `retryConfig.maxAttempts`: ≥ 0
- `retryConfig.initialDelayMs`: ≥ 0
- `retryConfig.backoffMultiplier`: ≥ 1.0
- `retryConfig.maxDelayMs`: ≥ 0
- `timeoutMs`: > 0

**Error Responses:**

`400 Bad Request` - Validation errors:
```json
{
  "timestamp": "2026-09-15T13:30:00Z",
  "status": 400,
  "error": "VALIDATION_ERROR",
  "message": "Validation failed for request",
  "path": "/api/v1/workflows",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "fieldErrors": [
    {
      "field": "name",
      "message": "Workflow name is required",
      "rejectedValue": null
    }
  ]
}
```

`400 Bad Request` - Workflow validation errors:
```json
{
  "timestamp": "2026-09-15T13:30:00Z",
  "status": 400,
  "error": "WORKFLOW_VALIDATION_ERROR",
  "message": "Cyclic dependency detected involving task: resize-image",
  "path": "/api/v1/workflows",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

### 2. Get Workflow by ID

Retrieves a specific workflow by its ID.

**Endpoint:** `GET /workflows/{workflowId}`

**Path Parameters:**
- `workflowId`: Workflow identifier

**Request Headers:**
```
Authorization: Bearer <jwt_token>  (to be implemented)
```

**Response:** `200 OK`
```json
{
  "workflowId": "65f1b2c3d4e5f6a7b8c9d0e1",
  "ownerId": "user-123",
  "name": "Image Processing Pipeline",
  "description": "Workflow for resizing and optimizing images",
  "tasks": [ /* task definitions */ ],
  "createdAt": "2026-09-15T13:30:00Z",
  "updatedAt": "2026-09-15T13:30:00Z"
}
```

**Error Responses:**

`404 Not Found` - Workflow not found or not owned by user:
```json
{
  "timestamp": "2026-09-15T13:30:00Z",
  "status": 404,
  "error": "WORKFLOW_NOT_FOUND",
  "message": "Workflow not found with ID: 65f1b2c3d4e5f6a7b8c9d0e1",
  "path": "/api/v1/workflows/65f1b2c3d4e5f6a7b8c9d0e1",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

### 3. Get All Workflows for User

Retrieves all workflows owned by the authenticated user.

**Endpoint:** `GET /workflows`

**Request Headers:**
```
Authorization: Bearer <jwt_token>  (to be implemented)
```

**Response:** `200 OK`
```json
[
  {
    "workflowId": "65f1b2c3d4e5f6a7b8c9d0e1",
    "ownerId": "user-123",
    "name": "Image Processing Pipeline",
    "description": "Workflow for resizing and optimizing images",
    "tasks": [ /* task definitions */ ],
    "createdAt": "2026-09-15T13:30:00Z",
    "updatedAt": "2026-09-15T13:30:00Z"
  },
  {
    "workflowId": "75f1b2c3d4e5f6a7b8c9d0e2",
    "ownerId": "user-123",
    "name": "Data ETL Pipeline",
    "description": "Extract, transform, and load data",
    "tasks": [ /* task definitions */ ],
    "createdAt": "2026-09-14T10:00:00Z",
    "updatedAt": "2026-09-14T10:00:00Z"
  }
]
```

**Note:** Returns an empty array if no workflows are found.

---

### 4. Delete Workflow

Deletes a workflow by its ID.

**Endpoint:** `DELETE /workflows/{workflowId}`

**Path Parameters:**
- `workflowId`: Workflow identifier

**Request Headers:**
```
Authorization: Bearer <jwt_token>  (to be implemented)
```

**Response:** `204 No Content`

**Error Responses:**

`404 Not Found` - Workflow not found or not owned by user:
```json
{
  "timestamp": "2026-09-15T13:30:00Z",
  "status": 404,
  "error": "WORKFLOW_NOT_FOUND",
  "message": "Workflow not found with ID: 65f1b2c3d4e5f6a7b8c9d0e1",
  "path": "/api/v1/workflows/65f1b2c3d4e5f6a7b8c9d0e1",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000"
}
```

---

## Data Models

### Workflow

| Field | Type | Description |
|-------|------|-------------|
| workflowId | string | Unique identifier (MongoDB ObjectId) |
| ownerId | string | User ID of workflow owner |
| name | string | Human-readable workflow name |
| description | string (optional) | Workflow description |
| tasks | array[TaskDefinition] | List of task definitions |
| createdAt | timestamp (ISO 8601) | Creation timestamp |
| updatedAt | timestamp (ISO 8601) | Last modification timestamp |

### TaskDefinition

| Field | Type | Description |
|-------|------|-------------|
| taskId | string | Unique identifier within workflow |
| name | string | Human-readable task name |
| taskType | string | Task type (determines worker capability) |
| dependencies | array[string] | Task IDs that must complete first |
| configuration | object | Task-specific configuration |
| retryConfig | RetryConfiguration | Retry behavior |
| timeoutMs | long | Maximum execution time (milliseconds) |
| description | string (optional) | Task description |

### RetryConfiguration

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| maxAttempts | integer | 3 | Maximum retry attempts (0 = no retries) |
| initialDelayMs | long | 5000 | Initial delay before first retry (ms) |
| backoffMultiplier | double | 2.0 | Exponential backoff multiplier |
| maxDelayMs | long | 300000 | Maximum delay cap (ms) |

**Delay Calculation:** `delay = min(initialDelayMs * (backoffMultiplier ^ (attempt - 1)), maxDelayMs)`

---

## Error Handling

All error responses follow a consistent structure:

```json
{
  "timestamp": "2026-09-15T13:30:00Z",
  "status": 400,
  "error": "ERROR_CODE",
  "message": "Human-readable error message",
  "path": "/api/v1/workflows",
  "correlationId": "550e8400-e29b-41d4-a716-446655440000",
  "fieldErrors": [/* optional, only for validation errors */]
}
```

### Error Codes

| Code | HTTP Status | Description |
|------|-------------|-------------|
| WORKFLOW_NOT_FOUND | 404 | Workflow doesn't exist or not owned by user |
| WORKFLOW_VALIDATION_ERROR | 400 | Invalid workflow structure (cycles, invalid dependencies) |
| VALIDATION_ERROR | 400 | Request validation failed (missing required fields, invalid values) |
| INTERNAL_SERVER_ERROR | 500 | Unexpected server error |

**Correlation ID:** Each error includes a unique correlation ID for tracking and debugging purposes. Include this ID when reporting issues.

---

## Examples

### Example 1: Simple Sequential Workflow

```bash
curl -X POST http://localhost:8081/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Sequential Pipeline",
    "description": "Three tasks running in sequence",
    "tasks": [
      {
        "taskId": "task-1",
        "name": "First Task",
        "taskType": "STEP_1",
        "dependencies": [],
        "configuration": {"param": "value1"},
        "retryConfig": {
          "maxAttempts": 3,
          "initialDelayMs": 5000,
          "backoffMultiplier": 2.0,
          "maxDelayMs": 300000
        },
        "timeoutMs": 60000
      },
      {
        "taskId": "task-2",
        "name": "Second Task",
        "taskType": "STEP_2",
        "dependencies": ["task-1"],
        "configuration": {"param": "value2"},
        "retryConfig": {
          "maxAttempts": 3,
          "initialDelayMs": 5000,
          "backoffMultiplier": 2.0,
          "maxDelayMs": 300000
        },
        "timeoutMs": 60000
      },
      {
        "taskId": "task-3",
        "name": "Third Task",
        "taskType": "STEP_3",
        "dependencies": ["task-2"],
        "configuration": {"param": "value3"},
        "retryConfig": {
          "maxAttempts": 3,
          "initialDelayMs": 5000,
          "backoffMultiplier": 2.0,
          "maxDelayMs": 300000
        },
        "timeoutMs": 60000
      }
    ]
  }'
```

### Example 2: Parallel Tasks with Join

```bash
curl -X POST http://localhost:8081/api/v1/workflows \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Parallel Processing",
    "description": "Two parallel tasks followed by a join",
    "tasks": [
      {
        "taskId": "parallel-1",
        "name": "Parallel Task 1",
        "taskType": "PROCESS_A",
        "dependencies": [],
        "configuration": {},
        "retryConfig": {
          "maxAttempts": 3,
          "initialDelayMs": 5000,
          "backoffMultiplier": 2.0,
          "maxDelayMs": 300000
        },
        "timeoutMs": 60000
      },
      {
        "taskId": "parallel-2",
        "name": "Parallel Task 2",
        "taskType": "PROCESS_B",
        "dependencies": [],
        "configuration": {},
        "retryConfig": {
          "maxAttempts": 3,
          "initialDelayMs": 5000,
          "backoffMultiplier": 2.0,
          "maxDelayMs": 300000
        },
        "timeoutMs": 60000
      },
      {
        "taskId": "join-task",
        "name": "Join Results",
        "taskType": "MERGE",
        "dependencies": ["parallel-1", "parallel-2"],
        "configuration": {},
        "retryConfig": {
          "maxAttempts": 3,
          "initialDelayMs": 5000,
          "backoffMultiplier": 2.0,
          "maxDelayMs": 300000
        },
        "timeoutMs": 60000
      }
    ]
  }'
```

### Example 3: Get All Workflows

```bash
curl -X GET http://localhost:8081/api/v1/workflows
```

### Example 4: Get Specific Workflow

```bash
curl -X GET http://localhost:8081/api/v1/workflows/65f1b2c3d4e5f6a7b8c9d0e1
```

### Example 5: Delete Workflow

```bash
curl -X DELETE http://localhost:8081/api/v1/workflows/65f1b2c3d4e5f6a7b8c9d0e1
```

---

## Notes

### Current Limitations

1. **Authentication:** Currently using hardcoded `ownerId = "user-123"`. JWT-based authentication will be implemented in a future phase.

2. **Workflow Execution:** This API only manages workflow definitions. Actual workflow execution (triggering, scheduling, status tracking) will be implemented in subsequent phases.

3. **Updates:** Workflow updates are not yet supported. Create a new workflow or delete and recreate.

4. **Pagination:** The GET /workflows endpoint returns all workflows. Pagination will be added when needed.

### Validation Details

**Workflow Validation Process:**
1. Validate request structure (JSON, required fields)
2. Validate task uniqueness (no duplicate task IDs)
3. Validate dependencies (all referenced tasks exist)
4. Detect cyclic dependencies using depth-first search (DFS)

**Cycle Detection:** The system prevents workflows with circular dependencies (e.g., Task A depends on Task B, Task B depends on Task C, Task C depends on Task A).

### MongoDB Indexes

The following indexes are automatically created:
- `ownerId`: For efficient user workflow lookups
- `createdAt`: For time-based queries and sorting

---

## Testing

Unit tests and integration tests are available in:
- `src/test/java/com/chronos/workflow/validation/WorkflowValidatorTest.java` - 9 tests for validation logic
- `src/test/java/com/chronos/workflow/service/WorkflowServiceTest.java` - 6 tests for service operations
- `src/test/java/com/chronos/workflow/repository/WorkflowRepositoryIntegrationTest.java` - Integration tests with MongoDB (requires Docker)

Run tests:
```bash
# Unit tests only (no Docker required)
mvn test -Dtest='**/*Test,!**/*IntegrationTest'

# All tests (requires Docker for MongoDB Testcontainers)
mvn test
```

---

## Changelog

### Version 1.0.0 - 2026-09-15

**Initial Release**
- POST /workflows - Create workflow
- GET /workflows/{workflowId} - Get workflow by ID
- GET /workflows - List user workflows
- DELETE /workflows/{workflowId} - Delete workflow
- Workflow validation (uniqueness, dependencies, cycles)
- Structured error responses with correlation IDs
- MongoDB persistence with indexes
