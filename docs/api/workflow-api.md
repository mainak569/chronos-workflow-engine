# Chronos REST API

All examples below were captured from a running Chronos stack.

**Base URL:** `http://localhost:8080/api/v1` (API gateway). The workflow service also serves the same
API directly on `http://localhost:8081/api/v1`.

**Authentication:** every endpoint except `/auth/**` needs `Authorization: Bearer <token>`. Tokens come
from register or login. Users only see their own workflows and executions: another user's resource
is reported as `404 Not Found`.

**Correlation IDs:** send `X-Correlation-ID` to trace a request; the gateway generates one otherwise
and returns it in the response header and in error bodies.

**Rate limiting:** the gateway allows 120 requests per minute per user (per IP for anonymous calls)
and answers `429 Too Many Requests` with a `Retry-After` header beyond that.

A Postman collection with every request is in
[`postman/chronos-api.postman_collection.json`](../../postman/chronos-api.postman_collection.json).

---

## Authentication

### Register — `POST /auth/register`

```json
{ "email": "jane@example.com", "username": "jane", "password": "SecurePass123!" }
```

`201 Created`
```json
{
  "token": "eyJhbGciOiJIUzUxMiJ9...",
  "tokenType": "Bearer",
  "userId": "6aacf2321a704a0127be73bb",
  "email": "jane@example.com",
  "username": "jane",
  "expiresIn": 3600000
}
```

`expiresIn` is in milliseconds. Errors: `400` (invalid email, username 3–50 characters,
password 8–100 characters), `409 DUPLICATE_RESOURCE` (email already registered).

### Login — `POST /auth/login`

```json
{ "email": "jane@example.com", "password": "SecurePass123!" }
```

`200 OK` with the same body as register. Wrong credentials: `401 AUTHENTICATION_FAILED`.

---

## Workflows

### Create workflow — `POST /workflows`

```json
{
  "name": "Image Processing Pipeline",
  "description": "Resize, compress and upload an image",
  "tasks": [
    {
      "taskId": "resize",
      "name": "Resize image",
      "taskType": "IMAGE_RESIZE",
      "configuration": { "width": 1280, "height": 720 }
    },
    {
      "taskId": "compress",
      "name": "Compress image",
      "taskType": "IMAGE_COMPRESS",
      "dependencies": ["resize"],
      "configuration": { "quality": 85 },
      "retryConfig": { "maxAttempts": 3, "initialDelayMs": 2000, "backoffMultiplier": 2.0, "maxDelayMs": 60000 },
      "timeoutMs": 60000
    },
    {
      "taskId": "validate",
      "name": "Validate output",
      "taskType": "DATA_VALIDATION",
      "dependencies": ["compress"]
    }
  ]
}
```

`201 Created`
```json
{
  "workflowId": "6aacf2321a704a0127be73bc",
  "ownerId": "6aacf2321a704a0127be73bb",
  "name": "Image Processing Pipeline",
  "description": "Resize, compress and upload an image",
  "tasks": [
    {
      "taskId": "resize",
      "name": "Resize image",
      "taskType": "IMAGE_RESIZE",
      "dependencies": [],
      "configuration": { "width": 1280, "height": 720 },
      "retryConfig": { "maxAttempts": 3, "initialDelayMs": 5000, "backoffMultiplier": 2.0, "maxDelayMs": 300000 },
      "timeoutMs": 300000,
      "description": null
    }
  ],
  "schedule": null,
  "timezone": null,
  "createdAt": "2026-09-18T08:11:30.189Z",
  "updatedAt": "2026-09-18T08:11:30.189Z"
}
```

**Scheduled workflows:** add a Spring cron expression (6 fields: second minute hour day month weekday)
and optionally a time zone (default UTC). The scheduler then starts executions automatically:

```json
{ "name": "Nightly report", "schedule": "0 0 2 * * *", "timezone": "Asia/Kolkata", "tasks": [ ... ] }
```

**Validation** (`400`):
- `name` required; at least one task
- each task: `taskId` (unique), `name`, `taskType` required
- `dependencies` must reference task IDs of the same workflow; cycles are rejected
- `retryConfig`: `maxAttempts ≥ 0`, `initialDelayMs ≥ 0`, `backoffMultiplier ≥ 1`, `maxDelayMs ≥ 0`
- `timeoutMs > 0` (default 300000)
- `schedule` must be a valid cron expression, `timezone` a valid zone ID

Supported task types (configured on workers): `IMAGE_RESIZE`, `IMAGE_COMPRESS`, `DATA_PROCESSING`,
`DATA_VALIDATION` (plus `FILE_CONVERSION`, `NOTIFICATION` in Docker).

### List workflows — `GET /workflows`

`200 OK` — array of workflow objects owned by the caller.

### Get workflow — `GET /workflows/{workflowId}`

`200 OK` — workflow object. `404 WORKFLOW_NOT_FOUND` if it does not exist or belongs to someone else.

### Delete workflow — `DELETE /workflows/{workflowId}`

`204 No Content`. Deleting a scheduled workflow also stops its schedule.

---

## Executions

### Start execution — `POST /workflows/{workflowId}/execute`

Body is optional:
```json
{ "input": { "imageUrl": "s3://bucket/photo.jpg" } }
```

`201 Created`
```json
{
  "executionId": "6aacf2321a704a0127be73be",
  "workflowId": "6aacf2321a704a0127be73bc",
  "workflowName": "Image Processing Pipeline",
  "triggeredBy": "6aacf2321a704a0127be73bb",
  "status": "PENDING",
  "input": { "imageUrl": "s3://bucket/photo.jpg" },
  "output": null,
  "errorMessage": null,
  "errorType": null,
  "createdAt": "2026-09-18T08:11:30.240Z",
  "updatedAt": "2026-09-18T08:11:30.240Z",
  "startedAt": null,
  "completedAt": null,
  "durationMs": null
}
```

`triggeredBy` is always the authenticated user (`scheduler` for cron runs).
Execution status: `PENDING → RUNNING → COMPLETED | FAILED | CANCELLED`.

### Get execution — `GET /workflows/executions/{executionId}`

`200 OK` (after completion; `output` aggregates each task's result by task ID):
```json
{
  "executionId": "6aacf2321a704a0127be73be",
  "status": "COMPLETED",
  "output": {
    "resize":   { "status": "success", "attempt": 1, "processedBy": "worker-7f55a92b7d6f", "taskId": "resize" },
    "compress": { "status": "success", "attempt": 1, "processedBy": "worker-7f55a92b7d6f", "taskId": "compress" },
    "validate": { "status": "success", "attempt": 1, "processedBy": "worker-7f55a92b7d6f", "taskId": "validate" }
  },
  "startedAt": "2026-09-18T08:11:30.836Z",
  "completedAt": "2026-09-18T08:11:35.891Z",
  "durationMs": 5055
}
```
(other fields as in the start response)

### Task executions — `GET /workflows/executions/{executionId}/tasks`

`200 OK`
```json
[
  {
    "id": "6aacf2321a704a0127be73bf",
    "executionId": "6aacf2321a704a0127be73be",
    "taskId": "resize",
    "taskName": "Resize image",
    "taskType": "IMAGE_RESIZE",
    "status": "COMPLETED",
    "dependsOn": [],
    "configuration": { "width": 1280, "height": 720 },
    "input": {},
    "output": { "status": "success", "attempt": 1, "processedBy": "worker-7f55a92b7d6f", "taskId": "resize" },
    "errorMessage": null,
    "errorType": null,
    "workerId": "worker-7f55a92b7d6f",
    "attemptNumber": 1,
    "maxRetries": 3,
    "retriable": true,
    "startedAt": "2026-09-18T08:11:30.833Z",
    "completedAt": "2026-09-18T08:11:31.842Z",
    "durationMs": 1009
  }
]
```

Task status: `PENDING → RUNNING → COMPLETED | FAILED | CANCELLED`. A retriable failure puts the task
back to `PENDING` with `attemptNumber + 1` until `maxRetries` attempts are used.

### Single task — `GET /workflows/executions/{executionId}/tasks/{taskId}`

`200 OK` — one task object; `404 EXECUTION_NOT_FOUND` if the task does not exist.

### Statistics — `GET /workflows/executions/{executionId}/statistics`

```json
{ "totalTasks": 3, "pendingTasks": 0, "runningTasks": 0, "completedTasks": 3,
  "failedTasks": 0, "cancelledTasks": 0, "completionPercentage": 100.0 }
```

### Cancel — `POST /workflows/executions/{executionId}/cancel`

`200 OK`
```json
{ "status": "success", "message": "Execution cancelled", "executionId": "6aacf23b1a704a0127be73c2" }
```
Tasks that have not finished are cancelled; a task already running on a worker completes but its result
is ignored. Cancelling a finished execution returns `409 INVALID_EXECUTION_STATE`.

---

## Errors

All errors use this shape:

```json
{
  "timestamp": "2026-09-18T08:11:30.206Z",
  "status": 400,
  "error": "VALIDATION_ERROR",
  "message": "Validation failed for request",
  "path": "/api/v1/workflows",
  "correlationId": "3220d177-ff10-40a9-8ba5-53809d292cbc",
  "fieldErrors": [
    { "field": "name", "message": "Workflow name is required", "rejectedValue": null }
  ]
}
```

| `error` | Status | Meaning |
|---|---|---|
| `VALIDATION_ERROR` | 400 | Request fields invalid (see `fieldErrors`) |
| `WORKFLOW_VALIDATION_ERROR` | 400 | Invalid workflow structure (duplicate IDs, unknown dependency, cycle, bad cron) |
| `MALFORMED_REQUEST` | 400 | Body missing or not valid JSON |
| `UNAUTHORIZED` | 401 | Missing, invalid or expired Bearer token |
| `AUTHENTICATION_FAILED` | 401 | Wrong email or password |
| `FORBIDDEN` | 403 | Access denied |
| `WORKFLOW_NOT_FOUND` | 404 | Workflow missing or owned by another user |
| `EXECUTION_NOT_FOUND` | 404 | Execution or task missing or owned by another user |
| `DUPLICATE_RESOURCE` | 409 | Email already registered |
| `INVALID_EXECUTION_STATE` | 409 | Operation not allowed in the current state |
| `CONCURRENT_MODIFICATION` | 409 | Resource changed concurrently, retry |
| `RATE_LIMIT_EXCEEDED` | 429 | Too many requests through the gateway |
| `BAD_GATEWAY` / `GATEWAY_TIMEOUT` | 502 / 504 | Gateway could not reach the workflow service |
| `INTERNAL_SERVER_ERROR` | 500 | Unexpected error |

---

## Simulated Tasks

Workers do not run real image or data processing: each attempt sleeps and returns a result. The task
`configuration` can control this, which is useful for trying out retries and failures:

| Key | Effect |
|---|---|
| `simulateDurationMs` | How long the attempt runs (default 1000) |
| `failUntilAttempt` | Fail with a retriable error while `attemptNumber <= value` |
| `failPermanently` | Fail with a non-retriable error (no retries, execution fails) |

Combined with `timeoutMs`, a `simulateDurationMs` above the timeout exercises timeout handling.
