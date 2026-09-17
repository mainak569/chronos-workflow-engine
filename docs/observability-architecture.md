# Chronos Observability Architecture

## Overview
Complete observability solution for distributed workflow orchestration with metrics, logging, tracing, and visualization.

**Date**: September 16, 2026  
**Status**: Implementation Complete

---

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Metrics](#metrics)
3. [Logging](#logging)
4. [Distributed Tracing](#distributed-tracing)
5. [Dashboards](#dashboards)
6. [Implementation](#implementation)
7. [Security](#security)
8. [Operations](#operations)

---

## Architecture Overview

### Components

```
┌─────────────────────────────────────────────────────────────────┐
│                        Chronos Services                         │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐         │
│  │Workflow  │  │Scheduler │  │ Worker   │  │  API     │         │
│  │ Service  │  │ Service  │  │ Service  │  │ Gateway  │         │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘         │
│       │             │             │             │               │
│       ├─────────────┴─────────────┴─────────────┘               │
│       │ Micrometer + Spring Actuator                            │
└───────┼─────────────────────────────────────────────────────────┘
        │
        │ /actuator/prometheus (HTTP scrape)
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                         Prometheus                              │
│  - Metric storage (time-series database)                        │
│  - Scrapes /actuator/prometheus every 15s                       │
│  - Retention: 15 days default                                   │
│  - Alerting rules                                               │
└───────┬─────────────────────────────────────────────────────────┘
        │
        │ PromQL queries
        ▼
┌─────────────────────────────────────────────────────────────────┐
│                          Grafana                                │
│  - Visualization dashboards                                     │
│  - Alerting and notifications                                   │
│  - Pre-built dashboards for workflows, tasks, workers           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                      Logging Pipeline                           │
│                                                                 │
│  Services → Logback → JSON format → stdout → Log aggregator     │
│             (structured)  (correlation IDs)                     │
└─────────────────────────────────────────────────────────────────┘
```

### Technology Stack
- **Metrics**: Micrometer + Prometheus
- **Logging**: Logback + Logstash JSON Encoder
- **Visualization**: Grafana
- **Tracing**: Correlation IDs (X-Correlation-ID)
- **Exposure**: Spring Boot Actuator

---

## Metrics

### 1. Workflow Metrics

#### Workflow Executions
```
chronos_workflow_executions_total{status, workflow_id}
  - Type: Counter
  - Labels: status (RUNNING|COMPLETED|FAILED), workflow_id
  - Description: Total number of workflow executions
```

#### Workflow Duration
```
chronos_workflow_duration_seconds{workflow_id, status}
  - Type: Timer/Histogram
  - Labels: workflow_id, status
  - Description: Workflow execution duration
  - Buckets: 1s, 5s, 10s, 30s, 1m, 5m, 10m, 30m, 1h
```

#### Active Workflows
```
chronos_workflow_active{workflow_id}
  - Type: Gauge
  - Labels: workflow_id
  - Description: Number of currently executing workflows
```

#### Workflow Failures
```
chronos_workflow_failures_total{workflow_id, error_type}
  - Type: Counter
  - Labels: workflow_id, error_type
  - Description: Total workflow failures by type
```

### 2. Task Metrics

#### Task Executions
```
chronos_task_executions_total{task_type, status}
  - Type: Counter
  - Labels: task_type, status (STARTED|COMPLETED|FAILED)
  - Description: Total task executions
```

#### Task Duration
```
chronos_task_duration_seconds{task_type, worker_id}
  - Type: Timer/Histogram
  - Labels: task_type, worker_id
  - Description: Task execution duration
  - Buckets: 100ms, 500ms, 1s, 5s, 10s, 30s, 1m, 5m
```

#### Task Retries
```
chronos_task_retries_total{task_type, retry_reason}
  - Type: Counter
  - Labels: task_type, retry_reason
  - Description: Total task retry attempts
```

#### Task Queue Depth
```
chronos_task_queue_depth{task_type}
  - Type: Gauge
  - Labels: task_type
  - Description: Number of tasks waiting in queue
```

#### Task Latency
```
chronos_task_latency_seconds{task_type}
  - Type: Histogram
  - Labels: task_type
  - Description: Time from task creation to execution start
```

### 3. Worker Metrics

#### Available Workers
```
chronos_worker_available{worker_id, task_type}
  - Type: Gauge
  - Labels: worker_id, task_type
  - Description: Number of available workers per type
```

#### Worker Heartbeats
```
chronos_worker_heartbeat_timestamp{worker_id}
  - Type: Gauge
  - Labels: worker_id
  - Description: Last heartbeat timestamp (Unix epoch)
```

#### Worker Task Processing
```
chronos_worker_tasks_processed_total{worker_id, task_type, status}
  - Type: Counter
  - Labels: worker_id, task_type, status
  - Description: Tasks processed by worker
```

#### Worker Failures
```
chronos_worker_failures_total{worker_id, failure_type}
  - Type: Counter
  - Labels: worker_id, failure_type
  - Description: Worker failure count
```

#### Worker Utilization
```
chronos_worker_utilization_ratio{worker_id}
  - Type: Gauge (0.0 to 1.0)
  - Labels: worker_id
  - Description: Worker utilization percentage
```

### 4. Kafka Metrics

#### Consumer Lag
```
kafka_consumer_lag{topic, partition, consumer_group}
  - Type: Gauge
  - Description: Number of messages behind
  - Source: Kafka metrics (built-in)
```

#### Message Processing Rate
```
chronos_kafka_messages_processed_total{topic, consumer_group}
  - Type: Counter
  - Labels: topic, consumer_group
  - Description: Total messages processed
```

#### Message Processing Duration
```
chronos_kafka_message_processing_seconds{topic}
  - Type: Histogram
  - Labels: topic
  - Description: Time to process a message
```

#### Kafka Errors
```
chronos_kafka_errors_total{topic, error_type}
  - Type: Counter
  - Labels: topic, error_type
  - Description: Kafka processing errors
```

### 5. Scheduler Metrics

#### Leader Election
```
chronos_scheduler_leader{instance_id}
  - Type: Gauge (0 or 1)
  - Labels: instance_id
  - Description: Whether this instance is leader
```

#### Scheduling Lag
```
chronos_scheduler_lag_seconds
  - Type: Histogram
  - Description: Difference between scheduled time and actual execution
```

#### Scheduled Workflows
```
chronos_scheduler_workflows_scheduled_total
  - Type: Counter
  - Description: Total workflows scheduled
```

### 6. Database Metrics

#### MongoDB Operations
```
mongodb_driver_commands_seconds{command, status}
  - Type: Timer
  - Labels: command, status
  - Description: MongoDB command duration
  - Source: Spring Data MongoDB metrics
```

#### Connection Pool
```
mongodb_driver_pool_size{pool}
  - Type: Gauge
  - Description: MongoDB connection pool size
```

### 7. JVM Metrics (Built-in)

```
jvm_memory_used_bytes{area}
jvm_gc_pause_seconds{action, cause}
jvm_threads_live
process_cpu_usage
process_uptime_seconds
```

---

## Logging

### Structured Logging Format

All logs are JSON-formatted with consistent structure:

```json
{
  "timestamp": "2026-09-16T23:30:45.123Z",
  "level": "INFO",
  "thread": "http-nio-8081-exec-1",
  "logger": "com.chronos.workflow.service.WorkflowService",
  "message": "Workflow execution started",
  "correlation_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "service": "workflow-service",
  "workflow_id": "wf-123",
  "execution_id": "exec-456",
  "user_id": "user-789",
  "context": {
    "workflow_name": "data-pipeline",
    "task_count": 5
  }
}
```

### Log Levels

- **ERROR**: Failures requiring immediate attention
- **WARN**: Potentially harmful situations (retries, degraded performance)
- **INFO**: Important business events (workflow started/completed, task executed)
- **DEBUG**: Detailed diagnostic information (disabled in production)
- **TRACE**: Very detailed diagnostic information (never in production)

### Correlation IDs

#### Generation
- Generated at API Gateway for external requests
- Propagated through HTTP headers: `X-Correlation-ID`
- Propagated through Kafka message headers
- Stored in MDC (Mapped Diagnostic Context)

#### Usage
```java
// Automatic in logs via MDC
log.info("Processing workflow"); // Includes correlation_id

// Manual access
String correlationId = MDC.get("correlation_id");
```

### Sensitive Data Filtering

**Never log**:
- Passwords (plaintext or hashed)
- JWT tokens (full token)
- API keys
- Personal identifiable information (PII) unless necessary
- Full task configurations (may contain secrets)

**Safe to log**:
- User IDs (not emails)
- Workflow IDs, task IDs, execution IDs
- Status and state transitions
- Timing and duration
- Error types (not full stack traces with data)

### Log Aggregation

Recommended tools:
1. **ELK Stack** (Elasticsearch, Logstash, Kibana)
2. **Loki** + Grafana
3. **CloudWatch Logs** (AWS)
4. **Stackdriver** (GCP)

Configuration:
- JSON logs to stdout
- Container runtime captures stdout
- Log aggregator ingests from containers
- Query via Kibana/Grafana

---

## Distributed Tracing

### Correlation ID Flow

```
┌──────────┐   X-Correlation-ID     ┌──────────┐
│  Client  │ ─────────────────────> │   API    │
└──────────┘                        │ Gateway  │
                                    └────┬─────┘
                                         │ Generate if missing
                                         │
                                    ┌────▼─────┐
                                    │ Workflow │
                                    │ Service  │
                                    └────┬─────┘
                                         │ Propagate via HTTP
                                         │
                  ┌──────────────────────┼──────────────────────┐
                  │                      │                      │
            ┌─────▼─────┐          ┌─────▼─────┐        ┌─────▼─────┐
            │ Scheduler │          │  Worker   │        │  Worker   │
            │  Service  │          │ Service 1 │        │ Service 2 │
            └───────────┘          └───────────┘        └───────────┘
                  │                      │                      │
                  └──────────────────────┴──────────────────────┘
                                         │
                                         ▼
                               Kafka (via headers)
```

### Implementation

#### 1. Generate at API Gateway
```java
@Component
public class CorrelationIdFilter implements Filter {
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, 
                        FilterChain chain) {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        String correlationId = httpRequest.getHeader("X-Correlation-ID");
        
        if (correlationId == null) {
            correlationId = UUID.randomUUID().toString();
        }
        
        MDC.put("correlation_id", correlationId);
        chain.doFilter(request, response);
        MDC.clear();
    }
}
```

#### 2. Propagate via HTTP
```java
@Component
public class CorrelationIdInterceptor implements ClientHttpRequestInterceptor {
    @Override
    public ClientHttpResponse intercept(HttpRequest request, byte[] body, 
                                       ClientHttpRequestExecution execution) {
        String correlationId = MDC.get("correlation_id");
        if (correlationId != null) {
            request.getHeaders().set("X-Correlation-ID", correlationId);
        }
        return execution.execute(request, body);
    }
}
```

#### 3. Propagate via Kafka
```java
public void sendMessage(String topic, Object payload) {
    ProducerRecord<String, Object> record = new ProducerRecord<>(topic, payload);
    
    String correlationId = MDC.get("correlation_id");
    if (correlationId != null) {
        record.headers().add("X-Correlation-ID", 
            correlationId.getBytes(StandardCharsets.UTF_8));
    }
    
    kafkaTemplate.send(record);
}
```

#### 4. Extract from Kafka
```java
@KafkaListener
public void handleMessage(ConsumerRecord<String, Object> record) {
    Header correlationHeader = record.headers()
        .lastHeader("X-Correlation-ID");
    
    if (correlationHeader != null) {
        String correlationId = new String(correlationHeader.value(), 
            StandardCharsets.UTF_8);
        MDC.put("correlation_id", correlationId);
    }
    
    try {
        // Process message
    } finally {
        MDC.clear();
    }
}
```

---

## Dashboards

### 1. Workflow Execution Dashboard

**Purpose**: Monitor workflow health and performance

**Panels**:
1. **Workflow Execution Rate** (Graph)
   - Query: `rate(chronos_workflow_executions_total[5m])`
   - Split by status

2. **Active Workflows** (Gauge)
   - Query: `sum(chronos_workflow_active)`

3. **Workflow Success Rate** (Graph)
   - Query: `rate(chronos_workflow_executions_total{status="COMPLETED"}[5m]) / rate(chronos_workflow_executions_total[5m])`

4. **Workflow Duration p50/p95/p99** (Graph)
   - Query: `histogram_quantile(0.95, chronos_workflow_duration_seconds)`

5. **Top Failed Workflows** (Table)
   - Query: `topk(10, sum by (workflow_id) (chronos_workflow_failures_total))`

6. **Workflow Execution Timeline** (Heatmap)
   - Query: `chronos_workflow_duration_seconds`

### 2. Task Success/Failure Dashboard

**Purpose**: Monitor task execution health

**Panels**:
1. **Task Execution Rate by Type** (Graph)
   - Query: `rate(chronos_task_executions_total[5m])`
   - Split by task_type

2. **Task Success Rate** (Gauge)
   - Query: `rate(chronos_task_executions_total{status="COMPLETED"}[5m]) / rate(chronos_task_executions_total[5m])`

3. **Task Failure Rate** (Graph)
   - Query: `rate(chronos_task_executions_total{status="FAILED"}[5m])`

4. **Task Retry Rate** (Graph)
   - Query: `rate(chronos_task_retries_total[5m])`

5. **Top Failing Tasks** (Table)
   - Query: `topk(10, sum by (task_type) (chronos_task_executions_total{status="FAILED"}))`

6. **Task Queue Depth** (Graph)
   - Query: `chronos_task_queue_depth`

### 3. Worker Availability Dashboard

**Purpose**: Monitor worker health and capacity

**Panels**:
1. **Total Available Workers** (Stat)
   - Query: `sum(chronos_worker_available)`

2. **Workers by Type** (Pie Chart)
   - Query: `sum by (task_type) (chronos_worker_available)`

3. **Worker Heartbeat Status** (Table)
   - Query: `time() - chronos_worker_heartbeat_timestamp > 60`
   - Shows stale workers (no heartbeat >60s)

4. **Worker Utilization** (Gauge)
   - Query: `avg(chronos_worker_utilization_ratio)`

5. **Worker Failure Rate** (Graph)
   - Query: `rate(chronos_worker_failures_total[5m])`

6. **Tasks Processed per Worker** (Bar Chart)
   - Query: `sum by (worker_id) (chronos_worker_tasks_processed_total)`

### 4. Task Latency Dashboard

**Purpose**: Monitor task performance and bottlenecks

**Panels**:
1. **Task Latency p50/p95/p99** (Graph)
   - Query: `histogram_quantile(0.95, rate(chronos_task_duration_seconds_bucket[5m]))`

2. **Task Latency Heatmap** (Heatmap)
   - Query: `chronos_task_duration_seconds`

3. **Queue Wait Time** (Graph)
   - Query: `chronos_task_latency_seconds`

4. **Slowest Tasks** (Table)
   - Query: `topk(10, histogram_quantile(0.99, chronos_task_duration_seconds))`

5. **Task Duration by Type** (Bar Chart)
   - Query: `avg by (task_type) (chronos_task_duration_seconds)`

### 5. Kafka Activity Dashboard

**Purpose**: Monitor message processing and lag

**Panels**:
1. **Consumer Lag** (Graph)
   - Query: `kafka_consumer_lag`
   - Split by topic

2. **Message Processing Rate** (Graph)
   - Query: `rate(chronos_kafka_messages_processed_total[5m])`

3. **Message Processing Duration** (Graph)
   - Query: `histogram_quantile(0.95, chronos_kafka_message_processing_seconds)`

4. **Kafka Errors** (Graph)
   - Query: `rate(chronos_kafka_errors_total[5m])`

5. **Consumer Group Lag Table** (Table)
   - Query: `max by (topic, partition) (kafka_consumer_lag)`

### 6. Retry Counts Dashboard

**Purpose**: Monitor retry patterns and failure recovery

**Panels**:
1. **Total Retries** (Stat)
   - Query: `sum(chronos_task_retries_total)`

2. **Retry Rate by Type** (Graph)
   - Query: `rate(chronos_task_retries_total[5m])`
   - Split by task_type

3. **Retries by Reason** (Pie Chart)
   - Query: `sum by (retry_reason) (chronos_task_retries_total)`

4. **Retry Success Rate** (Gauge)
   - Query: `(chronos_task_executions_total{status="COMPLETED"} after retry) / chronos_task_retries_total`

5. **Tasks with Most Retries** (Table)
   - Query: `topk(10, chronos_task_retries_total)`

---

## Implementation

### 1. Dependencies (pom.xml)

```xml
<!-- Actuator -->
<dependency>
    <groupId>org.springframework.boot</groupId>
    <artifactId>spring-boot-starter-actuator</artifactId>
</dependency>

<!-- Micrometer Prometheus -->
<dependency>
    <groupId>io.micrometer</groupId>
    <artifactId>micrometer-registry-prometheus</artifactId>
</dependency>

<!-- Structured Logging -->
<dependency>
    <groupId>net.logstash.logback</groupId>
    <artifactId>logstash-logback-encoder</artifactId>
    <version>7.4</version>
</dependency>
```

### 2. Application Configuration

```yaml
# application.yml
management:
  endpoints:
    web:
      exposure:
        include: health,info,metrics,prometheus
      base-path: /actuator
  endpoint:
    health:
      show-details: always
    prometheus:
      enabled: true
  metrics:
    export:
      prometheus:
        enabled: true
    tags:
      application: ${spring.application.name}
      environment: ${ENVIRONMENT:dev}
    distribution:
      percentiles-histogram:
        http.server.requests: true
        chronos.workflow.duration: true
        chronos.task.duration: true

logging:
  level:
    root: INFO
    com.chronos: DEBUG
  pattern:
    console: "%d{yyyy-MM-dd HH:mm:ss} [%X{correlation_id}] [%thread] %-5level %logger{36} - %msg%n"
```

### 3. Logback Configuration (logback-spring.xml)

```xml
<configuration>
    <include resource="org/springframework/boot/logging/logback/defaults.xml"/>
    
    <appender name="CONSOLE_JSON" class="ch.qos.logback.core.ConsoleAppender">
        <encoder class="net.logstash.logback.encoder.LogstashEncoder">
            <includeMdcKeyName>correlation_id</includeMdcKeyName>
            <includeMdcKeyName>workflow_id</includeMdcKeyName>
            <includeMdcKeyName>execution_id</includeMdcKeyName>
            <includeMdcKeyName>task_id</includeMdcKeyName>
            <includeMdcKeyName>user_id</includeMdcKeyName>
            <customFields>{"service":"${spring.application.name}"}</customFields>
            <fieldNames>
                <timestamp>timestamp</timestamp>
                <message>message</message>
                <logger>logger</logger>
                <level>level</level>
                <thread>thread</thread>
            </fieldNames>
        </encoder>
    </appender>
    
    <root level="INFO">
        <appender-ref ref="CONSOLE_JSON"/>
    </root>
</configuration>
```

### 4. Metrics Configuration Class

```java
@Configuration
public class MetricsConfiguration {
    
    @Bean
    public MeterRegistryCustomizer<MeterRegistry> metricsCommonTags(
            @Value("${spring.application.name}") String applicationName) {
        return registry -> registry.config()
            .commonTags("application", applicationName);
    }
}
```

### 5. Custom Metrics

```java
@Component
public class WorkflowMetrics {
    
    private final Counter workflowExecutions;
    private final Timer workflowDuration;
    private final Gauge activeWorkflows;
    
    public WorkflowMetrics(MeterRegistry registry) {
        this.workflowExecutions = Counter.builder("chronos.workflow.executions")
            .description("Total workflow executions")
            .tags("status", "")
            .register(registry);
            
        this.workflowDuration = Timer.builder("chronos.workflow.duration")
            .description("Workflow execution duration")
            .publishPercentiles(0.5, 0.95, 0.99)
            .register(registry);
            
        this.activeWorkflows = Gauge.builder("chronos.workflow.active", 
            () -> getActiveWorkflowCount())
            .description("Active workflows")
            .register(registry);
    }
}
```

---

## Security

### Actuator Security

**Production Configuration**:
```yaml
management:
  endpoints:
    web:
      exposure:
        include: health,prometheus
  endpoint:
    health:
      show-details: when-authorized
```

**Secure Endpoints**:
- `/actuator/health` - Public (for load balancers)
- `/actuator/prometheus` - Internal network only (Prometheus scrape)
- All other actuator endpoints - Disabled in production

### Metric Data Security

**Do NOT expose in metrics**:
- User credentials
- API keys
- Sensitive task data
- Personal information

**Safe metric labels**:
- IDs (workflow_id, task_id, worker_id)
- Types (task_type, error_type)
- Status (COMPLETED, FAILED)
- Service names

### Log Security

**Filtering sensitive data**:
```java
@Component
public class SensitiveDataFilter implements Filter {
    private static final Pattern PASSWORD_PATTERN = 
        Pattern.compile("password\\s*=\\s*\"[^\"]*\"");
    
    public String filter(String message) {
        return PASSWORD_PATTERN.matcher(message)
            .replaceAll("password=\"***\"");
    }
}
```

---

## Operations

### Prometheus Setup

**prometheus.yml**:
```yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'chronos-services'
    metrics_path: '/actuator/prometheus'
    static_configs:
      - targets:
          - 'workflow-service:8081'
          - 'scheduler-service:8082'
          - 'worker-service:8083'
          - 'api-gateway:8080'
```

### Grafana Setup

**Data Source**:
- Type: Prometheus
- URL: http://prometheus:9090
- Access: Server (default)

**Dashboard Import**:
1. Copy JSON from `grafana-dashboards/` directory
2. Grafana UI → Dashboards → Import
3. Paste JSON or upload file
4. Select Prometheus data source

### Alerting Rules

**prometheus-alerts.yml**:
```yaml
groups:
  - name: chronos_alerts
    interval: 30s
    rules:
      - alert: HighWorkflowFailureRate
        expr: rate(chronos_workflow_failures_total[5m]) > 0.1
        for: 5m
        labels:
          severity: critical
        annotations:
          summary: "High workflow failure rate"
          
      - alert: WorkerDown
        expr: time() - chronos_worker_heartbeat_timestamp > 120
        for: 2m
        labels:
          severity: warning
        annotations:
          summary: "Worker {{ $labels.worker_id }} is down"
          
      - alert: HighKafkaLag
        expr: kafka_consumer_lag > 1000
        for: 5m
        labels:
          severity: warning
        annotations:
          summary: "High Kafka lag on {{ $labels.topic }}"
```

### Troubleshooting

**Common Issues**:

1. **No metrics appearing**
   - Check `/actuator/prometheus` is accessible
   - Verify Prometheus scrape config
   - Check firewall rules

2. **High cardinality warnings**
   - Avoid unbounded label values
   - Use finite set of labels
   - Aggregate at query time

3. **Missing correlation IDs**
   - Verify MDC.put() in all entry points
   - Check filter/interceptor order
   - Confirm Kafka header propagation

4. **Logs not structured**
   - Verify logback-spring.xml
   - Check Logstash encoder dependency
   - Validate JSON format

---

## Summary

**Complete observability stack**:
- Prometheus metrics for all services
- Structured JSON logging with correlation IDs
- Grafana dashboards for visualization
- Alerting rules for critical issues
- Secure sensitive data filtering
- Production-ready configuration

**Key metrics tracked**:
- Workflow: executions, failures, duration, active count
- Tasks: started, completed, failed, retries, latency, queue depth
- Workers: availability, heartbeats, utilization, failures
- Kafka: consumer lag, processing rate, errors
- Scheduler: leader election, scheduling lag

**Dashboards provided**:
1. Workflow Execution Overview
2. Task Success/Failure Analysis
3. Worker Health Monitoring
4. Task Latency Performance
5. Kafka Activity Tracking
6. Retry Pattern Analysis

Ready for production deployment with full observability!
