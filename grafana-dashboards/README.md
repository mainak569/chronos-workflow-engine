# Chronos Grafana Dashboards

This directory contains pre-built Grafana dashboards for monitoring Chronos workflow orchestration.

## Dashboards

1. **workflow-execution.json** - Workflow Execution Overview
   - Workflow execution rate
   - Active workflows
   - Success/failure rates
   - Duration percentiles
   - Top failed workflows

2. **task-success-failure.json** - Task Success/Failure Analysis
   - Task execution rates
   - Success/failure breakdown
   - Retry patterns
   - Queue depth monitoring

3. **worker-availability.json** - Worker Health Monitoring
   - Available workers count
   - Worker heartbeat status
   - Worker utilization
   - Tasks processed per worker

4. **task-latency.json** - Task Latency Performance
   - Task duration percentiles
   - Queue wait times
   - Latency heatmaps
   - Slowest tasks

5. **kafka-activity.json** - Kafka Activity Tracking
   - Consumer lag
   - Message processing rates
   - Processing duration
   - Error rates

6. **retry-counts.json** - Retry Pattern Analysis
   - Total retries
   - Retry rates by task type
   - Retry reasons breakdown
   - Retry success rates

## Import Instructions

### Method 1: Grafana UI
1. Log in to Grafana
2. Go to Dashboards → Import
3. Click "Upload JSON file"
4. Select dashboard JSON file
5. Select Prometheus data source
6. Click "Import"

### Method 2: Grafana API
```bash
curl -X POST http://grafana:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer YOUR_API_KEY" \
  -d @workflow-execution.json
```

### Method 3: Provisioning (Automated)
1. Copy JSON files to Grafana provisioning directory:
   ```
   /etc/grafana/provisioning/dashboards/
   ```

2. Create provisioning config:
   ```yaml
   # /etc/grafana/provisioning/dashboards/chronos.yml
   apiVersion: 1
   providers:
     - name: 'Chronos Dashboards'
       orgId: 1
       folder: 'Chronos'
       type: file
       disableDeletion: false
       updateIntervalSeconds: 30
       options:
         path: /etc/grafana/provisioning/dashboards/chronos
   ```

3. Restart Grafana

## Data Source Configuration

All dashboards require a Prometheus data source named "Prometheus".

Configure in Grafana:
1. Configuration → Data Sources → Add data source
2. Select "Prometheus"
3. URL: `http://prometheus:9090`
4. Access: Server (default)
5. Save & Test

## Variables

Dashboards support the following variables for filtering:
- `$environment` - Environment (dev, staging, prod)
- `$service` - Service name
- `$workflow_id` - Specific workflow ID
- `$task_type` - Task type filter

## Alerting

Dashboards include pre-configured alert panels:
- High workflow failure rate (>10% for 5 minutes)
- Worker down (no heartbeat for 2 minutes)
- High Kafka lag (>1000 messages for 5 minutes)
- High task retry rate (>20% for 5 minutes)

Configure notification channels in Grafana:
1. Alerting → Notification channels
2. Add channel (email, Slack, PagerDuty, etc.)
3. Link to dashboards

## Refresh Rates

- Default refresh: 30s
- Options: 10s, 30s, 1m, 5m, 15m, 30m, 1h
- Auto-refresh enabled by default

## Time Ranges

Default time range: Last 1 hour

Quick ranges available:
- Last 5 minutes
- Last 15 minutes
- Last 30 minutes
- Last 1 hour
- Last 3 hours
- Last 6 hours
- Last 12 hours
- Last 24 hours
- Last 7 days

## Customization

To customize dashboards:
1. Import dashboard
2. Make changes in Grafana UI
3. Save as new version or export JSON
4. Commit updated JSON to repository

## Troubleshooting

### No data appearing
- Verify Prometheus is scraping metrics: `http://prometheus:9090/targets`
- Check service `/actuator/prometheus` endpoint
- Verify time range and refresh interval
- Check Prometheus data source configuration

### Incorrect metrics
- Verify metric names match implementation
- Check label filters (case-sensitive)
- Validate PromQL queries in Prometheus UI

### Slow dashboard loading
- Reduce time range
- Increase scrape interval
- Use recording rules in Prometheus
- Optimize queries (avoid high cardinality)

## Maintenance

- Review and update dashboards quarterly
- Remove obsolete panels
- Add new metrics as features are added
- Keep dashboard JSON in version control
- Document custom queries and panels

## Support

For issues or enhancements:
1. Check metric availability in Prometheus
2. Test PromQL queries in Prometheus UI first
3. Verify data source configuration
4. Review Grafana logs for errors
