# Chronos Observability Stack

Complete observability infrastructure for Chronos workflow orchestration platform.

## Quick Start

```bash
# Start all services
docker-compose up -d

# Verify
docker-compose ps

# Access services
open http://localhost:9090  # Prometheus
open http://localhost:3000  # Grafana (admin/admin)
open http://localhost:9093  # Alertmanager
```

## Services

| Service | Port | Purpose |
|---------|------|---------|
| Prometheus | 9090 | Metrics collection and storage |
| Grafana | 3000 | Visualization and dashboards |
| Alertmanager | 9093 | Alert routing and management |
| Kafka Exporter | 9308 | Kafka metrics for Prometheus |
| MongoDB Exporter | 9216 | MongoDB metrics for Prometheus |
| Node Exporter | 9100 | Host system metrics |
| Loki | 3100 | Log aggregation |
| Promtail | - | Log shipping to Loki |

## Configuration Files

### Prometheus
- `prometheus.yml` - Main configuration
- `prometheus-alerts.yml` - Alert rules

### Grafana
- `grafana-provisioning/` - Auto-provisioning config
- `../grafana-dashboards/` - Dashboard JSON files

### Alertmanager
- `alertmanager.yml` - Notification routing

### Loki
- `loki-config.yml` - Log aggregation config
- `promtail-config.yml` - Log shipping config

## Environment Variables

Required for alerting (optional for basic setup):

```bash
# SMTP for email alerts
export SMTP_PASSWORD=your-smtp-password

# PagerDuty integration
export PAGERDUTY_SERVICE_KEY=your-pagerduty-key

# Slack integration
export SLACK_WEBHOOK_URL=your-slack-webhook
```

## Metrics Endpoints

Chronos services expose metrics at:
- Workflow Service: http://localhost:8081/actuator/prometheus
- Scheduler Service: http://localhost:8082/actuator/prometheus
- Worker Service: http://localhost:8083/actuator/prometheus
- API Gateway: http://localhost:8080/actuator/prometheus

## Data Persistence

Volumes:
- `prometheus-data` - Metric time-series (15 days retention)
- `grafana-data` - Dashboards and settings
- `alertmanager-data` - Alert history
- `loki-data` - Log storage

Backup:
```bash
docker-compose stop
docker run --rm -v chronos_prometheus-data:/data -v $(pwd):/backup alpine tar czf /backup/prometheus-backup.tar.gz -C /data .
docker-compose start
```

## Common Tasks

### View Logs
```bash
docker-compose logs -f prometheus
docker-compose logs -f grafana
```

### Reload Prometheus Config
```bash
curl -X POST http://localhost:9090/-/reload
```

### Check Scrape Targets
```bash
curl http://localhost:9090/api/v1/targets | jq
```

### Import Grafana Dashboard
```bash
curl -X POST http://admin:admin@localhost:3000/api/dashboards/db \
  -H "Content-Type: application/json" \
  -d @../grafana-dashboards/workflow-execution.json
```

### Test Alert
```bash
# Trigger test alert by stopping a service
docker stop workflow-service

# Check alerts
curl http://localhost:9090/api/v1/alerts | jq
curl http://localhost:9093/api/v2/alerts | jq

# Restart service
docker start workflow-service
```

## Troubleshooting

### No Metrics Appearing
1. Check Prometheus targets: http://localhost:9090/targets
2. Verify service actuator endpoints are accessible
3. Check network connectivity
4. Review Prometheus logs: `docker-compose logs prometheus`

### Grafana Can't Connect to Prometheus
1. Verify data source URL: `http://prometheus:9090`
2. Check Docker network: `docker network inspect chronos-network`
3. Test from Grafana container: `docker exec chronos-grafana wget -O- http://prometheus:9090/api/v1/status/config`

### Alerts Not Firing
1. Check alert rules: http://localhost:9090/rules
2. Verify Alertmanager config: http://localhost:9093/#/status
3. Review Alertmanager logs: `docker-compose logs alertmanager`
4. Test alert routing: http://localhost:9093/#/config

### High Disk Usage
```bash
# Check volume sizes
docker system df -v

# Clean old data (careful!)
docker-compose down
docker volume rm chronos_prometheus-data
docker-compose up -d
```

## Upgrading

```bash
# Pull latest images
docker-compose pull

# Restart with new images
docker-compose up -d

# Verify
docker-compose ps
```

## Development

### Local Testing
```bash
# Use local Prometheus config
docker run -d -p 9090:9090 \
  -v $(pwd)/prometheus.yml:/etc/prometheus/prometheus.yml \
  prom/prometheus

# Test scraping
curl http://localhost:8081/actuator/prometheus
```

### Custom Alerts
1. Edit `prometheus-alerts.yml`
2. Validate syntax: `promtool check rules prometheus-alerts.yml`
3. Reload: `curl -X POST http://localhost:9090/-/reload`
4. Check: http://localhost:9090/rules

## Production Considerations

### Security
- [ ] Enable authentication on Grafana
- [ ] Restrict actuator endpoints to internal network
- [ ] Use TLS for Prometheus/Grafana
- [ ] Set strong passwords (not admin/admin)
- [ ] Limit exposed ports in docker-compose.yml

### Performance
- [ ] Tune Prometheus retention (default: 15d)
- [ ] Configure recording rules for expensive queries
- [ ] Use remote write for long-term storage
- [ ] Monitor Prometheus resource usage

### High Availability
- [ ] Run multiple Prometheus instances
- [ ] Use Prometheus federation or Thanos
- [ ] Configure Alertmanager clustering
- [ ] Set up Grafana HA with shared database

### Monitoring the Monitors
- [ ] Monitor Prometheus scrape health
- [ ] Alert on Grafana downtime
- [ ] Track Alertmanager notification delivery
- [ ] Set up external synthetic checks

## Resources

- [Prometheus Documentation](https://prometheus.io/docs/)
- [Grafana Documentation](https://grafana.com/docs/)
- [Alertmanager Documentation](https://prometheus.io/docs/alerting/latest/alertmanager/)
- [Chronos Architecture](../docs/observability-architecture.md)
- [Quick Start Guide](../docs/OBSERVABILITY_QUICK_START.md)

## Architecture

```
┌─────────────────────────────────────────┐
│         Chronos Services                │
│  ┌──────┐ ┌──────┐ ┌──────┐ ┌──────┐  │
│  │ Work │ │Sched │ │Worker│ │ API  │  │
│  │flow  │ │uler  │ │      │ │Gateway│ │
│  └──┬───┘ └──┬───┘ └──┬───┘ └──┬───┘  │
└─────┼────────┼────────┼────────┼───────┘
      │        │        │        │
      └────────┴────────┴────────┘
           /actuator/prometheus
                  │
                  ▼
      ┌──────────────────────┐
      │     Prometheus        │
      │  - Scrapes metrics    │
      │  - Evaluates alerts   │
      │  - 15 day retention   │
      └───────┬──────────────┘
              │
              ▼
      ┌──────────────────────┐
      │       Grafana         │
      │  - Dashboards         │
      │  - Visualization      │
      └──────────────────────┘
```

## License

Same as Chronos main project.

## Support

For issues or questions:
1. Check troubleshooting section
2. Review logs: `docker-compose logs`
3. Consult architecture documentation
4. Verify configuration files

---

**Status**: ✅ Production Ready  
**Version**: 1.0.0  
**Last Updated**: September 16, 2026
