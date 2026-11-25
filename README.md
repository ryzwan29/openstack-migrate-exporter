# openstack-migrate-exporter

Prometheus exporter for Nova live migrations (status, duration, memory speed/progress). Stdlib only, no deps.
Data: Nova API (`/os-migrations`, `/servers/{id}/migrations/{id}`), admin credentials needed.

## run
```
docker compose -f deploy/docker-compose.yml up -d
curl -s localhost:9181/metrics | grep nova_live_migration
```
Auth: `clouds.yaml` (`OS_CLOUD`) or `OS_*` env. Flags/env: `-listen :9181`, `-poll-interval 5s`, `-lookback 24h`,
`-nova-microversion 2.80`, `-interface internal`, `-nova-endpoint URL`, `-insecure`.

## metrics
| metric | note |
|---|---|
| `nova_live_migration_in_progress` | count |
| `nova_live_migration_{info,elapsed_seconds}{migration,instance,source,dest}` | per running migration |
| `nova_live_migration_memory_{total,processed,remaining}_bytes` | per running (disk_* too if block migration) |
| `nova_live_migration_memory_{speed,avg_speed}_bytes_per_second` | instant (between polls) / avg since start |
| `nova_live_migration_memory_progress_ratio` | 1 - remaining/total |
| `nova_live_migrations_total{source,dest,status}` | finished, by outcome |
| `nova_live_migration_duration_seconds` | histogram, updated_at - created_at |
| `nova_live_migration_last_*{source,dest,status}` | last finished: duration, timestamp, avg speed, bytes |
| `nova_migrate_exporter_{up,poll_errors_total,...}` | self metrics |

Progress/speed only exist while a migration is running (Nova drops them after). Migrations that finish
between two polls still get duration + outcome, but no speed.

## build / push
```
make test
docker login ghcr.io -u ryzwan29        # PAT: write:packages
make push VERSION=v1.0.0                 # pushes :v1.0.0 and :latest
```
Or push a tag `v1.0.0` -> GitHub Actions builds amd64+arm64 and pushes both tags.

## grafana
Copy `deploy/grafana/openstack-live-migration.json` to `/opt/grafana/grafana/dashboards/`, `deploy/grafana/dashboards.yml`
to `/opt/grafana/grafana/provisioning/dashboards/`, mount `/opt/grafana/grafana/dashboards:/etc/grafana/dashboards:ro`.
