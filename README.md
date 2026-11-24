# Distributed Job Scheduler (Spring Boot 3.3, Java 17)

PostgreSQL = source of truth, Redis ZSET = hot timer index, Kafka = dispatch queue, Redisson = overlap locks.

## Run
```bash
docker compose up -d                 # Postgres, Redis, Kafka (KRaft)
mvn spring-boot:run                  # API on :8080 (Flyway creates the schema)
mvn test                             # unit tests (no infrastructure needed)
```

## Try it
```bash
# one-time HTTP job in 30s
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "tenantId":"acme","name":"ping","jobType":"HTTP","scheduleType":"ONE_TIME","delaySeconds":30,
  "payload":{"url":"https://httpbin.org/post","method":"POST","body":{"hello":"world"}}}'

# cron job every 10s (Spring 6-field cron)
curl -X POST localhost:8080/api/v1/jobs -H 'Content-Type: application/json' -d '{
  "tenantId":"acme","name":"cron-ping","jobType":"HTTP","scheduleType":"CRON","cronExpr":"*/10 * * * * *",
  "misfirePolicy":"FIRE_NOW","payload":{"url":"https://httpbin.org/get","method":"GET"}}'

curl -X PUT  localhost:8080/api/v1/jobs/{id}/pause
curl -X PUT  localhost:8080/api/v1/jobs/{id}/resume
curl -X POST localhost:8080/api/v1/jobs/{id}/trigger
curl        localhost:8080/api/v1/jobs/{id}/history
curl -X DELETE localhost:8080/api/v1/jobs/{id}
```

## Deployment roles
One codebase, split into services by toggling `scheduler.roles.{api,engine,worker,recovery}`
(env: `SCHEDULER_ROLES_WORKER=false`, etc.). Run N replicas of each; all are active-active.

## Flow
Loader (DB, SKIP LOCKED) -> Redis `sched:{slot}:due` -> Picker (atomic Lua pop to `inflight`) -> Kafka `job.dispatch`
-> Worker claims row (unique key) -> Redisson lock -> executor -> fenced status update.
Retries re-enter the ZSET with exponential backoff; exhausted runs go to `job.dlt`.
Dead workers are detected by lease expiry and reclaimed by the recovery role.

## Known gaps (be aware before production)
- Not compiled or run in the environment that generated it: no Maven repository access. Run `mvn test` first and expect to fix small compile or wiring issues.
- No separate audit service or `job.execution.events` topic; history is written directly to PostgreSQL.
- No SQL executor; SHELL executor is off by default and allow-list only.
- No authentication, tenant isolation on the API, or table partitioning for `job_execution_audit`.
- Kafka listener uses Spring's default error handler; configure a `DefaultErrorHandler` with backoff for poison messages.
