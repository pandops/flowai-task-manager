# task-manager

task-manager owns current tasks and their attempts: intake deduplication,
fan-out of one Source task to every bound Flow, the FIFO task queues, claim
arbitration and claim tokens, releases, retries, operator cancels and the
append-only task archive. It talks to other services only over NATS and
publishes only through its outbox.

## Subjects

- Served requests (queue group `task-manager`):
  - `task-manager.ui.<op>.sub` from API Gateway: `list-tasks`, `list-archive`,
    `list-executor-tasks`, `get-task`, `get-archived-task`, `list-attempts`,
    `cancel`;
  - `task-manager.admin.list-tasks.sub` from admin-panel;
  - `task-manager.service.claim-current.sub` and
    `task-manager.service.executor-claims.sub`;
  - `task-manager.executor.<executor>.claim-token.sub` from Executors.
- Consumed, through the durable consumers in
  [schemas/consumers.json](schemas/consumers.json):
  `source-controller.task.*.sub` (`task-manager-intake`), and
  `team-registry`, `executor-registry` and Executor events from `ALL_EVENT`
  (`task-manager`).
- Published: `task-manager.task.<team>.<tag>.<flow>.pub` (the task consumer
  `tasks-<team>-<tag_hash>` is ensured first),
  `task-manager.event.<kind>.pub`, `task-manager.audit.<action>.pub` and
  `task-manager.command.<executor>.pub`.
- Called: `flow-registry.service.targets.pub`.

Request, reply and message bodies follow the files in [schemas/](schemas/).
Every state change commits with its outbox rows in one transaction; the
relay publishes them in `publisher_seq` order with `Nats-Msg-Id`
`task-manager-<publisher_seq>`.

## Running

The image runs `/task-manager --config /etc/flowai/config.yaml`; see
[config.example.yaml](config.example.yaml) for every key. NATS needs
`tls://`, the stand CA and operator-mode user credentials; PostgreSQL needs
`sslmode: verify-full`. Migrations run at startup in `task_manager_schema`
(created only when it is missing). The HTTP listener serves only
`/v1/livez` and `/v1/readyz`; readiness requires the broker connection, both
durable consumers and the database.

## Tests

```sh
export FLOWAI_CONTRACT_FIXTURE_DIR=/absolute/path/to/contract-fixtures
GOWORK=off go test -mod=readonly -count=1 ./...
GOWORK=off go test -mod=readonly -count=1 -tags=integration ./test
```

Unit and integration tests live in `test/`. Integration tests use one
disposable PostgreSQL container and the build tag `integration`. Provide
publisher schemas, their referenced definitions and the test ownership map
in a fixture directory outside this checkout, using the baseline-relative paths named
in the tests. Owned schemas resolve from this repository's `schemas/`.
