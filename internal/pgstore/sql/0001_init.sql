-- task-manager schema. Every object is created unqualified, in the schema
-- the connection's search_path names (task_manager_schema).

CREATE SEQUENCE publisher_seq AS bigint MINVALUE 1 START 1;

CREATE TABLE teams (
    team_id       bigint PRIMARY KEY CHECK (team_id > 0),
    archived      boolean NOT NULL DEFAULT false,
    default_image text NOT NULL DEFAULT ''
);

CREATE TABLE executors (
    executor_id      bigint PRIMARY KEY CHECK (executor_id > 0),
    team_id          bigint NOT NULL CHECK (team_id > 0),
    tag              text NOT NULL DEFAULT '',
    removed          boolean NOT NULL DEFAULT false,
    lost_seq_applied bigint NOT NULL DEFAULT 0
);

CREATE TABLE team_counters (
    team_id bigint NOT NULL,
    kind    text NOT NULL,
    last    bigint NOT NULL,
    PRIMARY KEY (team_id, kind)
);

CREATE TABLE consumed (
    publisher_key text NOT NULL,
    seq           bigint NOT NULL,
    PRIMARY KEY (publisher_key, seq)
);

CREATE TABLE tasks (
    team_id        bigint NOT NULL CHECK (team_id > 0),
    task_id        bigint NOT NULL CHECK (task_id > 0),
    source_id      bigint NOT NULL,
    external_id    text NOT NULL,
    flow_id        bigint NOT NULL,
    required_tag   text NOT NULL,
    state          text NOT NULL,
    ingested_at    timestamptz NOT NULL,
    next_due_at    timestamptz,
    held_executor  bigint,
    last_executor  bigint,
    record         jsonb NOT NULL,
    PRIMARY KEY (team_id, task_id),
    UNIQUE (team_id, source_id, external_id, flow_id)
);

CREATE INDEX tasks_order ON tasks (team_id, ingested_at DESC, task_id DESC);
CREATE INDEX tasks_due ON tasks (next_due_at) WHERE next_due_at IS NOT NULL;
CREATE INDEX tasks_held ON tasks (held_executor) WHERE held_executor IS NOT NULL;

CREATE TABLE task_archive (
    team_id        bigint NOT NULL CHECK (team_id > 0),
    task_id        bigint NOT NULL CHECK (task_id > 0),
    source_id      bigint NOT NULL,
    external_id    text NOT NULL,
    flow_id        bigint NOT NULL,
    required_tag   text NOT NULL,
    state          text NOT NULL,
    ingested_at    timestamptz NOT NULL,
    last_executor  bigint,
    archived_at    timestamptz NOT NULL,
    record         jsonb NOT NULL,
    PRIMARY KEY (team_id, task_id)
);

CREATE INDEX task_archive_order ON task_archive (team_id, ingested_at DESC, task_id DESC);

CREATE FUNCTION task_archive_append_only() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'task_archive is append-only';
END;
$$;

CREATE TRIGGER task_archive_append_only BEFORE UPDATE OR DELETE ON task_archive
    FOR EACH ROW EXECUTE FUNCTION task_archive_append_only();

CREATE TABLE controls (
    team_id     bigint NOT NULL,
    control_id  bigint NOT NULL,
    task_id     bigint NOT NULL,
    operator_id bigint NOT NULL,
    action      text NOT NULL,
    status      text NOT NULL,
    created_at  timestamptz NOT NULL,
    PRIMARY KEY (team_id, control_id),
    UNIQUE (team_id, task_id, operator_id, action)
);

CREATE TABLE outbox (
    publisher_seq bigint PRIMARY KEY,
    subject       text NOT NULL,
    header        jsonb NOT NULL,
    data          bytea NOT NULL,
    ensure_team   bigint NOT NULL DEFAULT 0,
    ensure_tag    text NOT NULL DEFAULT ''
);
