-- v0050: every task has one immutable name <team_name>-<task_id>, built
-- from the team projection when the task is created. No default and no
-- backfill: this migration runs on empty tables of a deployment recreated
-- from empty schemas and fails with a NOT NULL violation on a database that
-- still holds teams or tasks, by design.
ALTER TABLE teams ADD COLUMN team_name text NOT NULL CHECK (team_name <> '');
ALTER TABLE tasks ADD COLUMN task_name text NOT NULL CHECK (task_name <> '');
ALTER TABLE task_archive ADD COLUMN task_name text NOT NULL CHECK (task_name <> '');

CREATE UNIQUE INDEX tasks_task_name ON tasks (task_name);
CREATE UNIQUE INDEX task_archive_task_name ON task_archive (task_name);

-- A task keeps the name it was stored with; the archive is already
-- append-only.
CREATE FUNCTION tasks_task_name_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.task_name IS DISTINCT FROM OLD.task_name THEN
        RAISE EXCEPTION 'tasks.task_name is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER tasks_task_name_immutable BEFORE UPDATE OF task_name ON tasks
    FOR EACH ROW EXECUTE FUNCTION tasks_task_name_immutable();
