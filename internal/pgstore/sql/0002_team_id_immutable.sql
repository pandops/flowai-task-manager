-- A current task keeps the team_id it was stored with: no write, by the
-- service or anyone else, may move it to another team (service-platform,
-- team-owned parent keeps its team). The archive is already append-only.
CREATE FUNCTION tasks_team_id_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF NEW.team_id IS DISTINCT FROM OLD.team_id THEN
        RAISE EXCEPTION 'tasks.team_id is immutable' USING ERRCODE = 'integrity_constraint_violation';
    END IF;
    RETURN NEW;
END
$$;

CREATE TRIGGER tasks_team_id_immutable BEFORE UPDATE OF team_id ON tasks
    FOR EACH ROW EXECUTE FUNCTION tasks_team_id_immutable();
