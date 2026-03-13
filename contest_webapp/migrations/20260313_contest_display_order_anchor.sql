ALTER TABLE contest_entries
ADD COLUMN IF NOT EXISTS display_order_anchor TIMESTAMPTZ;

UPDATE contest_entries
SET display_order_anchor = COALESCE(display_order_anchor, submitted_at, created_at, NOW())
WHERE status = 'approved'
  AND COALESCE(is_deleted, FALSE) = FALSE
  AND display_order_anchor IS NULL;

CREATE OR REPLACE FUNCTION set_contest_entry_display_order_anchor()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.status = 'approved' AND COALESCE(NEW.is_deleted, FALSE) = FALSE THEN
        IF TG_OP = 'INSERT' THEN
            IF NEW.display_order_anchor IS NULL THEN
                NEW.display_order_anchor := NOW();
            END IF;
        ELSIF OLD.status <> 'approved'
            OR COALESCE(OLD.is_deleted, FALSE) = TRUE
            OR OLD.display_order_anchor IS NULL THEN
            NEW.display_order_anchor := NOW();
        END IF;
    END IF;
    RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS trg_contest_entry_display_order_anchor ON contest_entries;

CREATE TRIGGER trg_contest_entry_display_order_anchor
BEFORE INSERT OR UPDATE OF status, is_deleted ON contest_entries
FOR EACH ROW
EXECUTE FUNCTION set_contest_entry_display_order_anchor();