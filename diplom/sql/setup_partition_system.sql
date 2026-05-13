-- ============================================================
-- File: setup_partition_system.sql
-- Purpose: Stored procedures for dynamic partition maintenance
-- ============================================================


-- 0) Base partitioned table initialization

DO $$
BEGIN
    IF to_regclass('public.trains_with_partition') IS NULL THEN
        CREATE TABLE public.trains_with_partition (
            train_id SERIAL,
            train_number VARCHAR(10) NOT NULL,
            departure_station VARCHAR(50) NOT NULL,
            arrival_station VARCHAR(50) NOT NULL,
            departure_time TIMESTAMP NOT NULL,
            train_type VARCHAR(20) NOT NULL,
            distance_km INT NOT NULL,
            ticket_price DECIMAL(10,2) NOT NULL
        ) PARTITION BY LIST (train_type);
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'public.trains_with_partition'::regclass
          AND pg_get_expr(c.relpartbound, c.oid) = 'DEFAULT'
    ) THEN
        CREATE TABLE public.trains_default
            PARTITION OF public.trains_with_partition DEFAULT;
    END IF;
END $$;

-- 1) Operation log table
CREATE TABLE IF NOT EXISTS public.partition_log (
    id              BIGSERIAL PRIMARY KEY,
    operation_type  VARCHAR(50)  NOT NULL,
    partition_name  VARCHAR(255),
    train_type      VARCHAR(255),
    rows_affected   BIGINT       DEFAULT 0,
    status          VARCHAR(20)  DEFAULT 'SUCCESS',
    error_message   TEXT,
    executed_by     VARCHAR(100) DEFAULT 'system',
    executed_at     TIMESTAMPTZ  DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_partition_log_date
    ON public.partition_log(executed_at);
CREATE INDEX IF NOT EXISTS idx_partition_log_status
    ON public.partition_log(status);

-- 2) Stable and safe partition name builder
CREATE OR REPLACE FUNCTION public.build_partition_name(p_train_type TEXT)
RETURNS TEXT
LANGUAGE plpgsql
IMMUTABLE
AS $$
DECLARE
    normalized TEXT;
BEGIN
    normalized := regexp_replace(lower(coalesce(p_train_type, 'null')), '[^a-z0-9]+', '_', 'g');
    normalized := trim(both '_' from normalized);

    IF normalized = '' THEN
        normalized := 'type_' || substr(md5(coalesce(p_train_type, 'null')), 1, 16);
    END IF;

    RETURN left('trains_' || normalized, 63);
END;
$$;

-- 3) Create missing partitions and move rows out of default
CREATE OR REPLACE PROCEDURE public.create_partitions_for_new_types()
LANGUAGE plpgsql
AS $$
DECLARE
    v_type           TEXT;
    v_partition_name TEXT;
    v_rows_moved     BIGINT;
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtext('public.create_partitions_for_new_types')) THEN
        RAISE NOTICE 'Partition creation is already running in another transaction';
        RETURN;
    END IF;

    FOR v_type IN
        SELECT DISTINCT train_type
        FROM public.trains_default
    LOOP
        v_partition_name := public.build_partition_name(v_type);

        BEGIN
            DROP TABLE IF EXISTS temp_move;

            CREATE TEMP TABLE temp_move ON COMMIT DROP AS
            WITH moved AS (
                DELETE FROM public.trains_default
                WHERE train_type IS NOT DISTINCT FROM v_type
                RETURNING *
            )
            SELECT * FROM moved;

            SELECT COUNT(*) INTO v_rows_moved FROM temp_move;
            IF v_rows_moved = 0 THEN
                CONTINUE;
            END IF;

            BEGIN
                EXECUTE format(
                    'CREATE TABLE public.%I PARTITION OF public.trains_with_partition 
                    FOR VALUES IN (%L)',
                    v_partition_name,
                    v_type
                );
            EXCEPTION
                WHEN duplicate_table OR invalid_object_definition THEN
                    NULL;
            END;
            INSERT INTO public.trains_with_partition
            SELECT * FROM temp_move;

            INSERT INTO public.partition_log
                (operation_type, partition_name, train_type, rows_affected, status, executed_by)
            VALUES
                ('CREATE_PARTITION', v_partition_name, v_type, v_rows_moved, 'SUCCESS', 
                'stored_procedure');

        EXCEPTION WHEN OTHERS THEN
            BEGIN
                INSERT INTO public.trains_default SELECT * FROM temp_move;
            EXCEPTION WHEN undefined_table THEN
                NULL;
            END;

            INSERT INTO public.partition_log
                (operation_type, partition_name, train_type, rows_affected, status, 
                error_message, executed_by)
            VALUES
                ('CREATE_PARTITION', v_partition_name, v_type, 0, 'ERROR', SQLERRM, 
                'stored_procedure');

            RAISE WARNING 'Error creating/moving partition "%": %', v_partition_name, 
            SQLERRM;
        END;
    END LOOP;
END;
$$;


-- 4) Archive old partitions
CREATE OR REPLACE PROCEDURE public.archive_old_partitions(
    p_days_threshold INTEGER DEFAULT 365
)
LANGUAGE plpgsql
AS $$
DECLARE
    part          RECORD;
    max_departure TIMESTAMPTZ;
    rows_count    BIGINT;
    archive_name  TEXT;
BEGIN
    IF NOT pg_try_advisory_xact_lock(hashtext('public.archive_old_partitions')) THEN
        RAISE NOTICE 'Partition archival is already running in another transaction';
        RETURN;
    END IF;

    FOR part IN
        SELECT c.relname AS partition_name
        FROM pg_inherits i
        JOIN pg_class c ON c.oid = i.inhrelid
        WHERE i.inhparent = 'public.trains_with_partition'::regclass
          AND c.relname <> 'trains_default'
    LOOP
        EXECUTE format(
            'SELECT MAX(departure_time) FROM public.%I',
            part.partition_name
        ) INTO max_departure;

        IF max_departure IS NULL
           OR max_departure >= NOW() - make_interval(days => p_days_threshold)
        THEN
            CONTINUE;
        END IF;

        archive_name := left('archive_' || part.partition_name, 63);

        BEGIN
            EXECUTE format('SELECT COUNT(*) FROM public.%I', part.partition_name)
            INTO rows_count;

            EXECUTE format(
                'CREATE TABLE IF NOT EXISTS public.%I (LIKE public.%I INCLUDING ALL)',
                archive_name,
                part.partition_name
            );

            EXECUTE format(
                'INSERT INTO public.%I SELECT * FROM public.%I',
                archive_name,
                part.partition_name
            );

            EXECUTE format(
                'ALTER TABLE public.trains_with_partition DETACH PARTITION public.%I',
                part.partition_name
            );

            EXECUTE format('DROP TABLE IF EXISTS public.%I', part.partition_name);

            INSERT INTO public.partition_log (
                operation_type,
                partition_name,
                rows_affected,
                status,
                executed_by
            ) VALUES (
                'ARCHIVE_PARTITION',
                part.partition_name,
                rows_count,
                'SUCCESS',
                'stored_procedure'
            );

        EXCEPTION WHEN OTHERS THEN
            INSERT INTO public.partition_log (
                operation_type,
                partition_name,
                status,
                error_message,
                executed_by
            ) VALUES (
                'ARCHIVE_PARTITION',
                part.partition_name,
                'ERROR',
                SQLERRM,
                'stored_procedure'
            );

            RAISE WARNING 'Error archiving "%": %', part.partition_name, SQLERRM;
        END;
    END LOOP;
END;
$$;

-- 5) Cleanup old partition operation logs
CREATE OR REPLACE PROCEDURE public.cleanup_partition_logs(
    p_retention_days INTEGER DEFAULT 90
)
LANGUAGE plpgsql
AS $$
DECLARE
    deleted_count BIGINT;
BEGIN
    DELETE FROM public.partition_log
    WHERE executed_at < NOW() - make_interval(days => p_retention_days);

    GET DIAGNOSTICS deleted_count = ROW_COUNT;
    RAISE NOTICE 'Cleaned up % old log entries', deleted_count;
END;
$$;
