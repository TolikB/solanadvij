"""Route durable raw events into bounded daily PostgreSQL partitions.

Revision ID: 20260831_0008
Revises: 20260824_0007
"""

import sqlalchemy as sa
from alembic import op

revision = "20260831_0008"
down_revision = "20260824_0007"
branch_labels = None
depends_on = None
RUNTIME_ADVISORY_LOCK_KEY = 0x534E49504552
PARTITION_ADVISORY_LOCK_CLASS_ID = 21328
ENSURE_PARTITION_FUNCTION = "ensure_raw_chain_events_partition"


def _assert_runtime_stopped() -> str:
    bind = op.get_bind()
    dialect = bind.dialect.name
    if dialect == "postgresql":
        owns_migration_lease = bool(
            bind.scalar(
                sa.text(
                    f"SELECT pg_try_advisory_xact_lock({RUNTIME_ADVISORY_LOCK_KEY})"
                )
            )
        )
        if not owns_migration_lease:
            raise RuntimeError(
                "migration 0008 blocked: a bot process owns the runtime lease"
            )
        return dialect
    active_runs = int(
        bind.scalar(
            sa.text("SELECT count(*) FROM system_runs WHERE stopped_at IS NULL")
        )
        or 0
    )
    if active_runs:
        raise RuntimeError(
            "migration 0008 blocked: active system runs must be stopped first"
        )
    return dialect


def upgrade() -> None:
    if _assert_runtime_stopped() != "postgresql":
        return

    op.execute(
        """
        DO $migration$
        DECLARE
            target_date date;
            range_end date;
            partition_name text;
            check_name text;
            expected_rows bigint;
            moved_rows bigint;
            attached_rows bigint;
        BEGIN
            LOCK TABLE public.raw_chain_events IN ACCESS EXCLUSIVE MODE;
            LOCK TABLE public.raw_chain_events_default IN ACCESS EXCLUSIVE MODE;

            FOR target_date IN
                SELECT DISTINCT block_date
                FROM public.raw_chain_events_default
                ORDER BY block_date
            LOOP
                range_end := target_date + 1;
                partition_name :=
                    'raw_chain_events_' || to_char(target_date, 'YYYYMMDD');
                check_name := partition_name || '_block_date_check';

                IF pg_catalog.to_regclass(
                    pg_catalog.format('public.%I', partition_name)
                ) IS NOT NULL THEN
                    RAISE EXCEPTION
                        'migration 0008 blocked: target relation % already exists',
                        partition_name;
                END IF;

                SELECT count(*)
                INTO expected_rows
                FROM public.raw_chain_events_default
                WHERE block_date = target_date;

                EXECUTE pg_catalog.format(
                    'CREATE TABLE public.%I '
                    '(LIKE public.raw_chain_events INCLUDING ALL)',
                    partition_name
                );
                EXECUTE pg_catalog.format(
                    'ALTER TABLE public.%I ADD CONSTRAINT %I '
                    'CHECK (block_date >= DATE %L AND block_date < DATE %L)',
                    partition_name,
                    check_name,
                    target_date,
                    range_end
                );
                EXECUTE pg_catalog.format(
                    'WITH moved AS ('
                    'DELETE FROM public.raw_chain_events_default '
                    'WHERE block_date = %L::date RETURNING *'
                    ') INSERT INTO public.%I SELECT * FROM moved',
                    target_date,
                    partition_name
                );
                GET DIAGNOSTICS moved_rows = ROW_COUNT;
                IF moved_rows <> expected_rows THEN
                    RAISE EXCEPTION
                        'migration 0008 row-count mismatch for %: '
                        'expected %, moved %',
                        target_date,
                        expected_rows,
                        moved_rows;
                END IF;

                EXECUTE pg_catalog.format(
                    'ALTER TABLE public.raw_chain_events '
                    'ATTACH PARTITION public.%I '
                    'FOR VALUES FROM (%L) TO (%L)',
                    partition_name,
                    target_date,
                    range_end
                );
                EXECUTE pg_catalog.format(
                    'SELECT count(*) FROM public.%I',
                    partition_name
                ) INTO attached_rows;
                IF attached_rows <> expected_rows THEN
                    RAISE EXCEPTION
                        'migration 0008 attached-row mismatch for %: '
                        'expected %, found %',
                        target_date,
                        expected_rows,
                        attached_rows;
                END IF;
            END LOOP;

            IF EXISTS (SELECT 1 FROM public.raw_chain_events_default) THEN
                RAISE EXCEPTION
                    'migration 0008 refused to replace a non-empty '
                    'default partition';
            END IF;

        END
        $migration$;
        """
    )
    op.execute(
        f"""
        CREATE FUNCTION public.{ENSURE_PARTITION_FUNCTION}(target_date date)
        RETURNS void
        LANGUAGE plpgsql
        SECURITY DEFINER
        SET search_path = pg_catalog, public
        AS $function$
        DECLARE
            utc_today date :=
                (pg_catalog.statement_timestamp() AT TIME ZONE 'UTC')::date;
            range_end date;
            partition_name text;
            relation_oid regclass;
            is_attached boolean;
        BEGIN
            IF target_date IS NULL THEN
                RAISE EXCEPTION 'raw-event partition date must not be null';
            END IF;
            IF target_date < utc_today - 1 OR target_date > utc_today + 1 THEN
                RAISE EXCEPTION
                    'raw-event partition date % is outside the active UTC window',
                    target_date;
            END IF;

            range_end := target_date + 1;
            partition_name :=
                'raw_chain_events_' ||
                pg_catalog.to_char(target_date, 'YYYYMMDD');
            relation_oid := pg_catalog.to_regclass(
                pg_catalog.format('public.%I', partition_name)
            );
            IF relation_oid IS NOT NULL THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_inherits
                    WHERE inhparent = 'public.raw_chain_events'::regclass
                      AND inhrelid = relation_oid
                ) INTO is_attached;
                IF NOT is_attached THEN
                    RAISE EXCEPTION
                        'raw-event partition relation % exists but is not attached',
                        partition_name;
                END IF;
                RETURN;
            END IF;

            PERFORM pg_catalog.pg_advisory_xact_lock(
                {PARTITION_ADVISORY_LOCK_CLASS_ID},
                target_date - DATE '2000-01-01'
            );
            relation_oid := pg_catalog.to_regclass(
                pg_catalog.format('public.%I', partition_name)
            );
            IF relation_oid IS NOT NULL THEN
                SELECT EXISTS (
                    SELECT 1
                    FROM pg_catalog.pg_inherits
                    WHERE inhparent = 'public.raw_chain_events'::regclass
                      AND inhrelid = relation_oid
                ) INTO is_attached;
                IF NOT is_attached THEN
                    RAISE EXCEPTION
                        'raw-event partition relation % exists but is not attached',
                        partition_name;
                END IF;
                RETURN;
            END IF;

            EXECUTE pg_catalog.format(
                'CREATE TABLE public.%I '
                'PARTITION OF public.raw_chain_events '
                'FOR VALUES FROM (%L) TO (%L)',
                partition_name,
                target_date,
                range_end
            );
        END
        $function$;
        """
    )
    op.execute(
        f"REVOKE ALL ON FUNCTION "
        f"public.{ENSURE_PARTITION_FUNCTION}(date) FROM PUBLIC"
    )


def downgrade() -> None:
    if _assert_runtime_stopped() != "postgresql":
        return

    op.execute(
        f"REVOKE ALL ON FUNCTION "
        f"public.{ENSURE_PARTITION_FUNCTION}(date) FROM PUBLIC"
    )
    op.execute(f"DROP FUNCTION public.{ENSURE_PARTITION_FUNCTION}(date)")
    op.execute(
        """
        DO $migration$
        DECLARE
            partition_schema text;
            partition_name text;
            expected_rows bigint;
            moved_rows bigint;
        BEGIN
            LOCK TABLE public.raw_chain_events IN ACCESS EXCLUSIVE MODE;
            LOCK TABLE public.raw_chain_events_default IN ACCESS EXCLUSIVE MODE;

            IF EXISTS (
                SELECT 1
                FROM pg_catalog.pg_trigger
                WHERE tgrelid IN (
                    'public.raw_chain_events'::regclass,
                    'public.raw_chain_events_default'::regclass
                )
                  AND NOT tgisinternal
            ) THEN
                RAISE EXCEPTION
                    'migration 0008 downgrade blocked: raw-event user triggers exist';
            END IF;

            FOR partition_schema, partition_name IN
                SELECT namespace.nspname, child.relname
                FROM pg_catalog.pg_inherits AS inheritance
                JOIN pg_catalog.pg_class AS parent
                  ON parent.oid = inheritance.inhparent
                JOIN pg_catalog.pg_class AS child
                  ON child.oid = inheritance.inhrelid
                JOIN pg_catalog.pg_namespace AS namespace
                  ON namespace.oid = child.relnamespace
                WHERE parent.oid = 'public.raw_chain_events'::regclass
                  AND child.relname <> 'raw_chain_events_default'
                ORDER BY namespace.nspname, child.relname
            LOOP
                EXECUTE pg_catalog.format(
                    'SELECT count(*) FROM %I.%I',
                    partition_schema,
                    partition_name
                ) INTO expected_rows;
                EXECUTE pg_catalog.format(
                    'ALTER TABLE public.raw_chain_events '
                    'DETACH PARTITION %I.%I',
                    partition_schema,
                    partition_name
                );
                EXECUTE pg_catalog.format(
                    'INSERT INTO public.raw_chain_events_default '
                    'SELECT * FROM %I.%I',
                    partition_schema,
                    partition_name
                );
                GET DIAGNOSTICS moved_rows = ROW_COUNT;
                IF moved_rows <> expected_rows THEN
                    RAISE EXCEPTION
                        'migration 0008 downgrade row-count mismatch for %: '
                        'expected %, moved %',
                        partition_schema || '.' || partition_name,
                        expected_rows,
                        moved_rows;
                END IF;
                EXECUTE pg_catalog.format(
                    'DROP TABLE %I.%I',
                    partition_schema,
                    partition_name
                );
            END LOOP;
        END
        $migration$;
        """
    )
