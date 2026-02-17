-- Schedule incremental canonical sync from ingest buffer
-- Date: 2026-02-17
--
-- What this script does:
-- 1) Creates a safe job wrapper with advisory lock (no overlapping runs)
-- 2) Registers a pg_cron job (if pg_cron is available)
--
-- Run in DBeaver:
-- - Execute full script once.
-- - If pg_cron is unavailable, keep using n8n scheduler.

BEGIN;

DO $$
BEGIN
  IF to_regclass('public.smclick_ingest_buffer') IS NULL THEN
    RAISE EXCEPTION 'Table missing: public.smclick_ingest_buffer';
  END IF;

  IF to_regprocedure('public.smclick_sync_from_ingest_buffer(integer)') IS NULL THEN
    RAISE EXCEPTION 'Function missing: public.smclick_sync_from_ingest_buffer(integer)';
  END IF;
END $$;

-- Job wrapper to process multiple batches per run without overlap
CREATE OR REPLACE FUNCTION public.smclick_sync_from_ingest_buffer_job(
  p_batch_size integer DEFAULT 500,
  p_max_rounds integer DEFAULT 20
)
RETURNS TABLE(
  rounds integer,
  total_events integer,
  total_chats integer,
  total_messages integer,
  total_attendants integer,
  total_segments integer,
  skipped_by_lock boolean
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_round integer := 0;
  v_evt integer := 0;
  v_chat integer := 0;
  v_msg integer := 0;
  v_att integer := 0;
  v_seg integer := 0;
  v_row record;
  v_lock_key bigint := 92745132017533129;
  v_locked boolean := false;
BEGIN
  IF COALESCE(p_batch_size, 0) <= 0 THEN
    RAISE EXCEPTION 'p_batch_size must be > 0';
  END IF;
  IF COALESCE(p_max_rounds, 0) <= 0 THEN
    RAISE EXCEPTION 'p_max_rounds must be > 0';
  END IF;

  v_locked := pg_try_advisory_lock(v_lock_key);
  IF NOT v_locked THEN
    RETURN QUERY SELECT 0, 0, 0, 0, 0, 0, true;
    RETURN;
  END IF;

  BEGIN
    LOOP
      v_round := v_round + 1;

      SELECT *
      INTO v_row
      FROM public.smclick_sync_from_ingest_buffer(p_batch_size);

      v_evt := v_evt + COALESCE(v_row.events_loaded, 0);
      v_chat := v_chat + COALESCE(v_row.chats_upserted, 0);
      v_msg := v_msg + COALESCE(v_row.messages_upserted, 0);
      v_att := v_att + COALESCE(v_row.attendants_upserted, 0);
      v_seg := v_seg + COALESCE(v_row.segments_upserted, 0);

      EXIT WHEN COALESCE(v_row.events_loaded, 0) = 0 OR v_round >= p_max_rounds;
    END LOOP;

    PERFORM pg_advisory_unlock(v_lock_key);
    RETURN QUERY SELECT v_round, v_evt, v_chat, v_msg, v_att, v_seg, false;
  EXCEPTION
    WHEN OTHERS THEN
      PERFORM pg_advisory_unlock(v_lock_key);
      RAISE;
  END;
END;
$$;

COMMIT;

-- Optional: install pg_cron (requires server support)
-- CREATE EXTENSION IF NOT EXISTS pg_cron;

-- Register cron job if pg_cron exists
DO $$
DECLARE
  v_jobid bigint;
BEGIN
  IF to_regclass('cron.job') IS NULL THEN
    RAISE NOTICE 'pg_cron is not available. Keep using n8n Schedule Trigger.';
    RETURN;
  END IF;

  SELECT j.jobid
  INTO v_jobid
  FROM cron.job j
  WHERE j.jobname = 'smclick-sync-every-minute'
  LIMIT 1;

  IF v_jobid IS NOT NULL THEN
    PERFORM cron.unschedule(v_jobid);
  END IF;

  PERFORM cron.schedule(
    'smclick-sync-every-minute',
    '* * * * *',
    $job$SELECT * FROM public.smclick_sync_from_ingest_buffer_job(500, 20);$job$
  );

  RAISE NOTICE 'Job smclick-sync-every-minute created/updated.';
END $$;

-- Manual run (test)
-- SELECT * FROM public.smclick_sync_from_ingest_buffer_job(500, 20);

-- Verify pg_cron registration
-- SELECT jobid, jobname, schedule, command, active
-- FROM cron.job
-- WHERE jobname = 'smclick-sync-every-minute';

-- Verify latest runs (if pg_cron exists)
-- SELECT *
-- FROM cron.job_run_details
-- WHERE jobid = (SELECT jobid FROM cron.job WHERE jobname = 'smclick-sync-every-minute')
-- ORDER BY start_time DESC
-- LIMIT 20;
