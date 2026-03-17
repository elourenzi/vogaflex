-- Performance boost: bot_transfers table + missing indexes
-- Run on VPS: psql "$DATABASE_URL" < ops/sql/performance_boost.sql
-- Date: 2026-03-16

BEGIN;

-- ─── 1. Bot transfers table ───────────────────────────────────────
-- Pre-computed earliest bot→vendor transfer timestamp per chat.
-- Replaces 6× full-table ILIKE scans in dashboard_api.

CREATE TABLE IF NOT EXISTS public.bot_transfers (
  chat_id TEXT PRIMARY KEY,
  transfer_ts TIMESTAMPTZ NOT NULL,
  source TEXT NOT NULL DEFAULT 'backfill',
  detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ─── 2. Missing indexes on smclick_chat ───────────────────────────

CREATE INDEX IF NOT EXISTS idx_smclick_chat_created
  ON public.smclick_chat (chat_created_at);

CREATE INDEX IF NOT EXISTS idx_smclick_chat_attendant
  ON public.smclick_chat (attendant_name)
  WHERE attendant_name IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_smclick_chat_status
  ON public.smclick_chat (status);

-- ─── 3. Missing indexes on messages ───────────────────────────────

CREATE INDEX IF NOT EXISTS idx_messages_chat_ts
  ON public.messages (chat_id, "timestamp");

CREATE INDEX IF NOT EXISTS idx_messages_chat_from_ts
  ON public.messages (chat_id, from_client, "timestamp");

-- ─── 4. Partial index on smclick_message for bot detection ────────

CREATE INDEX IF NOT EXISTS idx_smclick_msg_bot_candidates
  ON public.smclick_message (chat_id, event_time)
  WHERE from_me = true AND sent_by_name IS NULL;

-- ─── 5. Backfill bot_transfers from existing data ─────────────────

INSERT INTO bot_transfers (chat_id, transfer_ts, source)
SELECT chat_id, MIN(bot_ts), 'backfill'
FROM (
  SELECT m.chat_id, m."timestamp" AS bot_ts
  FROM messages m
  WHERE m.from_client = false AND m.content IS NOT NULL
    AND (
      m.content ILIKE '%atendimento ao nosso setor de vendas%'
      OR m.content ILIKE '%atendimento ao nosso time de vendas%'
      OR m.content ILIKE '%encaminhar ao nosso time de vendas%'
    )

  UNION ALL

  SELECT sm.chat_id::text, sm.event_time AS bot_ts
  FROM smclick_message sm
  WHERE sm.from_me = true AND sm.sent_by_name IS NULL
    AND sm.content_text IS NOT NULL
    AND (
      sm.content_text ILIKE '%atendimento ao nosso setor de vendas%'
      OR sm.content_text ILIKE '%atendimento ao nosso time de vendas%'
      OR sm.content_text ILIKE '%encaminhar ao nosso time de vendas%'
    )
) _be
GROUP BY chat_id
ON CONFLICT (chat_id) DO UPDATE SET
  transfer_ts = LEAST(bot_transfers.transfer_ts, EXCLUDED.transfer_ts),
  detected_at = NOW();

COMMIT;
