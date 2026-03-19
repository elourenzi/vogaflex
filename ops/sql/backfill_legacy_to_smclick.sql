-- ============================================================
-- Backfill: conversations → smclick_chat, messages → smclick_message
-- One-time migration to consolidate all data into smclick tables.
-- Only inserts records that don't already exist in smclick.
-- ============================================================

BEGIN;

-- ── 1. Backfill conversations → smclick_chat ────────────────
-- Only chat_ids that are valid UUIDs and don't exist in smclick_chat

INSERT INTO public.smclick_chat (
    chat_id,
    status,
    current_stage,
    department_name,
    contact_name,
    contact_phone,
    attendant_name,
    budget_value,
    chat_created_at,
    chat_updated_at,
    last_event_at,
    inserted_at,
    refreshed_at
)
SELECT
    public.smclick_try_uuid(c.chat_id::text) AS chat_id,
    CASE
        WHEN LOWER(COALESCE(c.current_funnel_stage, '')) IN ('finalizado', 'finished', 'closed') THEN 'finished'
        WHEN LOWER(COALESCE(c.current_funnel_stage, '')) IN ('lixo') THEN 'closed'
        ELSE 'active'
    END AS status,
    c.current_funnel_stage AS current_stage,
    NULLIF(BTRIM(COALESCE(c.department_name, '')), '') AS department_name,
    c.contact_name,
    c.contact_phone,
    c.attendant_name,
    c.budget_value,
    COALESCE(c.start_time, c.created_at) AS chat_created_at,
    CASE
        WHEN LOWER(COALESCE(c.current_funnel_stage, '')) IN ('finalizado', 'finished', 'closed')
        THEN COALESCE(c.end_time, c.updated_at)
        ELSE NULL
    END AS chat_updated_at,
    COALESCE(c.updated_at, c.created_at) AS last_event_at,
    COALESCE(c.created_at, NOW()) AS inserted_at,
    COALESCE(c.updated_at, NOW()) AS refreshed_at
FROM public.conversations c
WHERE public.smclick_try_uuid(c.chat_id::text) IS NOT NULL
  AND NOT EXISTS (
      SELECT 1 FROM public.smclick_chat sc
      WHERE sc.chat_id = public.smclick_try_uuid(c.chat_id::text)
  )
ON CONFLICT (chat_id) DO NOTHING;

-- ── 2. Backfill messages → smclick_message ──────────────────
-- Only messages whose chat_id exists in smclick_chat (after step 1)

INSERT INTO public.smclick_message (
    chat_id,
    message_id,
    event_time,
    sent_at,
    message_type,
    from_me,
    content_text,
    first_seen_at,
    last_seen_at
)
SELECT
    sc.chat_id,
    COALESCE(
        NULLIF(BTRIM(m.message_id::text), ''),
        md5(m.chat_id::text || '|' || COALESCE(m."timestamp"::text, '') || '|' || COALESCE(m.content, ''))
    ) AS message_id,
    m."timestamp" AS event_time,
    m."timestamp" AS sent_at,
    m.message_type,
    NOT COALESCE(m.from_client, true) AS from_me,
    m.content AS content_text,
    COALESCE(m."timestamp", NOW()) AS first_seen_at,
    COALESCE(m."timestamp", NOW()) AS last_seen_at
FROM public.messages m
JOIN public.smclick_chat sc ON sc.chat_id = public.smclick_try_uuid(m.chat_id::text)
WHERE public.smclick_try_uuid(m.chat_id::text) IS NOT NULL
ON CONFLICT (chat_id, message_id) DO NOTHING;

COMMIT;
