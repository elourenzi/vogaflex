-- Backfill de KPI base
-- Objetivo: popular public.conversations e public.messages a partir de
-- public.semclick_conversations e public.semclick_messages.
--
-- Modo: seguro/incremental (UPDATE para preencher lacunas + INSERT de faltantes)
-- Recomendado: executar em janela de manutencao.

BEGIN;

SET LOCAL lock_timeout = '5s';
SET LOCAL statement_timeout = '0';

-- 0) Sanidade
SELECT current_database() AS db, current_schema() AS schema, current_user AS usr;

-- 1) Validacao de pre-requisitos
DO $$
BEGIN
  IF to_regclass('public.conversations') IS NULL THEN
    RAISE EXCEPTION 'Tabela alvo ausente: public.conversations';
  END IF;
  IF to_regclass('public.messages') IS NULL THEN
    RAISE EXCEPTION 'Tabela alvo ausente: public.messages';
  END IF;
  IF to_regclass('public.semclick_conversations') IS NULL THEN
    RAISE EXCEPTION 'Tabela fonte ausente: public.semclick_conversations';
  END IF;
  IF to_regclass('public.semclick_messages') IS NULL THEN
    RAISE EXCEPTION 'Tabela fonte ausente: public.semclick_messages';
  END IF;
END $$;

-- 2) Backup rapido (snapshot)
DO $$
DECLARE
  ts TEXT := to_char(now(), 'YYYYMMDD_HH24MISS');
BEGIN
  EXECUTE format('CREATE TABLE IF NOT EXISTS public.conversations_bkp_%s AS TABLE public.conversations', ts);
  EXECUTE format('CREATE TABLE IF NOT EXISTS public.messages_bkp_%s AS TABLE public.messages', ts);
END $$;

-- 3) Backfill de conversations
WITH src AS (
  SELECT
    sc.chat_id::text AS chat_id_txt,
    to_jsonb(sc) AS j
  FROM public.semclick_conversations sc
  WHERE sc.chat_id IS NOT NULL
),
norm AS (
  SELECT
    CASE
      WHEN chat_id_txt ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        THEN chat_id_txt::uuid
      ELSE NULL
    END AS chat_id,
    NULLIF(BTRIM(COALESCE(j->>'attendant_name', j->>'vendedor_nome', j->>'current_attendant_name', '')), '') AS attendant_name,
    NULLIF(BTRIM(COALESCE(j->>'current_funnel_stage', j->>'status_conversa', j->>'status', '')), '') AS current_funnel_stage,
    CASE
      WHEN NULLIF(BTRIM(COALESCE(j->>'start_time', j->>'data_criacao_chat', j->>'created_at', '')), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN NULLIF(BTRIM(COALESCE(j->>'start_time', j->>'data_criacao_chat', j->>'created_at', '')), '')::timestamptz
      ELSE NULL
    END AS start_time,
    CASE
      WHEN NULLIF(BTRIM(COALESCE(j->>'created_at', j->>'data_criacao_chat', j->>'start_time', '')), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN NULLIF(BTRIM(COALESCE(j->>'created_at', j->>'data_criacao_chat', j->>'start_time', '')), '')::timestamptz
      ELSE NULL
    END AS created_at,
    CASE
      WHEN NULLIF(BTRIM(COALESCE(j->>'end_time', j->>'data_fechamento_atual', j->>'data_fechamento', '')), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN NULLIF(BTRIM(COALESCE(j->>'end_time', j->>'data_fechamento_atual', j->>'data_fechamento', '')), '')::timestamptz
      ELSE NULL
    END AS end_time,
    CASE
      WHEN NULLIF(BTRIM(COALESCE(j->>'ai_agent_rating', '')), '') IS NULL
        THEN NULL
      ELSE
        CASE
          WHEN REPLACE(REGEXP_REPLACE(COALESCE(j->>'ai_agent_rating', ''), '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN REPLACE(REGEXP_REPLACE(COALESCE(j->>'ai_agent_rating', ''), '[^0-9,.-]', '', 'g'), ',', '.')::numeric
          WHEN REPLACE(REPLACE(REGEXP_REPLACE(COALESCE(j->>'ai_agent_rating', ''), '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN REPLACE(REPLACE(REGEXP_REPLACE(COALESCE(j->>'ai_agent_rating', ''), '[^0-9,.-]', '', 'g'), '.', ''), ',', '.')::numeric
          ELSE NULL
        END
    END AS ai_agent_rating,
    NULLIF(BTRIM(COALESCE(j->>'contact_reason', '')), '') AS contact_reason,
    CASE
      WHEN NULLIF(BTRIM(COALESCE(j->>'valor_orcamento_atual', j->>'valor_orcamento', j->>'budget_value', '')), '') IS NULL
        THEN NULL
      ELSE
        CASE
          WHEN REPLACE(REGEXP_REPLACE(COALESCE(j->>'valor_orcamento_atual', j->>'valor_orcamento', j->>'budget_value', ''), '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN REPLACE(REGEXP_REPLACE(COALESCE(j->>'valor_orcamento_atual', j->>'valor_orcamento', j->>'budget_value', ''), '[^0-9,.-]', '', 'g'), ',', '.')::numeric
          WHEN REPLACE(REPLACE(REGEXP_REPLACE(COALESCE(j->>'valor_orcamento_atual', j->>'valor_orcamento', j->>'budget_value', ''), '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
            THEN REPLACE(REPLACE(REGEXP_REPLACE(COALESCE(j->>'valor_orcamento_atual', j->>'valor_orcamento', j->>'budget_value', ''), '[^0-9,.-]', '', 'g'), '.', ''), ',', '.')::numeric
          ELSE NULL
        END
    END AS budget_value
  FROM src
),
clean AS (
  SELECT *
  FROM norm
  WHERE chat_id IS NOT NULL
),
upd AS (
  UPDATE public.conversations c
  SET
    attendant_name = COALESCE(c.attendant_name, n.attendant_name),
    current_funnel_stage = COALESCE(c.current_funnel_stage, n.current_funnel_stage),
    start_time = COALESCE(c.start_time, n.start_time),
    created_at = COALESCE(c.created_at, n.created_at),
    end_time = COALESCE(c.end_time, n.end_time),
    contact_reason = COALESCE(c.contact_reason, n.contact_reason)
  FROM clean n
  WHERE c.chat_id::text = n.chat_id::text
  RETURNING 1
),
upd_budget AS (
  UPDATE public.conversations c
  SET budget_value = n.budget_value
  FROM clean n
  WHERE c.chat_id::text = n.chat_id::text
    AND c.budget_value IS NULL
    AND n.budget_value IS NOT NULL
  RETURNING 1
),
upd_rating AS (
  UPDATE public.conversations c
  SET ai_agent_rating = n.ai_agent_rating
  FROM clean n
  WHERE c.chat_id::text = n.chat_id::text
    AND c.ai_agent_rating IS NULL
    AND n.ai_agent_rating IS NOT NULL
  RETURNING 1
),
ins AS (
  INSERT INTO public.conversations (
    chat_id,
    attendant_name,
    current_funnel_stage,
    start_time,
    created_at,
    end_time,
    budget_value,
    ai_agent_rating,
    contact_reason
  )
  SELECT
    n.chat_id,
    n.attendant_name,
    n.current_funnel_stage,
    n.start_time,
    COALESCE(n.created_at, n.start_time, NOW()),
    n.end_time,
    n.budget_value,
    n.ai_agent_rating,
    n.contact_reason
  FROM clean n
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.conversations c
    WHERE c.chat_id::text = n.chat_id::text
  )
  RETURNING 1
)
SELECT
  (SELECT COUNT(*) FROM upd) AS conversations_updated,
  (SELECT COUNT(*) FROM upd_budget) AS conversations_budget_updated,
  (SELECT COUNT(*) FROM upd_rating) AS conversations_rating_updated,
  (SELECT COUNT(*) FROM ins) AS conversations_inserted;

-- 4) Backfill de messages
WITH src AS (
  SELECT to_jsonb(sm) AS j
  FROM public.semclick_messages sm
  WHERE sm.chat_id IS NOT NULL
),
norm AS (
  SELECT
    CASE
      WHEN NULLIF(BTRIM(COALESCE(j->>'chat_id', '')), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
        THEN NULLIF(BTRIM(COALESCE(j->>'chat_id', '')), '')
      ELSE NULL
    END AS chat_id,
    COALESCE(
      NULLIF(BTRIM(COALESCE(j->>'message_id', '')), ''),
      NULLIF(BTRIM(COALESCE(j->>'id', '')), ''),
      (
        substr(md5(COALESCE(j->>'chat_id', '') || '|' || COALESCE(j->>'message_time', j->>'timestamp', j->>'created_at', '') || '|' || COALESCE(j->>'msg_conteudo', j->>'content', '')), 1, 8) || '-' ||
        substr(md5(COALESCE(j->>'chat_id', '') || '|' || COALESCE(j->>'message_time', j->>'timestamp', j->>'created_at', '') || '|' || COALESCE(j->>'msg_conteudo', j->>'content', '')), 9, 4) || '-' ||
        '4' || substr(md5(COALESCE(j->>'chat_id', '') || '|' || COALESCE(j->>'message_time', j->>'timestamp', j->>'created_at', '') || '|' || COALESCE(j->>'msg_conteudo', j->>'content', '')), 14, 3) || '-' ||
        'a' || substr(md5(COALESCE(j->>'chat_id', '') || '|' || COALESCE(j->>'message_time', j->>'timestamp', j->>'created_at', '') || '|' || COALESCE(j->>'msg_conteudo', j->>'content', '')), 18, 3) || '-' ||
        substr(md5(COALESCE(j->>'chat_id', '') || '|' || COALESCE(j->>'message_time', j->>'timestamp', j->>'created_at', '') || '|' || COALESCE(j->>'msg_conteudo', j->>'content', '')), 21, 12)
      )
    ) AS message_id,
    CASE
      WHEN NULLIF(BTRIM(COALESCE(j->>'message_time', j->>'timestamp', j->>'created_at', '')), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
        THEN NULLIF(BTRIM(COALESCE(j->>'message_time', j->>'timestamp', j->>'created_at', '')), '')::timestamptz
      ELSE NOW()
    END AS "timestamp",
    NULLIF(BTRIM(COALESCE(j->>'msg_conteudo', j->>'content', '')), '') AS content,
    CASE
      WHEN LOWER(COALESCE(j->>'author_type', '')) IN ('client', 'customer', 'contato', 'cliente', 'inbound') THEN TRUE
      WHEN LOWER(COALESCE(j->>'author_type', '')) IN ('agent', 'attendant', 'vendedor', 'seller', 'outbound', 'system', 'bot') THEN FALSE
      WHEN LOWER(COALESCE(j->>'msg_direcao', '')) IN ('inbound', 'recebida', 'received') THEN TRUE
      WHEN LOWER(COALESCE(j->>'msg_direcao', '')) IN ('outbound', 'enviada', 'sent') THEN FALSE
      WHEN LOWER(COALESCE(j->>'from_client', '')) IN ('true', 't', '1') THEN TRUE
      WHEN LOWER(COALESCE(j->>'from_client', '')) IN ('false', 'f', '0') THEN FALSE
      ELSE NULL
    END AS from_client,
    NULLIF(BTRIM(COALESCE(j->>'msg_tipo', j->>'message_type', j->>'type', '')), '') AS message_type
  FROM src
),
clean AS (
  SELECT
    chat_id,
    message_id,
    "timestamp",
    content,
    from_client,
    message_type
  FROM norm
  WHERE chat_id IS NOT NULL
),
dedup AS (
  SELECT DISTINCT ON (
    message_id,
    chat_id,
    "timestamp",
    COALESCE(content, ''),
    COALESCE(message_type, ''),
    COALESCE(from_client::text, '')
  )
    message_id,
    chat_id,
    "timestamp",
    content,
    from_client,
    message_type
  FROM clean
  ORDER BY
    message_id,
    chat_id,
    "timestamp" DESC
),
upd AS (
  UPDATE public.messages m
  SET
    chat_id = COALESCE(m.chat_id, d.chat_id),
    "timestamp" = COALESCE(m."timestamp", d."timestamp"),
    content = COALESCE(m.content, d.content),
    from_client = COALESCE(m.from_client, d.from_client),
    message_type = COALESCE(m.message_type, d.message_type)
  FROM dedup d
  WHERE m.message_id::text = d.message_id
  RETURNING 1
),
ins AS (
  INSERT INTO public.messages (
    message_id,
    chat_id,
    "timestamp",
    content,
    from_client,
    message_type
  )
  SELECT
    d.message_id,
    d.chat_id,
    d."timestamp",
    d.content,
    COALESCE(d.from_client, TRUE),
    d.message_type
  FROM dedup d
  WHERE NOT EXISTS (
    SELECT 1
    FROM public.messages m
    WHERE m.message_id::text = d.message_id
  )
  RETURNING 1
)
SELECT
  (SELECT COUNT(*) FROM upd) AS messages_updated,
  (SELECT COUNT(*) FROM ins) AS messages_inserted;

-- 5) Estatisticas finais
ANALYZE public.conversations;
ANALYZE public.messages;

COMMIT;

-- 6) Conferencia rapida
SELECT 'conversations' AS tabela, COUNT(*) AS total FROM public.conversations
UNION ALL
SELECT 'messages', COUNT(*) FROM public.messages
UNION ALL
SELECT 'semclick_conversations', COUNT(*) FROM public.semclick_conversations
UNION ALL
SELECT 'semclick_messages', COUNT(*) FROM public.semclick_messages
ORDER BY 1;
