-- Canonical SmClick pipeline (lean version)
-- Goal: consolidate full chat/message data from smclick_ingest_buffer
-- into stable tables for KPI and chat history.
-- Date: 2026-02-17

BEGIN;

CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE EXTENSION IF NOT EXISTS unaccent;

DO $$
BEGIN
  IF to_regclass('public.smclick_ingest_buffer') IS NULL THEN
    RAISE EXCEPTION 'Table missing: public.smclick_ingest_buffer';
  END IF;
END $$;

-- Safe helpers
CREATE OR REPLACE FUNCTION public.smclick_try_uuid(p_text text)
RETURNS uuid
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN NULLIF(BTRIM(p_text), '') ~* '^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
      THEN NULLIF(BTRIM(p_text), '')::uuid
    ELSE NULL
  END;
$$;

CREATE OR REPLACE FUNCTION public.smclick_try_timestamptz(p_text text)
RETURNS timestamptz
LANGUAGE sql
IMMUTABLE
AS $$
  SELECT CASE
    WHEN NULLIF(BTRIM(p_text), '') ~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'
      THEN NULLIF(BTRIM(p_text), '')::timestamptz
    ELSE NULL
  END;
$$;

CREATE OR REPLACE FUNCTION public.smclick_parse_numeric(p_text text)
RETURNS numeric
LANGUAGE sql
IMMUTABLE
AS $$
  WITH raw AS (
    SELECT NULLIF(BTRIM(COALESCE(p_text, '')), '') AS v
  )
  SELECT CASE
    WHEN v IS NULL THEN NULL
    WHEN REPLACE(REGEXP_REPLACE(v, '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
      THEN REPLACE(REGEXP_REPLACE(v, '[^0-9,.-]', '', 'g'), ',', '.')::numeric
    WHEN REPLACE(REPLACE(REGEXP_REPLACE(v, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') ~ '^-?[0-9]+(\.[0-9]+)?$'
      THEN REPLACE(REPLACE(REGEXP_REPLACE(v, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.')::numeric
    ELSE NULL
  END
  FROM raw;
$$;

-- Canonical tables
CREATE TABLE IF NOT EXISTS public.smclick_event_log (
  id BIGSERIAL PRIMARY KEY,
  source_buffer_id BIGINT NOT NULL UNIQUE,
  source_payload_hash TEXT,
  event_name TEXT,
  event_time TIMESTAMPTZ,
  chat_id UUID,
  message_id TEXT,
  payload JSONB NOT NULL,
  payload_sha256 TEXT NOT NULL UNIQUE,
  received_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  applied_at TIMESTAMPTZ,
  apply_error TEXT
);

CREATE TABLE IF NOT EXISTS public.smclick_chat (
  chat_id UUID PRIMARY KEY,
  protocol BIGINT,
  status TEXT,
  flow TEXT,
  department_name TEXT,
  current_stage TEXT,
  finish_reason TEXT,
  contact_name TEXT,
  contact_phone TEXT,
  attendant_name TEXT,
  attendant_email TEXT,
  budget_value NUMERIC(18,2),
  order_value NUMERIC(18,2),
  product TEXT,
  loss_reason TEXT,
  chat_created_at TIMESTAMPTZ,
  chat_updated_at TIMESTAMPTZ,
  last_event_at TIMESTAMPTZ,
  last_message_id TEXT,
  last_payload JSONB,
  inserted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS public.smclick_message (
  chat_id UUID NOT NULL,
  message_id TEXT NOT NULL,
  event_time TIMESTAMPTZ,
  message_type TEXT,
  message_stage TEXT,
  from_me BOOLEAN,
  message_status BOOLEAN,
  content_text TEXT,
  content_original_text TEXT,
  sent_at TIMESTAMPTZ,
  sent_by_name TEXT,
  sent_by_email TEXT,
  fail_reason TEXT,
  payload JSONB,
  first_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (chat_id, message_id),
  FOREIGN KEY (chat_id) REFERENCES public.smclick_chat(chat_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.smclick_chat_attendant (
  chat_id UUID NOT NULL,
  attendant_key TEXT NOT NULL,
  attendant_id UUID,
  attendant_name TEXT,
  attendant_email TEXT,
  principal BOOLEAN,
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw JSONB,
  PRIMARY KEY (chat_id, attendant_key),
  FOREIGN KEY (chat_id) REFERENCES public.smclick_chat(chat_id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS public.smclick_chat_segment (
  chat_id UUID NOT NULL,
  field_key TEXT NOT NULL,
  field_name TEXT,
  field_type TEXT,
  field_position INTEGER,
  content_text TEXT,
  content_numeric NUMERIC(18,2),
  last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  raw JSONB,
  PRIMARY KEY (chat_id, field_key),
  FOREIGN KEY (chat_id) REFERENCES public.smclick_chat(chat_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS smclick_event_log_pending_apply_idx
  ON public.smclick_event_log (id)
  WHERE applied_at IS NULL;

CREATE INDEX IF NOT EXISTS smclick_chat_last_evt_idx
  ON public.smclick_chat (last_event_at DESC);

CREATE INDEX IF NOT EXISTS smclick_message_chat_ts_idx
  ON public.smclick_message (chat_id, event_time ASC);

-- Incremental loader
CREATE OR REPLACE FUNCTION public.smclick_sync_from_ingest_buffer(p_limit integer DEFAULT 1000)
RETURNS TABLE(
  events_loaded integer,
  chats_upserted integer,
  messages_upserted integer,
  attendants_upserted integer,
  segments_upserted integer
)
LANGUAGE plpgsql
AS $$
DECLARE
  v_events integer := 0;
  v_chats integer := 0;
  v_msgs integer := 0;
  v_att integer := 0;
  v_seg integer := 0;
BEGIN
  CREATE TEMP TABLE tmp_smclick_evt ON COMMIT DROP AS
  WITH src AS (
    SELECT
      b.id AS source_buffer_id,
      b.payload_hash AS source_payload_hash,
      b.payload,
      COALESCE(NULLIF(BTRIM(b.event_name), ''), NULLIF(BTRIM(b.payload->>'event'), '')) AS event_name,
      COALESCE(b.event_time, public.smclick_try_timestamptz(b.payload->>'event_time'), b.received_at) AS event_time,
      COALESCE(b.chat_id, public.smclick_try_uuid(b.payload #>> '{infos,chat,id}')) AS chat_id,
      COALESCE(b.message_id::text, NULLIF(BTRIM(b.payload #>> '{infos,message,id}'), '')) AS message_id
    FROM public.smclick_ingest_buffer b
    LEFT JOIN public.smclick_event_log l ON l.source_buffer_id = b.id
    WHERE l.source_buffer_id IS NULL
    ORDER BY b.id
    LIMIT p_limit
  ),
  ins AS (
    INSERT INTO public.smclick_event_log (
      source_buffer_id, source_payload_hash, event_name, event_time, chat_id, message_id, payload, payload_sha256
    )
    SELECT
      s.source_buffer_id, s.source_payload_hash, s.event_name, s.event_time, s.chat_id, s.message_id, s.payload,
      encode(digest(s.payload::text, 'sha256'), 'hex')
    FROM src s
    ON CONFLICT DO NOTHING
    RETURNING id, source_buffer_id, event_name, event_time, chat_id, message_id, payload
  )
  SELECT * FROM ins;

  SELECT COUNT(*) INTO v_events FROM tmp_smclick_evt;
  IF v_events = 0 THEN
    RETURN QUERY SELECT 0, 0, 0, 0, 0;
    RETURN;
  END IF;

  INSERT INTO public.smclick_chat (
    chat_id, protocol, status, flow, department_name, current_stage, finish_reason,
    contact_name, contact_phone, attendant_name, attendant_email,
    budget_value, order_value, product, loss_reason,
    chat_created_at, chat_updated_at, last_event_at, last_message_id, last_payload, refreshed_at
  )
  SELECT
    e.chat_id,
    CASE
      WHEN NULLIF(BTRIM(e.payload #>> '{infos,chat,protocol}'), '') ~ '^[0-9]+$'
        THEN NULLIF(BTRIM(e.payload #>> '{infos,chat,protocol}'), '')::bigint
      ELSE NULL
    END,
    NULLIF(BTRIM(e.payload #>> '{infos,chat,status}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,flow}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,department,name}'), ''),
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,chat,crm_column,name}'), ''), seg_stage.content_text),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,finish_reason,reason}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,contact,name}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,contact,telephone}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,name}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,email}'), ''),
    seg_budget.content_numeric,
    seg_order.content_numeric,
    seg_product.content_text,
    seg_loss.content_text,
    public.smclick_try_timestamptz(e.payload #>> '{infos,chat,created_at}'),
    public.smclick_try_timestamptz(e.payload #>> '{infos,chat,updated_at}'),
    e.event_time,
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,message,id}'), ''), NULLIF(BTRIM(e.payload #>> '{infos,chat,last_message,id}'), '')),
    e.payload #> '{infos,chat}',
    NOW()
  FROM (
    SELECT DISTINCT ON (t.chat_id)
      t.*
    FROM tmp_smclick_evt t
    WHERE t.chat_id IS NOT NULL
    ORDER BY t.chat_id, t.event_time DESC NULLS LAST, t.source_buffer_id DESC
  ) e
  LEFT JOIN LATERAL (
    SELECT
      NULLIF(BTRIM(seg->>'content'), '') AS content_text,
      public.smclick_parse_numeric(seg->>'content') AS content_numeric
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array'
          THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%orcamento%'
    LIMIT 1
  ) seg_budget ON TRUE
  LEFT JOIN LATERAL (
    SELECT public.smclick_parse_numeric(seg->>'content') AS content_numeric
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array'
          THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%pedido%'
    LIMIT 1
  ) seg_order ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array'
          THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%produto%'
    LIMIT 1
  ) seg_product ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array'
          THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%motivo de perda%'
    LIMIT 1
  ) seg_loss ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(
      CASE
        WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array'
          THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)
        ELSE '[]'::jsonb
      END
    ) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%etapa%'
      AND unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%funil%'
    LIMIT 1
  ) seg_stage ON TRUE
  WHERE e.chat_id IS NOT NULL
  ON CONFLICT (chat_id) DO UPDATE
  SET
    protocol = COALESCE(public.smclick_chat.protocol, EXCLUDED.protocol),
    status = COALESCE(EXCLUDED.status, public.smclick_chat.status),
    flow = COALESCE(EXCLUDED.flow, public.smclick_chat.flow),
    department_name = COALESCE(EXCLUDED.department_name, public.smclick_chat.department_name),
    current_stage = COALESCE(EXCLUDED.current_stage, public.smclick_chat.current_stage),
    finish_reason = COALESCE(EXCLUDED.finish_reason, public.smclick_chat.finish_reason),
    contact_name = COALESCE(EXCLUDED.contact_name, public.smclick_chat.contact_name),
    contact_phone = COALESCE(EXCLUDED.contact_phone, public.smclick_chat.contact_phone),
    attendant_name = COALESCE(EXCLUDED.attendant_name, public.smclick_chat.attendant_name),
    attendant_email = COALESCE(EXCLUDED.attendant_email, public.smclick_chat.attendant_email),
    budget_value = COALESCE(EXCLUDED.budget_value, public.smclick_chat.budget_value),
    order_value = COALESCE(EXCLUDED.order_value, public.smclick_chat.order_value),
    product = COALESCE(EXCLUDED.product, public.smclick_chat.product),
    loss_reason = COALESCE(EXCLUDED.loss_reason, public.smclick_chat.loss_reason),
    chat_created_at = COALESCE(public.smclick_chat.chat_created_at, EXCLUDED.chat_created_at),
    chat_updated_at = CASE
      WHEN public.smclick_chat.chat_updated_at IS NULL THEN EXCLUDED.chat_updated_at
      WHEN EXCLUDED.chat_updated_at IS NULL THEN public.smclick_chat.chat_updated_at
      ELSE GREATEST(public.smclick_chat.chat_updated_at, EXCLUDED.chat_updated_at)
    END,
    last_event_at = CASE
      WHEN public.smclick_chat.last_event_at IS NULL THEN EXCLUDED.last_event_at
      WHEN EXCLUDED.last_event_at IS NULL THEN public.smclick_chat.last_event_at
      ELSE GREATEST(public.smclick_chat.last_event_at, EXCLUDED.last_event_at)
    END,
    last_message_id = COALESCE(EXCLUDED.last_message_id, public.smclick_chat.last_message_id),
    last_payload = EXCLUDED.last_payload,
    refreshed_at = NOW();
  GET DIAGNOSTICS v_chats = ROW_COUNT;

  INSERT INTO public.smclick_message (
    chat_id, message_id, event_time, message_type, message_stage, from_me, message_status,
    content_text, content_original_text, sent_at, sent_by_name, sent_by_email, fail_reason, payload, last_seen_at
  )
  SELECT
    e.chat_id,
    COALESCE(
      NULLIF(BTRIM(e.payload #>> '{infos,message,id}'), ''),
      md5(COALESCE(e.chat_id::text, '') || '|' || COALESCE(e.payload #>> '{infos,message,sent_at}', '') || '|' || COALESCE(e.payload #>> '{infos,message,content,text}', ''))
    ),
    COALESCE(public.smclick_try_timestamptz(e.payload #>> '{infos,message,sent_at}'), public.smclick_try_timestamptz(e.payload #>> '{infos,message,created_at}'), e.event_time),
    NULLIF(BTRIM(e.payload #>> '{infos,message,type}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,stage}'), ''),
    CASE
      WHEN LOWER(COALESCE(e.payload #>> '{infos,message,from_me}', '')) IN ('true', 't', '1') THEN TRUE
      WHEN LOWER(COALESCE(e.payload #>> '{infos,message,from_me}', '')) IN ('false', 'f', '0') THEN FALSE
      ELSE NULL
    END,
    CASE
      WHEN LOWER(COALESCE(e.payload #>> '{infos,message,status}', '')) IN ('true', 't', '1') THEN TRUE
      WHEN LOWER(COALESCE(e.payload #>> '{infos,message,status}', '')) IN ('false', 'f', '0') THEN FALSE
      ELSE NULL
    END,
    NULLIF(BTRIM(e.payload #>> '{infos,message,content,text}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,content,original_text}'), ''),
    public.smclick_try_timestamptz(e.payload #>> '{infos,message,sent_at}'),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,name}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,email}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,fail_reason}'), ''),
    e.payload #> '{infos,message}',
    COALESCE(e.event_time, NOW())
  FROM (
    SELECT DISTINCT ON (
      t.chat_id,
      COALESCE(
        NULLIF(BTRIM(t.payload #>> '{infos,message,id}'), ''),
        md5(
          COALESCE(t.chat_id::text, '')
          || '|'
          || COALESCE(t.payload #>> '{infos,message,sent_at}', '')
          || '|'
          || COALESCE(t.payload #>> '{infos,message,content,text}', '')
        )
      )
    )
      t.*
    FROM tmp_smclick_evt t
    WHERE t.chat_id IS NOT NULL
      AND t.payload #> '{infos,message}' IS NOT NULL
    ORDER BY
      t.chat_id,
      COALESCE(
        NULLIF(BTRIM(t.payload #>> '{infos,message,id}'), ''),
        md5(
          COALESCE(t.chat_id::text, '')
          || '|'
          || COALESCE(t.payload #>> '{infos,message,sent_at}', '')
          || '|'
          || COALESCE(t.payload #>> '{infos,message,content,text}', '')
        )
      ),
      t.event_time DESC NULLS LAST,
      t.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, message_id) DO UPDATE
  SET
    event_time = COALESCE(EXCLUDED.event_time, public.smclick_message.event_time),
    message_type = COALESCE(EXCLUDED.message_type, public.smclick_message.message_type),
    message_stage = COALESCE(EXCLUDED.message_stage, public.smclick_message.message_stage),
    from_me = COALESCE(EXCLUDED.from_me, public.smclick_message.from_me),
    message_status = COALESCE(EXCLUDED.message_status, public.smclick_message.message_status),
    content_text = COALESCE(EXCLUDED.content_text, public.smclick_message.content_text),
    content_original_text = COALESCE(EXCLUDED.content_original_text, public.smclick_message.content_original_text),
    sent_at = COALESCE(EXCLUDED.sent_at, public.smclick_message.sent_at),
    sent_by_name = COALESCE(EXCLUDED.sent_by_name, public.smclick_message.sent_by_name),
    sent_by_email = COALESCE(EXCLUDED.sent_by_email, public.smclick_message.sent_by_email),
    fail_reason = COALESCE(EXCLUDED.fail_reason, public.smclick_message.fail_reason),
    payload = COALESCE(EXCLUDED.payload, public.smclick_message.payload),
    last_seen_at = EXCLUDED.last_seen_at;
  GET DIAGNOSTICS v_msgs = ROW_COUNT;

  INSERT INTO public.smclick_chat_attendant (
    chat_id, attendant_key, attendant_id, attendant_name, attendant_email, principal, last_seen_at, raw
  )
  SELECT
    e.chat_id,
    e.attendant_key,
    e.attendant_id,
    e.attendant_name,
    e.attendant_email,
    e.principal,
    COALESCE(e.event_time, NOW()),
    e.attendant_raw
  FROM (
    SELECT DISTINCT ON (x.chat_id, x.attendant_key)
      x.*
    FROM (
      SELECT
        t.chat_id,
        t.event_time,
        t.source_buffer_id,
        COALESCE(NULLIF(BTRIM(a->>'id'), ''), md5(COALESCE(a->>'email', '') || '|' || COALESCE(a->>'name', ''))) AS attendant_key,
        public.smclick_try_uuid(a->>'id') AS attendant_id,
        NULLIF(BTRIM(a->>'name'), '') AS attendant_name,
        NULLIF(BTRIM(a->>'email'), '') AS attendant_email,
        CASE WHEN LOWER(COALESCE(a->>'principal', '')) IN ('true', 't', '1') THEN TRUE ELSE FALSE END AS principal,
        a AS attendant_raw
      FROM tmp_smclick_evt t
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(t.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array'
            THEN COALESCE(t.payload #> '{infos,chat,attendant}', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) a
      WHERE t.chat_id IS NOT NULL
    ) x
    ORDER BY x.chat_id, x.attendant_key, x.event_time DESC NULLS LAST, x.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, attendant_key) DO UPDATE
  SET
    attendant_id = COALESCE(EXCLUDED.attendant_id, public.smclick_chat_attendant.attendant_id),
    attendant_name = COALESCE(EXCLUDED.attendant_name, public.smclick_chat_attendant.attendant_name),
    attendant_email = COALESCE(EXCLUDED.attendant_email, public.smclick_chat_attendant.attendant_email),
    principal = COALESCE(EXCLUDED.principal, public.smclick_chat_attendant.principal),
    last_seen_at = EXCLUDED.last_seen_at,
    raw = EXCLUDED.raw;
  GET DIAGNOSTICS v_att = ROW_COUNT;

  INSERT INTO public.smclick_chat_segment (
    chat_id, field_key, field_name, field_type, field_position, content_text, content_numeric, last_seen_at, raw
  )
  SELECT
    e.chat_id,
    e.field_key,
    e.field_name,
    e.field_type,
    e.field_position,
    e.content_text,
    e.content_numeric,
    COALESCE(e.event_time, NOW()),
    e.segment_raw
  FROM (
    SELECT DISTINCT ON (x.chat_id, x.field_key)
      x.*
    FROM (
      SELECT
        t.chat_id,
        t.event_time,
        t.source_buffer_id,
        COALESCE(NULLIF(BTRIM(s->>'id'), ''), md5(s::text)) AS field_key,
        NULLIF(BTRIM(s->>'name'), '') AS field_name,
        NULLIF(BTRIM(s->>'type'), '') AS field_type,
        CASE WHEN NULLIF(BTRIM(s->>'position'), '') ~ '^[0-9]+$' THEN (s->>'position')::int ELSE NULL END AS field_position,
        NULLIF(BTRIM(s->>'content'), '') AS content_text,
        public.smclick_parse_numeric(s->>'content') AS content_numeric,
        s AS segment_raw
      FROM tmp_smclick_evt t
      CROSS JOIN LATERAL jsonb_array_elements(
        CASE
          WHEN jsonb_typeof(COALESCE(t.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array'
            THEN COALESCE(t.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)
          ELSE '[]'::jsonb
        END
      ) s
      WHERE t.chat_id IS NOT NULL
    ) x
    ORDER BY x.chat_id, x.field_key, x.event_time DESC NULLS LAST, x.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, field_key) DO UPDATE
  SET
    field_name = COALESCE(EXCLUDED.field_name, public.smclick_chat_segment.field_name),
    field_type = COALESCE(EXCLUDED.field_type, public.smclick_chat_segment.field_type),
    field_position = COALESCE(EXCLUDED.field_position, public.smclick_chat_segment.field_position),
    content_text = COALESCE(EXCLUDED.content_text, public.smclick_chat_segment.content_text),
    content_numeric = COALESCE(EXCLUDED.content_numeric, public.smclick_chat_segment.content_numeric),
    last_seen_at = EXCLUDED.last_seen_at,
    raw = EXCLUDED.raw;
  GET DIAGNOSTICS v_seg = ROW_COUNT;

  UPDATE public.smclick_event_log e
  SET applied_at = NOW(), apply_error = NULL
  WHERE e.id IN (SELECT id FROM tmp_smclick_evt);

  RETURN QUERY SELECT v_events, v_chats, v_msgs, v_att, v_seg;
END;
$$;

-- Read-only views (future backend switch)
CREATE OR REPLACE VIEW public.vw_smclick_conversations_latest AS
SELECT
  c.chat_id,
  c.protocol::text AS protocolo,
  c.contact_name AS cliente_nome,
  c.contact_phone AS cliente_telefone,
  c.attendant_name AS vendedor_nome,
  c.attendant_email AS vendedor_email,
  c.status AS status_conversa,
  c.current_stage AS etapa_funil,
  c.department_name AS departamento,
  c.current_stage AS coluna_kanban,
  c.chat_created_at AS data_criacao_chat,
  NULL::timestamptz AS data_fechamento,
  c.budget_value AS valor_orcamento,
  c.loss_reason AS motivo_perda,
  c.product AS produto_interesse,
  c.chat_updated_at AS updated_at,
  c.inserted_at AS created_at
FROM public.smclick_chat c;

CREATE OR REPLACE VIEW public.vw_smclick_messages_timeline AS
SELECT
  m.chat_id::text AS chat_id,
  m.message_id,
  COALESCE(m.event_time, m.sent_at) AS evento_timestamp,
  m.message_type AS msg_tipo,
  COALESCE(m.content_original_text, m.content_text) AS msg_conteudo,
  CASE
    WHEN m.from_me IS TRUE THEN FALSE
    WHEN m.from_me IS FALSE THEN TRUE
    ELSE NULL
  END AS msg_from_client,
  CASE
    WHEN m.message_status IS TRUE THEN NULL
    WHEN m.message_status IS FALSE THEN COALESCE(NULLIF(BTRIM(m.fail_reason), ''), 'false')
    ELSE NULL
  END AS msg_status_envio
FROM public.smclick_message m;

ANALYZE public.smclick_event_log;
ANALYZE public.smclick_chat;
ANALYZE public.smclick_message;
ANALYZE public.smclick_chat_attendant;
ANALYZE public.smclick_chat_segment;

COMMIT;

-- Run after install:
-- SELECT * FROM public.smclick_sync_from_ingest_buffer(2000);
