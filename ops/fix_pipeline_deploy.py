"""
Deploy the fixed smclick_sync_from_ingest_buffer function with SHA256 dedup fix.
Run: python ops/fix_pipeline_deploy.py
"""
import psycopg2, os, time

DATABASE_URL = os.environ.get(
    "DATABASE_URL",
    "postgres://postgres:da3d6b7ea6af546c3ddc@72.61.132.195:5454/dadosvogaflex?sslmode=disable"
)

FIXED_FUNCTION = r"""
CREATE OR REPLACE FUNCTION public.smclick_sync_from_ingest_buffer(p_limit integer DEFAULT 1000)
RETURNS TABLE(
  events_loaded integer,
  chats_upserted integer,
  messages_upserted integer,
  attendants_upserted integer,
  segments_upserted integer
)
LANGUAGE plpgsql
AS $func$
DECLARE
  v_events integer := 0;
  v_chats integer := 0;
  v_msgs integer := 0;
  v_att integer := 0;
  v_seg integer := 0;
BEGIN
  -- Select buffer rows not yet in event_log (by source_buffer_id),
  -- then deduplicate by SHA256 to skip events already processed via different buffer rows.
  CREATE TEMP TABLE tmp_smclick_evt ON COMMIT DROP AS
  WITH src AS (
    SELECT
      b.id AS source_buffer_id,
      b.payload_hash AS source_payload_hash,
      b.payload,
      COALESCE(NULLIF(BTRIM(b.event_name), ''), NULLIF(BTRIM(b.payload->>'event'), '')) AS event_name,
      COALESCE(b.event_time, public.smclick_try_timestamptz(b.payload->>'event_time'), b.received_at) AS event_time,
      COALESCE(b.chat_id, public.smclick_try_uuid(b.payload #>> '{infos,chat,id}')) AS chat_id,
      COALESCE(b.message_id::text, NULLIF(BTRIM(b.payload #>> '{infos,message,id}'), '')) AS message_id,
      encode(digest(b.payload::text, 'sha256'), 'hex') AS computed_sha256
    FROM public.smclick_ingest_buffer b
    LEFT JOIN public.smclick_event_log l ON l.source_buffer_id = b.id
    WHERE l.source_buffer_id IS NULL
    ORDER BY b.id
    LIMIT p_limit * 10
  ),
  deduped AS (
    SELECT DISTINCT ON (s.computed_sha256) s.*
    FROM src s
    WHERE NOT EXISTS (
      SELECT 1 FROM public.smclick_event_log el WHERE el.payload_sha256 = s.computed_sha256
    )
    ORDER BY s.computed_sha256, s.source_buffer_id
    LIMIT p_limit
  ),
  ins AS (
    INSERT INTO public.smclick_event_log (
      source_buffer_id, source_payload_hash, event_name, event_time, chat_id, message_id, payload, payload_sha256
    )
    SELECT d.source_buffer_id, d.source_payload_hash, d.event_name, d.event_time,
           d.chat_id, d.message_id, d.payload, d.computed_sha256
    FROM deduped d
    ON CONFLICT DO NOTHING
    RETURNING id, source_buffer_id, event_name, event_time, chat_id, message_id, payload
  )
  SELECT * FROM ins;

  SELECT COUNT(*) INTO v_events FROM tmp_smclick_evt;
  IF v_events = 0 THEN
    RETURN QUERY SELECT 0, 0, 0, 0, 0;
    RETURN;
  END IF;

  -- Upsert smclick_chat using principal attendant (not message sender)
  INSERT INTO public.smclick_chat (
    chat_id, protocol, status, flow, department_name, current_stage, finish_reason,
    contact_name, contact_phone, attendant_name, attendant_email,
    budget_value, order_value, product, loss_reason,
    chat_created_at, chat_updated_at, last_event_at, last_message_id, last_payload, refreshed_at
  )
  SELECT
    e.chat_id,
    CASE WHEN NULLIF(BTRIM(e.payload #>> '{infos,chat,protocol}'), '') ~ '^[0-9]+$'
      THEN NULLIF(BTRIM(e.payload #>> '{infos,chat,protocol}'), '')::bigint ELSE NULL END,
    NULLIF(BTRIM(e.payload #>> '{infos,chat,status}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,flow}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,department,name}'), ''),
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,chat,crm_column,name}'), ''), seg_stage.content_text),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,finish_reason,reason}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,contact,name}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,chat,contact,telephone}'), ''),
    COALESCE(
      (SELECT NULLIF(BTRIM(a->>'name'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a WHERE LOWER(COALESCE(a->>'principal', '')) IN ('true', 't', '1') LIMIT 1),
      (SELECT NULLIF(BTRIM(a->>'name'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a LIMIT 1)
    ),
    COALESCE(
      (SELECT NULLIF(BTRIM(a->>'email'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a WHERE LOWER(COALESCE(a->>'principal', '')) IN ('true', 't', '1') LIMIT 1),
      (SELECT NULLIF(BTRIM(a->>'email'), '') FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a LIMIT 1)
    ),
    seg_budget.content_numeric, seg_order.content_numeric, seg_product.content_text, seg_loss.content_text,
    public.smclick_try_timestamptz(e.payload #>> '{infos,chat,created_at}'),
    public.smclick_try_timestamptz(e.payload #>> '{infos,chat,updated_at}'),
    e.event_time,
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,message,id}'), ''), NULLIF(BTRIM(e.payload #>> '{infos,chat,last_message,id}'), '')),
    e.payload #> '{infos,chat}',
    NOW()
  FROM (
    SELECT DISTINCT ON (t.chat_id) t.*
    FROM tmp_smclick_evt t WHERE t.chat_id IS NOT NULL
    ORDER BY t.chat_id, t.event_time DESC NULLS LAST, t.source_buffer_id DESC
  ) e
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text, public.smclick_parse_numeric(seg->>'content') AS content_numeric
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%orcamento%' LIMIT 1
  ) seg_budget ON TRUE
  LEFT JOIN LATERAL (
    SELECT public.smclick_parse_numeric(seg->>'content') AS content_numeric
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%pedido%' LIMIT 1
  ) seg_order ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%produto%' LIMIT 1
  ) seg_product ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%motivo de perda%' LIMIT 1
  ) seg_loss ON TRUE
  LEFT JOIN LATERAL (
    SELECT NULLIF(BTRIM(seg->>'content'), '') AS content_text
    FROM jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(e.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) seg
    WHERE unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%etapa%'
      AND unaccent(lower(COALESCE(seg->>'name', ''))) LIKE '%funil%'
    LIMIT 1
  ) seg_stage ON TRUE
  WHERE e.chat_id IS NOT NULL
  ON CONFLICT (chat_id) DO UPDATE SET
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
    chat_updated_at = CASE WHEN public.smclick_chat.chat_updated_at IS NULL THEN EXCLUDED.chat_updated_at WHEN EXCLUDED.chat_updated_at IS NULL THEN public.smclick_chat.chat_updated_at ELSE GREATEST(public.smclick_chat.chat_updated_at, EXCLUDED.chat_updated_at) END,
    last_event_at = CASE WHEN public.smclick_chat.last_event_at IS NULL THEN EXCLUDED.last_event_at WHEN EXCLUDED.last_event_at IS NULL THEN public.smclick_chat.last_event_at ELSE GREATEST(public.smclick_chat.last_event_at, EXCLUDED.last_event_at) END,
    last_message_id = COALESCE(EXCLUDED.last_message_id, public.smclick_chat.last_message_id),
    last_payload = EXCLUDED.last_payload,
    refreshed_at = NOW();
  GET DIAGNOSTICS v_chats = ROW_COUNT;

  INSERT INTO public.smclick_message (
    chat_id, message_id, event_time, message_type, message_stage, from_me, message_status,
    content_text, content_original_text, sent_at, sent_by_name, sent_by_email, fail_reason, payload, last_seen_at
  )
  SELECT e.chat_id,
    COALESCE(NULLIF(BTRIM(e.payload #>> '{infos,message,id}'), ''), md5(COALESCE(e.chat_id::text, '') || '|' || COALESCE(e.payload #>> '{infos,message,sent_at}', '') || '|' || COALESCE(e.payload #>> '{infos,message,content,text}', ''))),
    COALESCE(public.smclick_try_timestamptz(e.payload #>> '{infos,message,sent_at}'), public.smclick_try_timestamptz(e.payload #>> '{infos,message,created_at}'), e.event_time),
    NULLIF(BTRIM(e.payload #>> '{infos,message,type}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,stage}'), ''),
    CASE WHEN LOWER(COALESCE(e.payload #>> '{infos,message,from_me}', '')) IN ('true', 't', '1') THEN TRUE WHEN LOWER(COALESCE(e.payload #>> '{infos,message,from_me}', '')) IN ('false', 'f', '0') THEN FALSE ELSE NULL END,
    CASE WHEN LOWER(COALESCE(e.payload #>> '{infos,message,status}', '')) IN ('true', 't', '1') THEN TRUE WHEN LOWER(COALESCE(e.payload #>> '{infos,message,status}', '')) IN ('false', 'f', '0') THEN FALSE ELSE NULL END,
    NULLIF(BTRIM(e.payload #>> '{infos,message,content,text}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,content,original_text}'), ''),
    public.smclick_try_timestamptz(e.payload #>> '{infos,message,sent_at}'),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,name}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,sent_by,email}'), ''),
    NULLIF(BTRIM(e.payload #>> '{infos,message,fail_reason}'), ''),
    e.payload #> '{infos,message}',
    COALESCE(e.event_time, NOW())
  FROM (
    SELECT DISTINCT ON (t.chat_id, COALESCE(NULLIF(BTRIM(t.payload #>> '{infos,message,id}'), ''), md5(COALESCE(t.chat_id::text,'') || '|' || COALESCE(t.payload #>> '{infos,message,sent_at}','') || '|' || COALESCE(t.payload #>> '{infos,message,content,text}','')))) t.*
    FROM tmp_smclick_evt t WHERE t.chat_id IS NOT NULL AND t.payload #> '{infos,message}' IS NOT NULL
    ORDER BY t.chat_id, COALESCE(NULLIF(BTRIM(t.payload #>> '{infos,message,id}'), ''), md5(COALESCE(t.chat_id::text,'') || '|' || COALESCE(t.payload #>> '{infos,message,sent_at}','') || '|' || COALESCE(t.payload #>> '{infos,message,content,text}',''))), t.event_time DESC NULLS LAST, t.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, message_id) DO UPDATE SET
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

  INSERT INTO public.smclick_chat_attendant (chat_id, attendant_key, attendant_id, attendant_name, attendant_email, principal, last_seen_at, raw)
  SELECT e.chat_id, e.attendant_key, e.attendant_id, e.attendant_name, e.attendant_email, e.principal, COALESCE(e.event_time, NOW()), e.attendant_raw
  FROM (
    SELECT DISTINCT ON (x.chat_id, x.attendant_key) x.*
    FROM (
      SELECT t.chat_id, t.event_time, t.source_buffer_id,
        COALESCE(NULLIF(BTRIM(a->>'id'), ''), md5(COALESCE(a->>'email', '') || '|' || COALESCE(a->>'name', ''))) AS attendant_key,
        public.smclick_try_uuid(a->>'id') AS attendant_id,
        NULLIF(BTRIM(a->>'name'), '') AS attendant_name, NULLIF(BTRIM(a->>'email'), '') AS attendant_email,
        CASE WHEN LOWER(COALESCE(a->>'principal', '')) IN ('true', 't', '1') THEN TRUE ELSE FALSE END AS principal,
        a AS attendant_raw
      FROM tmp_smclick_evt t
      CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(t.payload #> '{infos,chat,attendant}', '[]'::jsonb)) = 'array' THEN COALESCE(t.payload #> '{infos,chat,attendant}', '[]'::jsonb) ELSE '[]'::jsonb END) a
      WHERE t.chat_id IS NOT NULL
    ) x
    ORDER BY x.chat_id, x.attendant_key, x.event_time DESC NULLS LAST, x.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, attendant_key) DO UPDATE SET
    attendant_id = COALESCE(EXCLUDED.attendant_id, public.smclick_chat_attendant.attendant_id),
    attendant_name = COALESCE(EXCLUDED.attendant_name, public.smclick_chat_attendant.attendant_name),
    attendant_email = COALESCE(EXCLUDED.attendant_email, public.smclick_chat_attendant.attendant_email),
    principal = COALESCE(EXCLUDED.principal, public.smclick_chat_attendant.principal),
    last_seen_at = EXCLUDED.last_seen_at, raw = EXCLUDED.raw;
  GET DIAGNOSTICS v_att = ROW_COUNT;

  INSERT INTO public.smclick_chat_segment (chat_id, field_key, field_name, field_type, field_position, content_text, content_numeric, last_seen_at, raw)
  SELECT e.chat_id, e.field_key, e.field_name, e.field_type, e.field_position, e.content_text, e.content_numeric, COALESCE(e.event_time, NOW()), e.segment_raw
  FROM (
    SELECT DISTINCT ON (x.chat_id, x.field_key) x.*
    FROM (
      SELECT t.chat_id, t.event_time, t.source_buffer_id,
        COALESCE(NULLIF(BTRIM(s->>'id'), ''), md5(s::text)) AS field_key,
        NULLIF(BTRIM(s->>'name'), '') AS field_name, NULLIF(BTRIM(s->>'type'), '') AS field_type,
        CASE WHEN NULLIF(BTRIM(s->>'position'), '') ~ '^[0-9]+$' THEN (s->>'position')::int ELSE NULL END AS field_position,
        NULLIF(BTRIM(s->>'content'), '') AS content_text, public.smclick_parse_numeric(s->>'content') AS content_numeric,
        s AS segment_raw
      FROM tmp_smclick_evt t
      CROSS JOIN LATERAL jsonb_array_elements(CASE WHEN jsonb_typeof(COALESCE(t.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb)) = 'array' THEN COALESCE(t.payload #> '{infos,chat,contact,segmentation_fields}', '[]'::jsonb) ELSE '[]'::jsonb END) s
      WHERE t.chat_id IS NOT NULL
    ) x
    ORDER BY x.chat_id, x.field_key, x.event_time DESC NULLS LAST, x.source_buffer_id DESC
  ) e
  ON CONFLICT (chat_id, field_key) DO UPDATE SET
    field_name = COALESCE(EXCLUDED.field_name, public.smclick_chat_segment.field_name),
    field_type = COALESCE(EXCLUDED.field_type, public.smclick_chat_segment.field_type),
    field_position = COALESCE(EXCLUDED.field_position, public.smclick_chat_segment.field_position),
    content_text = COALESCE(EXCLUDED.content_text, public.smclick_chat_segment.content_text),
    content_numeric = COALESCE(EXCLUDED.content_numeric, public.smclick_chat_segment.content_numeric),
    last_seen_at = EXCLUDED.last_seen_at, raw = EXCLUDED.raw;
  GET DIAGNOSTICS v_seg = ROW_COUNT;

  UPDATE public.smclick_event_log e SET applied_at = NOW(), apply_error = NULL
  WHERE e.id IN (SELECT id FROM tmp_smclick_evt);

  RETURN QUERY SELECT v_events, v_chats, v_msgs, v_att, v_seg;
END;
$func$
"""

if __name__ == "__main__":
    conn = psycopg2.connect(DATABASE_URL)
    conn.autocommit = False
    cur = conn.cursor()

    try:
        print("Deploying fixed smclick_sync_from_ingest_buffer (SHA256 dedup fix + correct attendant_name)...")
        cur.execute(FIXED_FUNCTION)
        conn.commit()
        print("Deploy OK!\n")

        # Test one batch
        t0 = time.time()
        cur.execute("SELECT * FROM public.smclick_sync_from_ingest_buffer(2000)")
        row = cur.fetchone()
        conn.commit()
        elapsed = time.time() - t0
        print(f"Test batch 2000: events={row[0]}, chats={row[1]}, msgs={row[2]}, att={row[3]}, seg={row[4]} in {elapsed:.1f}s")

        cur.execute("SELECT COUNT(*), MAX(last_event_at) FROM smclick_chat")
        r = cur.fetchone()
        print(f"smclick_chat after: {r[0]} chats, newest: {r[1]}")

    except Exception as e:
        conn.rollback()
        print(f"ERROR: {e}")
        import traceback; traceback.print_exc()
    finally:
        conn.close()
