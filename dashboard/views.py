from django.conf import settings
from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render


MSG_FROM_CLIENT_SQL = """
CASE
  WHEN LOWER(COALESCE(r.msg_direcao, '')) IN ('inbound', 'recebida', 'received') THEN TRUE
  WHEN LOWER(COALESCE(r.msg_direcao, '')) IN ('outbound', 'enviada', 'sent') THEN FALSE
  ELSE NULL
END
"""

QUERY = f"""
SELECT
  r.id AS id,
  r.chat_id::text AS chat_id,
  r.protocolo::text AS protocolo,
  r.data_criacao_chat,
  r.status_conversa,
  r.tipo_fluxo,
  r.cliente_id_crm::text AS cliente_id_crm,
  r.cliente_nome,
  r.cliente_telefone,
  r.vendedor_id::text AS vendedor_id,
  r.vendedor_nome,
  r.vendedor_email,
  r.departamento,
  r.coluna_kanban,
  r.instancia_id::text AS instancia_id,
  r.instancia_nome,
  r.instancia_telefone,
  r.instancia_tipo,
  r.valor_orcamento,
  r.etapa_funil,
  r.produto_interesse,
  r.motivo_perda,
  r.data_fechamento,
  r.acessorios,
  r.msg_direcao,
  {MSG_FROM_CLIENT_SQL} AS msg_from_client,
  r.msg_tipo,
  r.msg_conteudo,
  CASE
    WHEN r.msg_status_envio IS TRUE THEN NULL
    WHEN r.msg_status_envio IS FALSE THEN COALESCE(NULLIF(BTRIM(r.msg_erro_motivo), ''), 'false')
    ELSE NULL
  END AS msg_status_envio,
  r.msg_erro_motivo,
  COALESCE(r.evento_timestamp, r.data_criacao_chat, r.ingested_at) AS evento_timestamp,
  r.ingested_at
FROM public.smclick_raw_events r
ORDER BY COALESCE(r.evento_timestamp, r.data_criacao_chat, r.ingested_at) DESC, r.id DESC
LIMIT 5000
"""


def fetch_events():
    with connection.cursor() as cursor:
        cursor.execute(QUERY)
        columns = [col[0] for col in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def events_api(request):
    try:
        events = fetch_events()
        return JsonResponse({"events": events})
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


def _messages_union_sql():
    return """
        SELECT
          1 AS source_priority,
          sm.id::text AS source_id,
          sm.chat_id::text AS chat_id,
          COALESCE(sm.message_time, sm.created_at) AS evento_timestamp,
          NULLIF(BTRIM(COALESCE(sm.msg_tipo, '')), '') AS msg_tipo,
          NULLIF(BTRIM(COALESCE(sm.msg_conteudo, '')), '') AS msg_conteudo,
          CASE
            WHEN LOWER(COALESCE(sm.author_type, '')) IN ('client', 'customer', 'contato', 'cliente', 'inbound') THEN TRUE
            WHEN LOWER(COALESCE(sm.author_type, '')) IN ('agent', 'attendant', 'vendedor', 'seller', 'outbound', 'system', 'bot') THEN FALSE
            ELSE NULL
          END AS msg_from_client,
          CASE
            WHEN sm.msg_status_envio IS TRUE THEN NULL
            WHEN sm.msg_status_envio IS FALSE THEN COALESCE(NULLIF(BTRIM(sm.msg_erro_motivo), ''), 'false')
            ELSE NULL
          END AS msg_status_envio
        FROM semclick_messages sm
        WHERE sm.chat_id IS NOT NULL

        UNION ALL

        SELECT
          2 AS source_priority,
          COALESCE(m.message_id::text, md5(COALESCE(m.chat_id::text, '') || '|' || COALESCE(m."timestamp"::text, '') || '|' || COALESCE(m.content, ''))) AS source_id,
          m.chat_id::text AS chat_id,
          m."timestamp" AS evento_timestamp,
          NULLIF(BTRIM(COALESCE(m.message_type, '')), '') AS msg_tipo,
          NULLIF(BTRIM(COALESCE(m.content, '')), '') AS msg_conteudo,
          CASE
            WHEN m.from_client IS TRUE THEN TRUE
            WHEN m.from_client IS FALSE THEN FALSE
            ELSE NULL
          END AS msg_from_client,
          NULL::text AS msg_status_envio
        FROM messages m
        WHERE m.chat_id IS NOT NULL
    """


def conversations_api(request):
    limit = int(request.GET.get("limit", "200"))
    offset = int(request.GET.get("offset", "0"))
    status = request.GET.get("status")
    etapa = request.GET.get("etapa")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    vendedor = request.GET.get("vendedor")

    where_clauses = []
    params = []
    messages_union_sql = _messages_union_sql()
    if status and status != "Todos":
        where_clauses.append("c.status_normalizado = %s")
        params.append(status)
    if etapa and etapa != "Todos":
        where_clauses.append("c.etapa_funil = %s")
        params.append(etapa)
    if date_from:
        where_clauses.append(
            "COALESCE(c.updated_at, c.data_criacao_chat, c.created_at)::date >= %s::date"
        )
        params.append(date_from)
    if date_to:
        where_clauses.append(
            "COALESCE(c.updated_at, c.data_criacao_chat, c.created_at)::date <= %s::date"
        )
        params.append(date_to)
    if vendedor and vendedor != "Todos":
        where_clauses.append("c.vendedor_nome = %s")
        params.append(vendedor)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        WITH messages_union AS (
          {messages_union_sql}
        ),
        latest_message AS (
          SELECT DISTINCT ON (mu.chat_id)
            mu.chat_id,
            mu.msg_tipo,
            mu.msg_conteudo,
            mu.msg_from_client,
            mu.msg_status_envio,
            mu.evento_timestamp
          FROM messages_union mu
          ORDER BY mu.chat_id, mu.evento_timestamp DESC NULLS LAST, mu.source_priority ASC, mu.source_id DESC
        ),
        conv_sources AS (
          SELECT
            1 AS source_priority,
            c.chat_id::text AS chat_id,
            to_jsonb(c) AS j
          FROM semclick_conversations c
          WHERE c.chat_id IS NOT NULL

          UNION ALL

          SELECT
            2 AS source_priority,
            c.chat_id::text AS chat_id,
            to_jsonb(c) AS j
          FROM conversations c
          WHERE c.chat_id IS NOT NULL
        ),
        conv AS (
          SELECT DISTINCT ON (src.chat_id)
            src.chat_id,
            NULLIF(BTRIM(COALESCE(src.j->>'protocolo', src.j->>'protocol', '')), '') AS protocolo,
            NULLIF(BTRIM(COALESCE(src.j->>'cliente_nome', src.j->>'customer_name', '')), '') AS cliente_nome,
            NULLIF(BTRIM(COALESCE(src.j->>'cliente_telefone', src.j->>'customer_phone', '')), '') AS cliente_telefone,
            NULLIF(BTRIM(COALESCE(src.j->>'vendedor_nome', src.j->>'attendant_name', src.j->>'current_attendant_name', '')), '') AS vendedor_nome,
            NULLIF(BTRIM(COALESCE(src.j->>'vendedor_email', src.j->>'attendant_email', '')), '') AS vendedor_email,
            NULLIF(BTRIM(COALESCE(src.j->>'status_conversa', src.j->>'current_funnel_stage', src.j->>'status', '')), '') AS status_conversa,
            NULLIF(BTRIM(COALESCE(src.j->>'etapa_funil_atual', src.j->>'etapa_funil', src.j->>'funnel_stage', src.j->>'current_funnel_stage', src.j->>'coluna_kanban', src.j->>'kanban_column', '')), '') AS etapa_funil,
            NULLIF(BTRIM(COALESCE(src.j->>'departamento', src.j->>'department_name', '')), '') AS departamento,
            NULLIF(BTRIM(COALESCE(src.j->>'coluna_kanban', src.j->>'kanban_column', '')), '') AS coluna_kanban,
            NULLIF(BTRIM(COALESCE(src.j->>'motivo_perda_atual', src.j->>'motivo_perda', src.j->>'finish_reason', '')), '') AS motivo_perda,
            NULLIF(BTRIM(COALESCE(src.j->>'produto_interesse_atual', src.j->>'produto_interesse', '')), '') AS produto_interesse,
            NULLIF(BTRIM(COALESCE(src.j->>'ai_agent_rating', '')), '') AS ai_agent_rating,
            NULLIF(BTRIM(COALESCE(src.j->>'ai_customer_sentiment', '')), '') AS ai_customer_sentiment,
            NULLIF(BTRIM(COALESCE(src.j->>'ai_summary', '')), '') AS ai_summary,
            NULLIF(BTRIM(COALESCE(src.j->>'ai_suggestion', '')), '') AS ai_suggestion,
            NULLIF(BTRIM(COALESCE(src.j->>'contact_reason', '')), '') AS contact_reason,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(src.j->>'data_criacao_chat', src.j->>'start_time', src.j->>'created_at', '')), '') ~ '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                THEN NULLIF(BTRIM(COALESCE(src.j->>'data_criacao_chat', src.j->>'start_time', src.j->>'created_at', '')), '')::timestamptz
              ELSE NULL
            END AS data_criacao_chat,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(src.j->>'data_fechamento_atual', src.j->>'data_fechamento', src.j->>'end_time', '')), '') ~ '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                THEN NULLIF(BTRIM(COALESCE(src.j->>'data_fechamento_atual', src.j->>'data_fechamento', src.j->>'end_time', '')), '')::timestamptz
              ELSE NULL
            END AS data_fechamento,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(src.j->>'updated_at', src.j->>'ultima_atualizacao', src.j->>'evento_timestamp', src.j->>'start_time', src.j->>'created_at', '')), '') ~ '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                THEN NULLIF(BTRIM(COALESCE(src.j->>'updated_at', src.j->>'ultima_atualizacao', src.j->>'evento_timestamp', src.j->>'start_time', src.j->>'created_at', '')), '')::timestamptz
              ELSE NULL
            END AS updated_at,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(src.j->>'created_at', src.j->>'data_criacao_chat', src.j->>'start_time', '')), '') ~ '^[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'
                THEN NULLIF(BTRIM(COALESCE(src.j->>'created_at', src.j->>'data_criacao_chat', src.j->>'start_time', '')), '')::timestamptz
              ELSE NULL
            END AS created_at,
            COALESCE(
              NULLIF(BTRIM(COALESCE(src.j->>'valor_orcamento_atual', src.j->>'valor_orcamento', src.j->>'budget_value', '')), ''),
              '0'
            ) AS valor_orcamento
          FROM conv_sources src
          ORDER BY src.chat_id, src.source_priority ASC
        ),
        conv_enriched AS (
          SELECT
            c.*,
            CASE
              WHEN LOWER(COALESCE(c.status_conversa, '')) IN ('screening', 'triagem') THEN 'Triagem'
              WHEN LOWER(COALESCE(c.status_conversa, '')) IN ('waiting', 'em espera', 'aguardando') THEN 'Aguardando'
              WHEN LOWER(COALESCE(c.status_conversa, '')) IN ('em atendimento', 'active') THEN 'Em atendimento'
              WHEN LOWER(COALESCE(c.status_conversa, '')) IN ('finalizado', 'finished', 'closed') THEN 'Finalizado'
              WHEN LOWER(COALESCE(c.etapa_funil, '')) IN ('finalizado', 'finished', 'closed') THEN 'Finalizado'
              WHEN LOWER(COALESCE(c.coluna_kanban, '')) IN ('finalizado', 'finished', 'closed') THEN 'Finalizado'
              ELSE NULL
            END AS status_normalizado
          FROM conv c
        )
        SELECT
          c.chat_id,
          c.protocolo,
          c.cliente_nome,
          c.cliente_telefone,
          c.vendedor_nome,
          c.vendedor_email,
          c.status_conversa,
          c.status_normalizado,
          c.etapa_funil,
          c.departamento,
          c.coluna_kanban,
          c.data_criacao_chat,
          c.data_fechamento,
          c.valor_orcamento,
          c.motivo_perda,
          c.produto_interesse,
          c.ai_agent_rating,
          c.ai_customer_sentiment,
          c.ai_summary,
          c.ai_suggestion,
          c.contact_reason,
          c.updated_at,
          c.created_at,
          lm.msg_tipo,
          lm.msg_conteudo,
          lm.msg_status_envio,
          lm.evento_timestamp,
          lm.msg_from_client
        FROM conv_enriched c
        LEFT JOIN latest_message lm ON lm.chat_id = c.chat_id
        {where_sql}
        ORDER BY COALESCE(lm.evento_timestamp, c.updated_at, c.data_criacao_chat, c.created_at) DESC
        LIMIT %s OFFSET %s;
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, [*params, limit, offset])
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return JsonResponse({"conversations": rows})
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


def messages_api(request):
    chat_id = request.GET.get("chat_id")
    if not chat_id:
        return JsonResponse({"error": "chat_id_required"}, status=400)

    limit = int(request.GET.get("limit", "500"))
    offset = int(request.GET.get("offset", "0"))

    messages_union_sql = _messages_union_sql()
    query = f"""
        WITH messages_union AS (
          {messages_union_sql}
        ),
        ranked AS (
          SELECT
            mu.source_id AS id,
            mu.chat_id,
            mu.evento_timestamp,
            mu.msg_conteudo,
            mu.msg_tipo,
            mu.msg_from_client,
            mu.msg_status_envio,
            ROW_NUMBER() OVER (
              PARTITION BY
                mu.chat_id,
                mu.evento_timestamp,
                mu.msg_from_client,
                COALESCE(mu.msg_tipo, ''),
                COALESCE(mu.msg_conteudo, '')
              ORDER BY mu.source_priority ASC, mu.source_id DESC
            ) AS dedup_rank
          FROM messages_union mu
          WHERE mu.chat_id = %s
            AND (
              mu.msg_tipo IS NOT NULL
              OR mu.msg_conteudo IS NOT NULL
            )
        )
        SELECT
          r.id,
          r.chat_id,
          r.evento_timestamp,
          r.msg_conteudo,
          r.msg_tipo,
          r.msg_from_client,
          r.msg_status_envio
        FROM ranked r
        WHERE r.dedup_rank = 1
        ORDER BY r.evento_timestamp ASC NULLS LAST, r.id ASC
        LIMIT %s OFFSET %s;
    """
    try:
        with connection.cursor() as cursor:
            cursor.execute(query, [chat_id, limit, offset])
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return JsonResponse({"messages": rows})
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


def dashboard_api(request):
    status = request.GET.get("status")
    etapa = request.GET.get("etapa")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    vendedor = request.GET.get("vendedor")

    where_clauses = ["c.chat_id IS NOT NULL"]
    params = []
    if status and status != "Todos":
        if status == "Triagem":
            where_clauses.append("c.current_funnel_stage = %s")
            params.append("screening")
        elif status == "Aguardando":
            where_clauses.append("c.current_funnel_stage IN (%s, %s)")
            params.extend(["waiting", "Em espera"])
        elif status == "Em atendimento":
            where_clauses.append("c.current_funnel_stage = %s")
            params.append("Em atendimento")
        elif status == "Finalizado":
            where_clauses.append("c.current_funnel_stage IN (%s, %s, %s)")
            params.extend(["Finalizado", "finished", "closed"])
    if etapa and etapa != "Todos":
        where_clauses.append("c.current_funnel_stage = %s")
        params.append(etapa)
    if date_from:
        where_clauses.append(
            "(COALESCE(c.start_time, c.created_at) AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date"
        )
        params.append(date_from)
    if date_to:
        where_clauses.append(
            "(COALESCE(c.start_time, c.created_at) AT TIME ZONE 'America/Sao_Paulo')::date <= %s::date"
        )
        params.append(date_to)
    if vendedor and vendedor != "Todos":
        where_clauses.append("c.attendant_name = %s")
        params.append(vendedor)

    where_sql = "WHERE " + " AND ".join(where_clauses)
    filtered_base_sql = f"""
      SELECT DISTINCT ON (c.chat_id)
        c.*
      FROM conversations c
      {where_sql}
      ORDER BY
        c.chat_id,
        COALESCE(c.start_time, c.created_at, c.end_time) DESC NULLS LAST,
        c.created_at DESC NULLS LAST
    """

    stats_query = f"""
        WITH filtered AS (
          {filtered_base_sql}
        ),
        bot_events AS (
          SELECT
            m.chat_id,
            MIN(m."timestamp") AS bot_transfer_ts
          FROM messages m
          JOIN filtered f ON f.chat_id = m.chat_id
          WHERE m.from_client = false
            AND m.content IS NOT NULL
            AND (
              m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
              OR m.content ILIKE '%%Vou verificar a disponibilidade com nosso time de vendas. Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
              OR m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso time de vendas%%'
              OR m.content ILIKE '%%Vou direcionar seu atendimento ao nosso time de vendas%%'
              OR m.content ILIKE '%%Vou encaminhar ao nosso time de vendas%%'
              OR m.content ILIKE '%%Obrigado, vou encaminhar ao nosso time de vendas%%'
              OR m.content ILIKE '%%Obrigada, vou encaminhar ao nosso time de vendas%%'
              OR m.content ILIKE '%%atendimento ao nosso setor de vendas.%%'
            )
          GROUP BY m.chat_id
        ),
        first_contact AS (
          SELECT
            f.chat_id,
            MIN(m."timestamp") FILTER (WHERE m.from_client = true) AS first_client_ts
          FROM filtered f
          JOIN messages m ON m.chat_id = f.chat_id
          GROUP BY f.chat_id
        ),
        business_duration AS (
          SELECT
            fc.chat_id,
            SUM(
              GREATEST(
                0,
                EXTRACT(
                  EPOCH FROM (
                    LEAST(f.end_time AT TIME ZONE 'America/Sao_Paulo', day_end)
                    - GREATEST(fc.first_client_ts AT TIME ZONE 'America/Sao_Paulo', day_start)
                  )
                )
              )
            ) AS business_seconds
          FROM first_contact fc
          JOIN filtered f ON f.chat_id = fc.chat_id
          JOIN LATERAL (
            SELECT
              day::timestamp + time '08:00' AS day_start,
              day::timestamp + time '18:00' AS day_end
            FROM generate_series(
              date_trunc('day', fc.first_client_ts AT TIME ZONE 'America/Sao_Paulo'),
              date_trunc('day', f.end_time AT TIME ZONE 'America/Sao_Paulo'),
              interval '1 day'
            ) AS day
            WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
          ) d ON TRUE
          WHERE fc.first_client_ts IS NOT NULL AND f.end_time IS NOT NULL
          GROUP BY fc.chat_id
        ),
        human_events AS (
          SELECT
            m.chat_id,
            MIN(m."timestamp") AS first_human_ts
          FROM messages m
          JOIN bot_events b ON b.chat_id = m.chat_id
          WHERE m.from_client = false
            AND m."timestamp" > b.bot_transfer_ts
            AND (
              m.content IS NULL OR (
                m.content NOT ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                AND m.content NOT ILIKE '%%Vou verificar a disponibilidade com nosso time de vendas. Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                AND m.content NOT ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso time de vendas%%'
                AND m.content NOT ILIKE '%%Vou direcionar seu atendimento ao nosso time de vendas%%'
                AND m.content NOT ILIKE '%%Vou encaminhar ao nosso time de vendas%%'
                AND m.content NOT ILIKE '%%Obrigado, vou encaminhar ao nosso time de vendas%%'
                AND m.content NOT ILIKE '%%Obrigada, vou encaminhar ao nosso time de vendas%%'
                AND m.content NOT ILIKE '%%atendimento ao nosso setor de vendas.%%'
              )
            )
          GROUP BY m.chat_id
        ),
        business_handoff AS (
          SELECT
            b.chat_id,
            SUM(
              GREATEST(
                0,
                EXTRACT(
                  EPOCH FROM (
                    LEAST(h.first_human_ts AT TIME ZONE 'America/Sao_Paulo', day_end)
                    - GREATEST(b.bot_transfer_ts AT TIME ZONE 'America/Sao_Paulo', day_start)
                  )
                )
              )
            ) AS business_seconds
          FROM bot_events b
          JOIN human_events h ON h.chat_id = b.chat_id
          JOIN LATERAL (
            SELECT
              day::timestamp + time '08:00' AS day_start,
              day::timestamp + time '18:00' AS day_end
            FROM generate_series(
              date_trunc('day', b.bot_transfer_ts AT TIME ZONE 'America/Sao_Paulo'),
              date_trunc('day', h.first_human_ts AT TIME ZONE 'America/Sao_Paulo'),
              interval '1 day'
            ) AS day
            WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
          ) d ON TRUE
          GROUP BY b.chat_id
        )
        SELECT
          AVG(bd.business_seconds) AS avg_duration_seconds,
          AVG(bh.business_seconds) AS avg_handoff_seconds
        FROM filtered f
        LEFT JOIN business_handoff bh ON bh.chat_id = f.chat_id
        LEFT JOIN business_duration bd ON bd.chat_id = f.chat_id;
    """

    stage_count_query = f"""
        WITH filtered AS (
          {filtered_base_sql}
        ),
        normalized AS (
          SELECT
            CASE
              WHEN current_funnel_stage = 'screening' THEN 'Triagem'
              WHEN current_funnel_stage IN ('waiting', 'Em espera') THEN 'Aguardando'
              WHEN current_funnel_stage = 'Em atendimento' THEN 'Em atendimento'
              WHEN current_funnel_stage IN ('Finalizado', 'finished', 'closed') THEN 'Finalizado'
              WHEN current_funnel_stage = 'active' THEN NULL
              ELSE COALESCE(current_funnel_stage, 'Sem etapa')
            END AS stage_name
          FROM filtered
          WHERE current_funnel_stage IS NOT NULL
        )
        SELECT stage_name, COUNT(*) AS total
        FROM normalized
        WHERE stage_name IS NOT NULL
        GROUP BY stage_name
        ORDER BY total DESC;
    """

    contacts_breakdown_query = f"""
        WITH filtered AS (
          {filtered_base_sql}
        ),
        normalized AS (
          SELECT
            CASE
              WHEN current_funnel_stage = 'screening' THEN 'Triagem'
              WHEN current_funnel_stage IN ('waiting', 'Em espera') THEN 'Aguardando'
              WHEN current_funnel_stage = 'Em atendimento' THEN 'Em atendimento'
              WHEN current_funnel_stage IN ('Finalizado', 'finalizado', 'finished', 'closed') THEN 'Finalizado'
              WHEN current_funnel_stage = 'active' THEN 'Ativo'
              ELSE COALESCE(current_funnel_stage, 'Sem etapa')
            END AS stage_name
          FROM filtered
          WHERE attendant_name IS NOT NULL
        )
        SELECT stage_name, COUNT(*) AS total
        FROM normalized
        GROUP BY stage_name
        ORDER BY total DESC;
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(stats_query, params)
            stats_row = cursor.fetchone()
            stats = {
                "avg_duration_seconds": float(stats_row[0]) if stats_row[0] is not None else 0,
                "avg_handoff_seconds": float(stats_row[1]) if stats_row[1] is not None else 0,
            }
            cursor.execute(stage_count_query, params)
            stage_rows = cursor.fetchall()
            stage_counts = [
                {"stage_name": row[0], "total": row[1]} for row in stage_rows
            ]

            cursor.execute(contacts_breakdown_query, params)
            contacts_rows = cursor.fetchall()
            contacts_stages = [
                {"stage_name": row[0], "total": row[1]} for row in contacts_rows
            ]
            contacts_total = sum(row[1] for row in contacts_rows) if contacts_rows else 0
            contacts_finalized = 0
            contacts_active = 0
            contacts_pending = 0
            for stage in contacts_stages:
                name = str(stage["stage_name"]).strip().lower()
                if name == "finalizado":
                    contacts_finalized += stage["total"]
                elif name in ("em atendimento", "ativo"):
                    contacts_active += stage["total"]
                elif name in ("triagem", "aguardando"):
                    contacts_pending += stage["total"]
            contacts_other = max(
                contacts_total - contacts_finalized - contacts_active - contacts_pending, 0
            )
            sdr_summary_query = f"""
                WITH filtered AS (
                  {filtered_base_sql}
                ),
                classified AS (
                  SELECT
                    f.chat_id,
                    translate(
                      lower(COALESCE(f.contact_reason, '')),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) AS reason_norm,
                    translate(
                      lower(COALESCE(f.current_funnel_stage, '')),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) AS stage_norm,
                    f.attendant_name
                  FROM filtered f
                ),
                message_stats AS (
                  SELECT
                    m.chat_id,
                    SUM(CASE WHEN m.from_client = false THEN 1 ELSE 0 END) AS outbound_count,
                    SUM(CASE WHEN m.from_client = true THEN 1 ELSE 0 END) AS inbound_count
                  FROM messages m
                  JOIN filtered f ON f.chat_id = m.chat_id
                  GROUP BY m.chat_id
                ),
                bot_events AS (
                  SELECT
                    m.chat_id,
                    MIN(m."timestamp") AS bot_transfer_ts
                  FROM messages m
                  JOIN filtered f ON f.chat_id = m.chat_id
                  WHERE m.from_client = false
                    AND m.content IS NOT NULL
                    AND (
                      m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                      OR m.content ILIKE '%%Vou verificar a disponibilidade com nosso time de vendas. Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                      OR m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Vou direcionar seu atendimento ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Obrigado, vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Obrigada, vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%atendimento ao nosso setor de vendas.%%'
                    )
                  GROUP BY m.chat_id
                )
                SELECT
                  COUNT(*) AS total_contacts,
                  COUNT(*) FILTER (WHERE cl.reason_norm ~ 'rastreio') AS total_tracking,
                  COUNT(*) FILTER (
                    WHERE cl.reason_norm ~ '(sac|pos[- ]?venda|duvidas?|suporte)'
                  ) AS total_sac,
                  COUNT(*) FILTER (
                    WHERE cl.stage_norm IN ('waiting', 'em espera', 'aguardando')
                  ) AS total_waiting,
                  COUNT(*) FILTER (
                    WHERE f.attendant_name IS NOT NULL
                      AND cl.stage_norm NOT IN ('waiting', 'em espera', 'aguardando')
                      AND cl.reason_norm !~ '(sac|pos[- ]?venda|duvidas?|suporte|rastreio)'
                  ) AS total_sales,
                  COUNT(*) FILTER (WHERE b.bot_transfer_ts IS NOT NULL) AS total_transferred,
                  COUNT(*) FILTER (WHERE COALESCE(ms.outbound_count, 0) = 0) AS total_dead
                FROM filtered f
                JOIN classified cl ON cl.chat_id = f.chat_id
                LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
                LEFT JOIN bot_events b ON b.chat_id = f.chat_id;
            """

            sdr_daily_query = f"""
                WITH filtered AS (
                  {filtered_base_sql}
                ),
                message_stats AS (
                  SELECT
                    m.chat_id,
                    SUM(CASE WHEN m.from_client = false THEN 1 ELSE 0 END) AS outbound_count
                  FROM messages m
                  JOIN filtered f ON f.chat_id = m.chat_id
                  GROUP BY m.chat_id
                )
                SELECT
                  date_trunc('day', COALESCE(f.start_time, f.created_at) AT TIME ZONE 'America/Sao_Paulo')::date AS day,
                  COUNT(*) AS contacts,
                  COUNT(*) FILTER (WHERE f.attendant_name IS NOT NULL) AS tracking,
                  COUNT(*) FILTER (WHERE COALESCE(ms.outbound_count, 0) = 0) AS dead
                FROM filtered f
                LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
                GROUP BY day
                ORDER BY day;
            """

            sdr_transferred_daily_query = f"""
                WITH filtered AS (
                  {filtered_base_sql}
                ),
                bot_events AS (
                  SELECT
                    m.chat_id,
                    MIN(m."timestamp") AS bot_transfer_ts
                  FROM messages m
                  JOIN filtered f ON f.chat_id = m.chat_id
                  WHERE m.from_client = false
                    AND m.content IS NOT NULL
                    AND (
                      m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                      OR m.content ILIKE '%%Vou verificar a disponibilidade com nosso time de vendas. Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                      OR m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Vou direcionar seu atendimento ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Obrigado, vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Obrigada, vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%atendimento ao nosso setor de vendas.%%'
                    )
                  GROUP BY m.chat_id
                )
                SELECT
                  date_trunc('day', b.bot_transfer_ts AT TIME ZONE 'America/Sao_Paulo')::date AS day,
                  COUNT(*) AS transferred
                FROM bot_events b
                JOIN filtered f ON f.chat_id = b.chat_id
                GROUP BY day
                ORDER BY day;
            """

            support_reason_pattern = "(pos[- ]?venda|duvidas?|sac|rastreio)"

            vendor_summary_query = f"""
                WITH filtered_base AS (
                  {filtered_base_sql}
                ),
                filtered AS (
                  SELECT
                    fb.*,
                    translate(
                      lower(COALESCE(fb.contact_reason, '')),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) AS reason_norm
                  FROM filtered_base fb
                ),
                bot_events AS (
                  SELECT
                    m.chat_id,
                    MIN(m."timestamp") AS bot_transfer_ts
                  FROM messages m
                  JOIN filtered f ON f.chat_id = m.chat_id
                  WHERE m.from_client = false
                    AND m.content IS NOT NULL
                    AND (
                      m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                      OR m.content ILIKE '%%Vou verificar a disponibilidade com nosso time de vendas. Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                      OR m.content ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Vou direcionar seu atendimento ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Obrigado, vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%Obrigada, vou encaminhar ao nosso time de vendas%%'
                      OR m.content ILIKE '%%atendimento ao nosso setor de vendas.%%'
                    )
                  GROUP BY m.chat_id
                ),
                message_stats AS (
                  SELECT
                    m.chat_id,
                    SUM(CASE WHEN m.from_client = false THEN 1 ELSE 0 END) AS outbound_count,
                    MIN(m."timestamp") FILTER (WHERE m.from_client = true) AS first_client_ts
                  FROM messages m
                  JOIN filtered f ON f.chat_id = m.chat_id
                  GROUP BY m.chat_id
                ),
                budget_values AS (
                  SELECT
                    m.chat_id,
                    MAX(
                      NULLIF(
                        REPLACE(REPLACE((matches)[1], '.', ''), ',', '.'),
                        ''
                      )::numeric
                    ) AS max_budget_msg
                  FROM messages m
                  JOIN filtered f ON f.chat_id = m.chat_id
                  JOIN LATERAL regexp_matches(
                    m.content,
                    'R\\$\\s*([0-9\\.]+(?:,[0-9]{2})?)',
                    'g'
                  ) AS matches ON TRUE
                  WHERE m.content IS NOT NULL
                  GROUP BY m.chat_id
                ),
                business_duration AS (
                  SELECT
                    ms.chat_id,
                    SUM(
                      GREATEST(
                        0,
                        EXTRACT(
                          EPOCH FROM (
                            LEAST(f.end_time AT TIME ZONE 'America/Sao_Paulo', day_end)
                            - GREATEST(ms.first_client_ts AT TIME ZONE 'America/Sao_Paulo', day_start)
                          )
                        )
                      )
                    ) AS business_seconds
                  FROM message_stats ms
                  JOIN filtered f ON f.chat_id = ms.chat_id
                  JOIN LATERAL (
                    SELECT
                      day::timestamp + time '08:00' AS day_start,
                      day::timestamp + time '18:00' AS day_end
                    FROM generate_series(
                      date_trunc('day', ms.first_client_ts AT TIME ZONE 'America/Sao_Paulo'),
                      date_trunc('day', f.end_time AT TIME ZONE 'America/Sao_Paulo'),
                      interval '1 day'
                    ) AS day
                    WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
                  ) d ON TRUE
                  WHERE ms.first_client_ts IS NOT NULL AND f.end_time IS NOT NULL
                  GROUP BY ms.chat_id
                ),
                human_events AS (
                  SELECT
                    m.chat_id,
                    MIN(m."timestamp") AS first_human_ts
                  FROM messages m
                  JOIN bot_events b ON b.chat_id = m.chat_id
                  WHERE m.from_client = false
                    AND m."timestamp" > b.bot_transfer_ts
                    AND (
                      m.content IS NULL OR (
                        m.content NOT ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                        AND m.content NOT ILIKE '%%Vou verificar a disponibilidade com nosso time de vendas. Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso setor de vendas%%'
                        AND m.content NOT ILIKE '%%Agradeço pelas informações! Estou direcionando o seu atendimento ao nosso time de vendas%%'
                        AND m.content NOT ILIKE '%%Vou direcionar seu atendimento ao nosso time de vendas%%'
                        AND m.content NOT ILIKE '%%Vou encaminhar ao nosso time de vendas%%'
                        AND m.content NOT ILIKE '%%Obrigado, vou encaminhar ao nosso time de vendas%%'
                        AND m.content NOT ILIKE '%%Obrigada, vou encaminhar ao nosso time de vendas%%'
                        AND m.content NOT ILIKE '%%atendimento ao nosso setor de vendas.%%'
                      )
                    )
                  GROUP BY m.chat_id
                ),
                business_handoff AS (
                  SELECT
                    b.chat_id,
                    SUM(
                      GREATEST(
                        0,
                        EXTRACT(
                          EPOCH FROM (
                            LEAST(h.first_human_ts AT TIME ZONE 'America/Sao_Paulo', day_end)
                            - GREATEST(b.bot_transfer_ts AT TIME ZONE 'America/Sao_Paulo', day_start)
                          )
                        )
                      )
                    ) AS business_seconds
                  FROM bot_events b
                  JOIN human_events h ON h.chat_id = b.chat_id
                  JOIN LATERAL (
                    SELECT
                      day::timestamp + time '08:00' AS day_start,
                      day::timestamp + time '18:00' AS day_end
                    FROM generate_series(
                      date_trunc('day', b.bot_transfer_ts AT TIME ZONE 'America/Sao_Paulo'),
                      date_trunc('day', h.first_human_ts AT TIME ZONE 'America/Sao_Paulo'),
                      interval '1 day'
                    ) AS day
                    WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
                  ) d ON TRUE
                  GROUP BY b.chat_id
                )
                SELECT
                  f.attendant_name AS vendedor,
                  COUNT(*) AS contacts_received,
                  COUNT(*) FILTER (
                    WHERE f.budget_value > 0
                      AND NOT (f.reason_norm ~ '{support_reason_pattern}')
                  ) AS budgets_count,
                  COUNT(*) FILTER (
                    WHERE (f.budget_value > 0 OR bv.max_budget_msg IS NOT NULL)
                      AND NOT (f.reason_norm ~ '{support_reason_pattern}')
                  ) AS budgets_detected_count,
                  COALESCE(
                    SUM(
                      CASE
                        WHEN f.budget_value > 0
                          AND NOT (f.reason_norm ~ '{support_reason_pattern}')
                        THEN f.budget_value
                        ELSE 0
                      END
                    ),
                    0
                  ) AS budgets_sum,
                  COALESCE(
                    SUM(
                      CASE
                        WHEN f.budget_value > 0 THEN 0
                        WHEN f.reason_norm ~ '{support_reason_pattern}' THEN 0
                        ELSE COALESCE(bv.max_budget_msg, 0)
                      END
                    ),
                    0
                  ) AS budgets_sum_detected,
                  COUNT(*) FILTER (WHERE COALESCE(ms.outbound_count, 0) = 0) AS dead_contacts,
                  AVG(bd.business_seconds) AS avg_duration_seconds,
                  AVG(bh.business_seconds) AS avg_handoff_seconds,
                  AVG(
                    NULLIF(
                      REGEXP_REPLACE(COALESCE(f.ai_agent_rating::text, ''), '[^0-9\\.]', '', 'g'),
                      ''
                    )::numeric
                  ) AS avg_score
                FROM filtered f
                LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
                LEFT JOIN budget_values bv ON bv.chat_id = f.chat_id
                LEFT JOIN business_duration bd ON bd.chat_id = f.chat_id
                LEFT JOIN business_handoff bh ON bh.chat_id = f.chat_id
                WHERE f.attendant_name IS NOT NULL
                GROUP BY f.attendant_name
                ORDER BY contacts_received DESC;
            """

            vendor_scores_query = f"""
                WITH filtered AS (
                  {filtered_base_sql}
                )
                SELECT
                  f.attendant_name AS vendedor,
                  COALESCE(CAST(f.ai_agent_rating AS TEXT), 'Sem score') AS score,
                  COUNT(*) AS total
                FROM filtered f
                WHERE f.attendant_name IS NOT NULL
                GROUP BY f.attendant_name, score
                ORDER BY f.attendant_name, score;
            """

            cursor.execute(sdr_summary_query, params)
            sdr_row = cursor.fetchone() or (0, 0, 0, 0, 0, 0, 0)
            sdr_summary = {
                "contacts": sdr_row[0] or 0,
                "tracking": sdr_row[1] or 0,
                "sac": sdr_row[2] or 0,
                "waiting": sdr_row[3] or 0,
                "sales": sdr_row[4] or 0,
                "transferred": sdr_row[5] or 0,
                "dead": sdr_row[6] or 0,
            }

            cursor.execute(sdr_daily_query, params)
            sdr_daily_rows = cursor.fetchall()
            sdr_daily = [
                {
                    "day": row[0].isoformat() if row[0] else None,
                    "contacts": row[1],
                    "tracking": row[2],
                    "dead": row[3],
                }
                for row in sdr_daily_rows
            ]

            cursor.execute(sdr_transferred_daily_query, params)
            transferred_rows = cursor.fetchall()
            sdr_transferred_daily = [
                {
                    "day": row[0].isoformat() if row[0] else None,
                    "transferred": row[1],
                }
                for row in transferred_rows
            ]

            cursor.execute(vendor_summary_query, params)
            vendor_rows = cursor.fetchall()
            vendors = [
                {
                    "vendedor": row[0],
                    "contacts_received": row[1],
                    "budgets_count": row[2],
                    "budgets_detected_count": row[3],
                    "budgets_sum": float(row[4]) if row[4] is not None else 0,
                    "budgets_sum_detected": float(row[5]) if row[5] is not None else 0,
                    "dead_contacts": row[6],
                    "avg_duration_seconds": float(row[7]) if row[7] is not None else 0,
                    "avg_handoff_seconds": float(row[8]) if row[8] is not None else 0,
                    "avg_score": float(row[9]) if row[9] is not None else 0,
                }
                for row in vendor_rows
            ]

            cursor.execute(vendor_scores_query, params)
            score_rows = cursor.fetchall()
            vendor_scores = {}
            for vendedor, score, total in score_rows:
                vendor_scores.setdefault(vendedor, []).append(
                    {"score": score, "total": total}
                )

        return JsonResponse(
            {
                "stats": stats,
                "stage_counts": stage_counts,
                "contacts_breakdown": {
                    "total": contacts_total,
                    "active": contacts_active,
                    "pending": contacts_pending,
                    "finalized": contacts_finalized,
                    "other": contacts_other,
                    "stages": contacts_stages,
                },
                "sdr": {
                    "summary": sdr_summary,
                    "daily": sdr_daily,
                    "transferred_daily": sdr_transferred_daily,
                },
                "vendors": {
                    "summary": vendors,
                    "scores": vendor_scores,
                },
            }
        )
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


def index(request):
    return render(request, "dashboard/index.html", {"debug": settings.DEBUG})
