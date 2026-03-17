from django.conf import settings
from django.core.cache import cache
from django.db import connection
from django.http import JsonResponse
from django.shortcuts import render

STAGE_STRATIFICATION_ORDER = [
    ("aguardando", "Aguardando"),
    ("triagem", "Triagem"),
    ("em_atendimento", "Em atendimento"),
    ("ativo", "Ativo"),
    ("cadastro", "Cadastro"),
    ("chamada_1", "1ª chamada"),
    ("chamada_2", "2ª chamada"),
    ("chamada_3", "3ª chamada"),
    ("proposta_enviada", "Proposta enviada"),
    ("pos_vendas", "Pós-vendas"),
    ("finalizado", "Finalizado"),
    ("lixo", "Lixo"),
]


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


def _owners_view_id_column():
    """
    Resolve coluna identificadora de conversa na view public.vw_conversations_owners.
    Retorna None quando nao estiver em PostgreSQL, view ausente ou sem coluna compativel.
    """
    if connection.vendor != "postgresql":
        return None
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = 'vw_conversations_owners'
                ORDER BY ordinal_position
                """
            )
            cols = {row[0] for row in cursor.fetchall()}
        for candidate in (
            "chat_id",
            "conversation_id",
            "conversation_uuid",
            "conversa_id",
            "chat_uuid",
            "protocolo",
            "protocol",
        ):
            if candidate in cols:
                return candidate
    except Exception:
        return None
    return None


def _messages_union_sql():
    return """
        SELECT
          0 AS source_priority,
          (sm.chat_id::text || '|' || sm.message_id) AS source_id,
          sm.chat_id::text AS chat_id,
          sm.evento_timestamp,
          NULLIF(BTRIM(COALESCE(sm.msg_tipo, '')), '') AS msg_tipo,
          NULLIF(BTRIM(COALESCE(sm.msg_conteudo, '')), '') AS msg_conteudo,
          sm.msg_from_client,
          sm.msg_status_envio,
          NULLIF(BTRIM(COALESCE(sm.sent_by_name, '')), '') AS sent_by_name
        FROM vw_smclick_messages_timeline sm
        WHERE sm.chat_id IS NOT NULL

        UNION ALL

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
          END AS msg_status_envio,
          NULLIF(BTRIM(COALESCE(sm.author_name, '')), '') AS sent_by_name
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
          NULL::text AS msg_status_envio,
          NULL::text AS sent_by_name
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
    elif not date_to:
        # sem filtro de data: limita aos últimos 30 dias como padrão de segurança
        where_clauses.append(
            "COALESCE(c.updated_at, c.data_criacao_chat, c.created_at)::date >= (CURRENT_DATE - INTERVAL '30 days')::date"
        )
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
            0 AS source_priority,
            c.chat_id::text AS chat_id,
            to_jsonb(c) AS j
          FROM vw_smclick_conversations_latest c
          WHERE c.chat_id IS NOT NULL

          UNION ALL

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
            ) AS valor_orcamento,
            NULLIF(BTRIM(COALESCE(src.j->>'valor_pedido', src.j->>'order_value', '')), '') AS valor_pedido
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
          c.valor_pedido,
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
            mu.sent_by_name,
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
          r.msg_status_envio,
          r.sent_by_name
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


def dashboard_stage_stratification_api(request):
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    vendedor = request.GET.get("vendedor")
    clients_limit_raw = request.GET.get("clients_limit", "300")

    try:
        clients_limit = max(1, min(int(clients_limit_raw), 1000))
    except (TypeError, ValueError):
        clients_limit = 300

    where_clauses = []
    params = []
    if date_from:
        where_clauses.append(
            "(COALESCE(c.created_ts, c.updated_ts) AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date"
        )
        params.append(date_from)
    if date_to:
        where_clauses.append(
            "(COALESCE(c.created_ts, c.updated_ts) AT TIME ZONE 'America/Sao_Paulo')::date <= %s::date"
        )
        params.append(date_to)
    if vendedor and vendedor != "Todos":
        where_clauses.append("c.vendedor_nome = %s")
        params.append(vendedor)

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    query = f"""
        WITH conv_sources AS (
          SELECT
            0 AS source_priority,
            sc.chat_id::text AS chat_id,
            to_jsonb(sc) AS j
          FROM vw_smclick_conversations_latest sc
          WHERE sc.chat_id IS NOT NULL

          UNION ALL

          SELECT
            1 AS source_priority,
            sc.chat_id::text AS chat_id,
            to_jsonb(sc) AS j
          FROM semclick_conversations sc
          WHERE sc.chat_id IS NOT NULL

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
            NULLIF(BTRIM(COALESCE(src.j->>'cliente_nome', src.j->>'customer_name', src.j->>'contact_name', src.j#>>'{{contact,name}}', src.j#>>'{{infos,chat,contact,name}}', '')), '') AS cliente_nome,
            NULLIF(BTRIM(COALESCE(src.j->>'cliente_telefone', src.j->>'customer_phone', src.j->>'contact_phone', src.j#>>'{{contact,telephone}}', src.j#>>'{{infos,chat,contact,telephone}}', '')), '') AS cliente_telefone,
            NULLIF(BTRIM(COALESCE(src.j->>'vendedor_nome', src.j->>'attendant_name', src.j->>'current_attendant_name', src.j#>>'{{infos,message,sent_by,name}}', '')), '') AS vendedor_nome,
            NULLIF(BTRIM(COALESCE(src.j->>'etapa_funil_atual', src.j->>'etapa_funil', src.j->>'funnel_stage', src.j->>'current_funnel_stage', src.j->>'coluna_kanban', src.j->>'kanban_column', src.j#>>'{{infos,chat,crm_column,name}}', '')), '') AS stage_raw,
            NULLIF(BTRIM(COALESCE(src.j->>'status', src.j->>'status_conversa', '')), '') AS status_raw,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(src.j->>'updated_at', src.j->>'ultima_atualizacao', src.j->>'evento_timestamp', src.j->>'start_time', src.j->>'created_at', src.j#>>'{{infos,chat,updated_at}}', '')), '') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                THEN NULLIF(BTRIM(COALESCE(src.j->>'updated_at', src.j->>'ultima_atualizacao', src.j->>'evento_timestamp', src.j->>'start_time', src.j->>'created_at', src.j#>>'{{infos,chat,updated_at}}', '')), '')::timestamptz
              ELSE NULL
            END AS updated_ts,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(src.j->>'data_criacao_chat', src.j->>'start_time', src.j->>'created_at', src.j#>>'{{infos,chat,created_at}}', '')), '') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                THEN NULLIF(BTRIM(COALESCE(src.j->>'data_criacao_chat', src.j->>'start_time', src.j->>'created_at', src.j#>>'{{infos,chat,created_at}}', '')), '')::timestamptz
              ELSE NULL
            END AS created_ts
          FROM conv_sources src
          ORDER BY
            src.chat_id,
            CASE
              WHEN NULLIF(BTRIM(COALESCE(src.j->>'updated_at', src.j->>'ultima_atualizacao', src.j->>'evento_timestamp', src.j->>'start_time', src.j->>'created_at', src.j#>>'{{infos,chat,updated_at}}', '')), '') ~ '^[0-9]{{4}}-[0-9]{{2}}-[0-9]{{2}}'
                THEN NULLIF(BTRIM(COALESCE(src.j->>'updated_at', src.j->>'ultima_atualizacao', src.j->>'evento_timestamp', src.j->>'start_time', src.j->>'created_at', src.j#>>'{{infos,chat,updated_at}}', '')), '')::timestamptz
              ELSE NULL
            END DESC NULLS LAST,
            src.source_priority ASC
        ),
        filtered AS (
          SELECT *
          FROM conv c
          {where_sql}
        ),
        classified AS (
          SELECT
            f.chat_id,
            f.cliente_nome,
            f.cliente_telefone,
            f.vendedor_nome,
            f.stage_raw,
            f.created_ts,
            f.updated_ts,
            CASE
              WHEN stage_norm IN ('waiting', 'em espera', 'aguardando') THEN 'aguardando'
              WHEN stage_norm IN ('screening', 'triagem') THEN 'triagem'
              WHEN stage_norm IN ('em atendimento') THEN 'em_atendimento'
              WHEN stage_norm IN ('active', 'ativo') THEN 'ativo'
              WHEN stage_norm IN ('cadastro') THEN 'cadastro'
              WHEN stage_norm IN ('contato feito') THEN 'chamada_1'
              WHEN stage_norm ~ '^1[aª]? ?chamada$' THEN 'chamada_1'
              WHEN stage_norm IN ('contato feito 2') THEN 'chamada_2'
              WHEN stage_norm ~ '^2[aª]? ?chamada$' THEN 'chamada_2'
              WHEN stage_norm ~ '^3[aª]? ?chamada$' THEN 'chamada_3'
              WHEN stage_norm IN ('proposta enviada') THEN 'proposta_enviada'
              WHEN stage_norm IN ('pos-vendas', 'pos vendas', 'posvendas', 'pos-venda', 'pos venda', 'recompra') THEN 'pos_vendas'
              WHEN stage_norm IN ('finalizado') THEN 'finalizado'
              WHEN stage_norm IN ('lixo') THEN 'lixo'
              WHEN stage_norm = '' THEN
                CASE
                  WHEN status_norm IN ('active', 'ativo') THEN 'ativo'
                  WHEN status_norm IN ('waiting', 'aguardando', 'em espera') THEN 'aguardando'
                  ELSE 'triagem'
                END
              ELSE NULL
            END AS stage_key,
            CASE
              WHEN stage_norm IN ('waiting', 'em espera', 'aguardando') THEN 1
              WHEN stage_norm IN ('screening', 'triagem') THEN 2
              WHEN stage_norm IN ('em atendimento') THEN 3
              WHEN stage_norm IN ('active', 'ativo') THEN 4
              WHEN stage_norm IN ('cadastro') THEN 5
              WHEN stage_norm IN ('contato feito') THEN 6
              WHEN stage_norm ~ '^1[aª]? ?chamada$' THEN 6
              WHEN stage_norm IN ('contato feito 2') THEN 7
              WHEN stage_norm ~ '^2[aª]? ?chamada$' THEN 7
              WHEN stage_norm ~ '^3[aª]? ?chamada$' THEN 8
              WHEN stage_norm IN ('proposta enviada') THEN 9
              WHEN stage_norm IN ('pos-vendas', 'pos vendas', 'posvendas', 'pos-venda', 'pos venda', 'recompra') THEN 10
              WHEN stage_norm IN ('finalizado') THEN 11
              WHEN stage_norm IN ('lixo') THEN 12
              WHEN stage_norm = '' THEN
                CASE
                  WHEN status_norm IN ('active', 'ativo') THEN 4
                  WHEN status_norm IN ('waiting', 'aguardando', 'em espera') THEN 1
                  ELSE 2
                END
              ELSE NULL
            END AS stage_order
          FROM (
            SELECT
              f.*,
              translate(
                lower(BTRIM(COALESCE(f.stage_raw, ''))),
                'áàâãäéèêëíìîïóòôõöúùûüç',
                'aaaaaeeeeiiiiooooouuuuc'
              ) AS stage_norm,
              translate(
                lower(BTRIM(COALESCE(f.status_raw, ''))),
                'áàâãäéèêëíìîïóòôõöúùûüç',
                'aaaaaeeeeiiiiooooouuuuc'
              ) AS status_norm
            FROM filtered f
          ) f
        )
        SELECT
          c.stage_key,
          c.stage_order,
          c.chat_id,
          c.cliente_nome,
          c.cliente_telefone,
          c.vendedor_nome,
          c.stage_raw,
          c.created_ts,
          c.updated_ts
        FROM classified c
        WHERE c.stage_key IS NOT NULL
        ORDER BY c.stage_order ASC, c.updated_ts DESC NULLS LAST, c.created_ts DESC NULLS LAST;
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]

        stage_map = {
            key: {"key": key, "label": label, "total": 0, "clients": []}
            for key, label in STAGE_STRATIFICATION_ORDER
        }
        stage_vendor_totals = {key: {} for key, _ in STAGE_STRATIFICATION_ORDER}
        global_vendor_totals = {}

        for row in rows:
            stage_key = row.get("stage_key")
            bucket = stage_map.get(stage_key)
            if not bucket:
                continue
            vendor_name = row.get("vendedor_nome") or "Sem vendedor"
            bucket["total"] += 1
            stage_vendor_totals[stage_key][vendor_name] = (
                stage_vendor_totals[stage_key].get(vendor_name, 0) + 1
            )
            global_vendor_totals[vendor_name] = (
                global_vendor_totals.get(vendor_name, 0) + 1
            )
            if len(bucket["clients"]) < clients_limit:
                bucket["clients"].append(
                    {
                        "chat_id": row.get("chat_id"),
                        "cliente_nome": row.get("cliente_nome") or row.get("chat_id"),
                        "cliente_telefone": row.get("cliente_telefone"),
                        "vendedor_nome": row.get("vendedor_nome"),
                        "stage_raw": row.get("stage_raw"),
                        "updated_at": row.get("updated_ts"),
                        "created_at": row.get("created_ts"),
                    }
                )

        stages = []
        for key, _ in STAGE_STRATIFICATION_ORDER:
            stage_item = stage_map[key]
            stage_item["vendors"] = [
                {"vendedor": name, "total": total}
                for name, total in sorted(
                    stage_vendor_totals[key].items(),
                    key=lambda item: (-item[1], item[0]),
                )
            ]
            stages.append(stage_item)
        total_classified = sum(item["total"] for item in stages)
        vendors = [
            {"vendedor": name, "total": total}
            for name, total in sorted(
                global_vendor_totals.items(),
                key=lambda item: (-item[1], item[0]),
            )
        ]
        return JsonResponse(
            {
                "stages": stages,
                "vendors": vendors,
                "total_classified": total_classified,
                "clients_limit": clients_limit,
            }
        )
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


def dashboard_api(request):
    status = request.GET.get("status")
    etapa = request.GET.get("etapa")
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    vendedor = request.GET.get("vendedor")

    _cache_key = f"dash_{date_from}_{date_to}_{vendedor or ''}_{status or ''}_{etapa or ''}"
    _cached = cache.get(_cache_key)
    if _cached is not None:
        return JsonResponse(_cached)

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

    where_clauses_no_vendor = list(where_clauses)
    params_no_vendor = list(params)

    if vendedor and vendedor != "Todos":
        where_clauses.append("c.attendant_name = %s")
        params.append(vendedor)

    where_sql = "WHERE " + " AND ".join(where_clauses)
    where_sql_no_vendor = "WHERE " + " AND ".join(where_clauses_no_vendor)
    # Unified base: smclick_chat (source 0) + conversations (source 1), mapped to conversations schema
    unified_base_sql = """
      SELECT DISTINCT ON (chat_id)
        chat_id, contact_name, attendant_name, department_name,
        current_funnel_stage, start_time, end_time, created_at, updated_at,
        budget_value, contact_reason, instance_id, ai_agent_rating
      FROM (
        SELECT
          sc.chat_id::text AS chat_id,
          sc.contact_name,
          sc.attendant_name,
          sc.department_name,
          sc.status AS current_funnel_stage,
          sc.chat_created_at AS start_time,
          CASE WHEN sc.status IN ('finished', 'closed') THEN sc.chat_updated_at ELSE NULL END AS end_time,
          sc.inserted_at AS created_at,
          sc.refreshed_at AS updated_at,
          sc.budget_value,
          NULL::text AS contact_reason,
          NULL::text AS instance_id,
          NULL::text AS ai_agent_rating,
          0 AS _src
        FROM smclick_chat sc
        WHERE sc.chat_id IS NOT NULL
        UNION ALL
        SELECT
          c.chat_id, c.contact_name, c.attendant_name,
          NULLIF(BTRIM(COALESCE(
            to_jsonb(c)->>'department_name',
            to_jsonb(c)->>'department',
            ''
          )), '') AS department_name,
          c.current_funnel_stage, c.start_time, c.end_time,
          c.created_at, c.updated_at, c.budget_value, c.contact_reason,
          c.instance_id::text AS instance_id,
          c.ai_agent_rating::text AS ai_agent_rating,
          1 AS _src
        FROM conversations c
        WHERE c.chat_id IS NOT NULL
      ) u
      ORDER BY chat_id, _src ASC, COALESCE(start_time, created_at) DESC NULLS LAST
    """
    filtered_base_sql = f"""
      SELECT DISTINCT ON (c.chat_id)
        c.*
      FROM ({unified_base_sql}) c
      {where_sql}
      ORDER BY
        c.chat_id,
        COALESCE(c.start_time, c.created_at, c.end_time) DESC NULLS LAST,
        c.created_at DESC NULLS LAST
    """
    filtered_base_sql_no_vendor = f"""
      SELECT DISTINCT ON (c.chat_id)
        c.*
      FROM ({unified_base_sql}) c
      {where_sql_no_vendor}
      ORDER BY
        c.chat_id,
        COALESCE(c.start_time, c.created_at, c.end_time) DESC NULLS LAST,
        c.created_at DESC NULLS LAST
    """
    messages_union_sql = _messages_union_sql()
    owners_id_col = _owners_view_id_column()
    owners_cte_sql = ""
    owners_join_sql = ""
    owners_norm_select = "''::text AS owner_norm,"
    if owners_id_col:
        owners_cte_sql = f"""
        ,
        owners_raw AS (
          SELECT
            NULLIF(BTRIM(v.{owners_id_col}::text), '') AS chat_id,
            lower(to_jsonb(v)::text) AS owner_norm
          FROM public.vw_conversations_owners v
          WHERE v.{owners_id_col} IS NOT NULL
        ),
        owners AS (
          SELECT
            chat_id,
            string_agg(owner_norm, ' ' ORDER BY owner_norm) AS owner_norm
          FROM owners_raw
          WHERE chat_id IS NOT NULL
          GROUP BY chat_id
        )
        """
        owners_join_sql = "LEFT JOIN owners o ON o.chat_id = f.chat_id"
        owners_norm_select = "COALESCE(o.owner_norm, ''::text) AS owner_norm,"

    # ── Optimization: materialize filtered as temp table ──────────
    _create_filtered_sql = filtered_base_sql
    _create_filtered_nv_sql = filtered_base_sql_no_vendor
    # Redefine: all f-string queries below will embed a trivial ref
    filtered_base_sql = "SELECT * FROM _tmp_filtered"
    filtered_base_sql_no_vendor = "SELECT * FROM _tmp_filtered_nv"
    # Bot events CTE: reads from pre-computed bot_transfers table
    _bot_events_cte = """
        bot_events AS (
            SELECT bt.chat_id, bt.transfer_ts AS bot_transfer_ts
            FROM bot_transfers bt
            JOIN _tmp_filtered f ON f.chat_id = bt.chat_id
        )
    """

    stats_query = f"""
        WITH filtered AS (
          {filtered_base_sql}
        ),
        {_bot_events_cte},
        conversation_times AS (
          SELECT
            f.chat_id,
            COALESCE(f.start_time, f.created_at) AS opened_ts,
            f.end_time
          FROM filtered f
          WHERE COALESCE(f.start_time, f.created_at) IS NOT NULL
            AND f.end_time IS NOT NULL
            AND LOWER(COALESCE(f.current_funnel_stage, '')) IN ('finalizado', 'finished', 'closed')
        ),
        business_duration AS (
          SELECT
            ct.chat_id,
            SUM(
              GREATEST(
                0,
                EXTRACT(
                  EPOCH FROM (
                    LEAST(ct.end_time AT TIME ZONE 'America/Sao_Paulo', day_end)
                    - GREATEST(ct.opened_ts AT TIME ZONE 'America/Sao_Paulo', day_start)
                  )
                )
              )
            ) AS business_seconds
          FROM conversation_times ct
          JOIN LATERAL (
            SELECT
              day::timestamp + time '08:00' AS day_start,
              day::timestamp + time '18:00' AS day_end
            FROM generate_series(
              date_trunc('day', ct.opened_ts AT TIME ZONE 'America/Sao_Paulo'),
              date_trunc('day', ct.end_time AT TIME ZONE 'America/Sao_Paulo'),
              interval '1 day'
            ) AS day
            WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
          ) d ON TRUE
          GROUP BY ct.chat_id
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
            # ── Materialize filtered base into temp tables (once, not 10×) ──
            cursor.execute("BEGIN")
            cursor.execute(
                f"CREATE TEMP TABLE _tmp_filtered ON COMMIT DROP AS {_create_filtered_sql}",
                params,
            )
            cursor.execute("CREATE INDEX ON _tmp_filtered (chat_id)")
            if vendedor and vendedor != "Todos":
                cursor.execute(
                    f"CREATE TEMP TABLE _tmp_filtered_nv ON COMMIT DROP AS {_create_filtered_nv_sql}",
                    params_no_vendor,
                )
                cursor.execute("CREATE INDEX ON _tmp_filtered_nv (chat_id)")
            else:
                # sem filtro de vendedor: ambas são iguais
                cursor.execute(
                    "CREATE TEMP TABLE _tmp_filtered_nv ON COMMIT DROP AS SELECT * FROM _tmp_filtered"
                )
                cursor.execute("CREATE INDEX ON _tmp_filtered_nv (chat_id)")

            cursor.execute(stats_query)
            stats_row = cursor.fetchone()
            stats = {
                "avg_duration_seconds": float(stats_row[0]) if stats_row[0] is not None else 0,
                "avg_handoff_seconds": float(stats_row[1]) if stats_row[1] is not None else 0,
            }
            cursor.execute(stage_count_query)
            stage_rows = cursor.fetchall()
            stage_counts = [
                {"stage_name": row[0], "total": row[1]} for row in stage_rows
            ]

            cursor.execute(contacts_breakdown_query)
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
            sdr_scope_sql = """
                (
                  translate(
                    lower(COALESCE(f.attendant_name, '')),
                    'áàâãäéèêëíìîïóòôõöúùûüç',
                    'aaaaaeeeeiiiiooooouuuuc'
                  ) ~ '(^|[^a-z])(emill?y|emily)([^a-z]|$)'
                  OR cl.department_norm ~ '(sdr|pre[- ]?venda|triagem)'
                  OR cl.owner_norm ~ '(sdr|pre[- ]?venda|triagem)'
                )
            """
            sdr_summary_query = f"""
                WITH filtered AS (
                  {filtered_base_sql}
                )
                {owners_cte_sql}
                ,
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
                    {owners_norm_select}
                    f.attendant_name,
                    COALESCE(f.department_name, '') AS department_name,
                    translate(
                      lower(COALESCE(f.department_name, '')),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) AS department_norm
                  FROM filtered f
                  {owners_join_sql}
                ),
                message_stats AS (
                  SELECT chat_id,
                         SUM(CASE WHEN outbound THEN 1 ELSE 0 END) AS outbound_count,
                         SUM(CASE WHEN NOT outbound THEN 1 ELSE 0 END) AS inbound_count
                  FROM (
                    SELECT m.chat_id, (m.from_client = false) AS outbound
                    FROM messages m JOIN filtered f ON f.chat_id = m.chat_id
                    UNION ALL
                    SELECT sm.chat_id::text,
                           (sm.from_me = true AND sm.sent_by_name IS NOT NULL) AS outbound
                    FROM smclick_message sm JOIN filtered f ON f.chat_id = sm.chat_id::text
                  ) _ms
                  GROUP BY chat_id
                ),
                {_bot_events_cte}
                SELECT
                  COUNT(*) FILTER (WHERE b.bot_transfer_ts IS NULL) AS total_contacts,
                  COUNT(*) FILTER (
                    WHERE cl.reason_norm ~ 'rastreio'
                       OR cl.owner_norm ~ 'rastreio'
                  ) AS total_tracking,
                  COUNT(*) FILTER (
                    WHERE cl.reason_norm ~ '(sac|pos[- ]?venda|duvidas?|suporte)'
                       OR cl.owner_norm ~ '(sac|pos[- ]?venda|duvidas?|suporte)'
                  ) AS total_sac,
                  COUNT(*) FILTER (
                    WHERE cl.stage_norm IN ('waiting', 'em espera', 'aguardando')
                       OR cl.owner_norm ~ '(waiting|em espera|aguardando)'
                  ) AS total_waiting,
                  COUNT(*) FILTER (
                    WHERE (
                        f.attendant_name IS NOT NULL
                        OR cl.owner_norm ~ '(vendas|venda|comercial)'
                    )
                      AND cl.stage_norm NOT IN ('waiting', 'em espera', 'aguardando')
                      AND cl.reason_norm !~ '(sac|pos[- ]?venda|duvidas?|suporte|rastreio)'
                      AND cl.owner_norm !~ '(sac|pos[- ]?venda|duvidas?|suporte|rastreio|waiting|em espera|aguardando)'
                  ) AS total_sales,
                  COUNT(*) FILTER (WHERE b.bot_transfer_ts IS NOT NULL) AS total_transferred,
                  COUNT(*) FILTER (WHERE COALESCE(ms.outbound_count, 0) = 0) AS total_dead
                FROM filtered f
                JOIN classified cl ON cl.chat_id = f.chat_id
                LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
                LEFT JOIN bot_events b ON b.chat_id = f.chat_id
                WHERE {sdr_scope_sql}
            """

            sdr_daily_query = f"""
                WITH filtered AS (
                  {filtered_base_sql}
                )
                {owners_cte_sql}
                ,
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
                    {owners_norm_select}
                    f.attendant_name,
                    COALESCE(f.department_name, '') AS department_name,
                    translate(
                      lower(COALESCE(f.department_name, '')),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) AS department_norm
                  FROM filtered f
                  {owners_join_sql}
                ),
                message_stats AS (
                  SELECT chat_id,
                         SUM(CASE WHEN outbound THEN 1 ELSE 0 END) AS outbound_count
                  FROM (
                    SELECT m.chat_id, (m.from_client = false) AS outbound
                    FROM messages m JOIN filtered f ON f.chat_id = m.chat_id
                    UNION ALL
                    SELECT sm.chat_id::text,
                           (sm.from_me = true AND sm.sent_by_name IS NOT NULL) AS outbound
                    FROM smclick_message sm JOIN filtered f ON f.chat_id = sm.chat_id::text
                  ) _ms
                  GROUP BY chat_id
                ),
                {_bot_events_cte}
                SELECT
                  date_trunc('day', COALESCE(f.start_time, f.created_at) AT TIME ZONE 'America/Sao_Paulo')::date AS day,
                  COUNT(*) FILTER (WHERE b.bot_transfer_ts IS NULL) AS contacts,
                  COUNT(*) FILTER (
                    WHERE (
                        f.attendant_name IS NOT NULL
                        OR cl.owner_norm ~ '(vendas|venda|comercial)'
                    )
                      AND cl.stage_norm NOT IN ('waiting', 'em espera', 'aguardando')
                      AND cl.reason_norm !~ '(sac|pos[- ]?venda|duvidas?|suporte|rastreio)'
                      AND cl.owner_norm !~ '(sac|pos[- ]?venda|duvidas?|suporte|rastreio|waiting|em espera|aguardando)'
                  ) AS sales,
                  COUNT(*) FILTER (
                    WHERE cl.reason_norm ~ 'rastreio'
                       OR cl.owner_norm ~ 'rastreio'
                  ) AS tracking,
                  COUNT(*) FILTER (
                    WHERE cl.reason_norm ~ '(sac|pos[- ]?venda|duvidas?|suporte)'
                       OR cl.owner_norm ~ '(sac|pos[- ]?venda|duvidas?|suporte)'
                  ) AS sac,
                  COUNT(*) FILTER (
                    WHERE cl.stage_norm IN ('waiting', 'em espera', 'aguardando')
                       OR cl.owner_norm ~ '(waiting|em espera|aguardando)'
                  ) AS waiting,
                  COUNT(*) FILTER (WHERE COALESCE(ms.outbound_count, 0) = 0) AS dead
                FROM filtered f
                JOIN classified cl ON cl.chat_id = f.chat_id
                LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
                LEFT JOIN bot_events b ON b.chat_id = f.chat_id
                WHERE {sdr_scope_sql}
                GROUP BY day
                ORDER BY day;
            """

            sdr_transferred_daily_query = f"""
                WITH filtered AS (
                  {filtered_base_sql}
                )
                {owners_cte_sql}
                ,
                classified AS (
                  SELECT
                    f.chat_id,
                    {owners_norm_select}
                    translate(
                      lower(
                        COALESCE(
                          NULLIF(
                            BTRIM(
                              COALESCE(f.department_name, '')
                            ),
                            ''
                          ),
                          ''
                        )
                      ),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) AS department_norm
                  FROM filtered f
                  {owners_join_sql}
                ),
                {_bot_events_cte}
                SELECT
                  date_trunc('day', b.bot_transfer_ts AT TIME ZONE 'America/Sao_Paulo')::date AS day,
                  COUNT(*) AS transferred
                FROM bot_events b
                JOIN filtered f ON f.chat_id = b.chat_id
                JOIN classified cl ON cl.chat_id = b.chat_id
                WHERE {sdr_scope_sql}
                GROUP BY day
                ORDER BY day;
            """

            sdr_members_query = f"""
                WITH filtered AS (
                  {filtered_base_sql_no_vendor}
                )
                SELECT
                  f.attendant_name AS nome,
                  COALESCE(
                    MAX(
                      NULLIF(
                        BTRIM(
                          COALESCE(f.department_name, '')
                        ),
                        ''
                      )
                    ),
                    '--'
                  ) AS departamento,
                  COUNT(*) AS total_contacts
                FROM filtered f
                WHERE f.attendant_name IS NOT NULL
                  AND (
                    translate(
                      lower(COALESCE(f.attendant_name, '')),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) ~ '(^|[^a-z])(emill?y|emily)([^a-z]|$)'
                    OR translate(
                      lower(
                        COALESCE(f.department_name, '')
                      ),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) ~ '(sdr|pre[- ]?venda|triagem)'
                  )
                GROUP BY f.attendant_name
                ORDER BY total_contacts DESC, f.attendant_name;
            """

            support_reason_pattern = "(pos[- ]?venda|duvidas?|sac|rastreio)"
            budget_outlier_ceiling = 10000000
            sdr_attendant_exclude_sql = """
              (
                translate(
                  lower(COALESCE(f.attendant_name, '')),
                  'áàâãäéèêëíìîïóòôõöúùûüç',
                  'aaaaaeeeeiiiiooooouuuuc'
                ) ~ '(^|[^a-z])(emill?y|emily)([^a-z]|$)'
                OR translate(
                  lower(
                    COALESCE(f.department_name, '')
                  ),
                  'áàâãäéèêëíìîïóòôõöúùûüç',
                  'aaaaaeeeeiiiiooooouuuuc'
                ) ~ '(sdr|pre[- ]?venda|triagem)'
              )
            """

            # Primary load (no vendor selected): use a lightweight query that only
            # counts contacts per vendor — no budget CTEs, no TMA/TME lateral joins.
            # Full query runs only for the vendor-specific call (~400 conversations).
            include_tma_tme = bool(vendedor and vendedor != "Todos")

            if not include_tma_tme:
                vendor_summary_query = f"""
                    WITH filtered_base AS (
                      {filtered_base_sql}
                    ),
                    filtered AS (
                      SELECT fb.*,
                        translate(lower(COALESCE(fb.contact_reason, '')),
                          'áàâãäéèêëíìîïóòôõöúùûüç', 'aaaaaeeeeiiiiooooouuuuc'
                        ) AS reason_norm
                      FROM filtered_base fb
                    )
                    SELECT
                      f.attendant_name AS vendedor,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(f.current_funnel_stage, '')) NOT IN
                          ('finalizado', 'finished', 'closed', 'lixo')
                      ) AS contacts_received,
                      0 AS budgets_count,
                      0 AS budgets_detected_count,
                      0::float AS budgets_sum,
                      0::float AS budgets_sum_detected,
                      0 AS dead_contacts,
                      0::float AS avg_duration_seconds,
                      0::float AS avg_handoff_seconds,
                      0::float AS avg_score
                    FROM filtered f
                    WHERE f.attendant_name IS NOT NULL
                      AND NOT ({sdr_attendant_exclude_sql})
                    GROUP BY f.attendant_name
                    ORDER BY contacts_received DESC;
                """
            else:
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
                messages_union AS (
                  {messages_union_sql}
                ),
                structured_budget_values AS (
                  SELECT
                    src.chat_id,
                    MAX(src.budget_value) AS budget_value
                  FROM (
                    SELECT
                      f.chat_id,
                      CASE
                        WHEN f.budget_value > 0 THEN f.budget_value
                        ELSE NULL
                      END AS budget_value
                    FROM filtered f

                    UNION ALL

                    SELECT
                      f.chat_id,
                      CASE
                        WHEN raw.raw_budget_txt IS NULL THEN NULL
                        WHEN REPLACE(REGEXP_REPLACE(raw.raw_budget_txt, '[^0-9,.-]', '', 'g'), ',', '.') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                          THEN REPLACE(REGEXP_REPLACE(raw.raw_budget_txt, '[^0-9,.-]', '', 'g'), ',', '.')::numeric
                        WHEN REPLACE(REPLACE(REGEXP_REPLACE(raw.raw_budget_txt, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.') ~ '^-?[0-9]+(\\.[0-9]+)?$'
                          THEN REPLACE(REPLACE(REGEXP_REPLACE(raw.raw_budget_txt, '[^0-9,.-]', '', 'g'), '.', ''), ',', '.')::numeric
                        ELSE NULL
                      END AS budget_value
                    FROM filtered f
                    JOIN public.semclick_conversations sc
                      ON sc.chat_id::text = f.chat_id::text
                    CROSS JOIN LATERAL (
                      SELECT NULLIF(
                        BTRIM(
                          COALESCE(
                            to_jsonb(sc)->>'valor_orcamento_atual',
                            to_jsonb(sc)->>'valor_orcamento',
                            to_jsonb(sc)->>'budget_value',
                            ''
                          )
                        ),
                        ''
                      ) AS raw_budget_txt
                    ) raw
                  ) src
                  WHERE src.budget_value IS NOT NULL
                    AND src.budget_value > 0
                    AND src.budget_value <= {budget_outlier_ceiling}
                  GROUP BY src.chat_id
                ),
                {_bot_events_cte},
                message_stats AS (
                  SELECT chat_id,
                         SUM(CASE WHEN outbound THEN 1 ELSE 0 END) AS outbound_count
                  FROM (
                    SELECT m.chat_id, (m.from_client = false) AS outbound
                    FROM messages m JOIN filtered f ON f.chat_id = m.chat_id
                    UNION ALL
                    SELECT sm.chat_id::text,
                           (sm.from_me = true AND sm.sent_by_name IS NOT NULL) AS outbound
                    FROM smclick_message sm JOIN filtered f ON f.chat_id = sm.chat_id::text
                  ) _ms
                  GROUP BY chat_id
                ),
                conversation_times AS (
                  SELECT
                    f.chat_id,
                    COALESCE(f.start_time, f.created_at) AS opened_ts,
                    f.end_time
                  FROM filtered f
                  WHERE COALESCE(f.start_time, f.created_at) IS NOT NULL
                    AND f.end_time IS NOT NULL
                    AND LOWER(COALESCE(f.current_funnel_stage, '')) IN ('finalizado', 'finished', 'closed')
                ),
                budget_values AS (
                  SELECT
                    src.chat_id_txt,
                    MAX(src.msg_budget) AS max_budget_msg
                  FROM (
                    SELECT
                      mu.chat_id AS chat_id_txt,
                      NULLIF(
                        REPLACE(REPLACE((matches)[1], '.', ''), ',', '.'),
                        ''
                      )::numeric AS msg_budget
                    FROM messages_union mu
                    JOIN filtered f ON f.chat_id::text = mu.chat_id
                    JOIN LATERAL regexp_matches(
                      translate(
                        lower(COALESCE(mu.msg_conteudo, '')),
                        'áàâãäéèêëíìîïóòôõöúùûüç',
                        'aaaaaeeeeiiiiooooouuuuc'
                      ),
                      'total\\s*[:\\-]?\\s*r\\$\\s*([0-9\\.]+(?:,[0-9]{2})?)',
                      'g'
                    ) AS matches ON TRUE
                    WHERE mu.msg_conteudo IS NOT NULL
                  ) src
                  WHERE src.msg_budget IS NOT NULL
                    AND src.msg_budget > 0
                    AND src.msg_budget <= {budget_outlier_ceiling}
                  GROUP BY src.chat_id_txt
                ),
                business_duration AS (
                  SELECT
                    ct.chat_id,
                    SUM(
                      GREATEST(
                        0,
                        EXTRACT(
                          EPOCH FROM (
                            LEAST(ct.end_time AT TIME ZONE 'America/Sao_Paulo', day_end)
                            - GREATEST(ct.opened_ts AT TIME ZONE 'America/Sao_Paulo', day_start)
                          )
                        )
                      )
                    ) AS business_seconds
                  FROM conversation_times ct
                  JOIN LATERAL (
                    SELECT
                      day::timestamp + time '08:00' AS day_start,
                      day::timestamp + time '18:00' AS day_end
                    FROM generate_series(
                      date_trunc('day', ct.opened_ts AT TIME ZONE 'America/Sao_Paulo'),
                      date_trunc('day', ct.end_time AT TIME ZONE 'America/Sao_Paulo'),
                      interval '1 day'
                    ) AS day
                    WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
                  ) d ON TRUE
                  GROUP BY ct.chat_id
                ),
                human_events AS (
                  SELECT chat_id, MIN(first_ts) AS first_human_ts FROM (
                    SELECT
                      m.chat_id,
                      MIN(m."timestamp") AS first_ts
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
                    UNION ALL
                    SELECT
                      sm.chat_id::text,
                      MIN(sm.event_time) AS first_ts
                    FROM smclick_message sm
                    JOIN bot_events b ON b.chat_id = sm.chat_id::text
                    WHERE sm.from_me = true
                      AND (sm.sent_by_name IS NOT NULL OR (sm.content_text IS NOT NULL AND sm.content_text ~ '^\*[^*]+\*'))
                      AND sm.event_time > b.bot_transfer_ts
                    GROUP BY sm.chat_id::text
                  ) _he GROUP BY chat_id
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
                ),
                first_vendor_msg AS (
                  SELECT chat_id, MIN(first_ts) AS first_ts FROM (
                    SELECT
                      m.chat_id,
                      MIN(m."timestamp") AS first_ts
                    FROM messages m
                    JOIN filtered f ON f.chat_id = m.chat_id
                    WHERE m.from_client = false
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
                    UNION ALL
                    SELECT
                      sm.chat_id::text,
                      MIN(sm.event_time) AS first_ts
                    FROM smclick_message sm
                    JOIN filtered f ON f.chat_id = sm.chat_id::text
                    WHERE sm.from_me = true
                      AND (sm.sent_by_name IS NOT NULL OR (sm.content_text IS NOT NULL AND sm.content_text ~ '^\*[^*]+\*'))
                    GROUP BY sm.chat_id::text
                  ) _fv GROUP BY chat_id
                ),
                last_msg_ts AS (
                  SELECT chat_id, MAX(last_ts) AS last_ts FROM (
                    SELECT m.chat_id, MAX(m."timestamp") AS last_ts
                    FROM messages m JOIN filtered f ON f.chat_id = m.chat_id
                    GROUP BY m.chat_id
                    UNION ALL
                    SELECT sm.chat_id::text, MAX(sm.event_time) AS last_ts
                    FROM smclick_message sm JOIN filtered f ON f.chat_id = sm.chat_id::text
                    GROUP BY sm.chat_id::text
                  ) _lm GROUP BY chat_id
                ),
                direct_handoff_business AS (
                  SELECT
                    f.chat_id,
                    SUM(
                      GREATEST(0, EXTRACT(EPOCH FROM (
                        LEAST(fv.first_ts AT TIME ZONE 'America/Sao_Paulo', d.day_end)
                        - GREATEST(COALESCE(f.start_time, f.created_at) AT TIME ZONE 'America/Sao_Paulo', d.day_start)
                      )))
                    ) AS handoff_seconds
                  FROM filtered f
                  JOIN first_vendor_msg fv ON fv.chat_id = f.chat_id
                  JOIN LATERAL (
                    SELECT
                      day::timestamp + time '08:00' AS day_start,
                      day::timestamp + time '18:00' AS day_end
                    FROM generate_series(
                      date_trunc('day', COALESCE(f.start_time, f.created_at) AT TIME ZONE 'America/Sao_Paulo'),
                      date_trunc('day', fv.first_ts AT TIME ZONE 'America/Sao_Paulo'),
                      interval '1 day'
                    ) AS day
                    WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
                  ) d ON TRUE
                  WHERE COALESCE(f.start_time, f.created_at) IS NOT NULL
                    AND fv.first_ts IS NOT NULL
                  GROUP BY f.chat_id
                ),
                direct_duration_business AS (
                  SELECT
                    f.chat_id,
                    SUM(
                      GREATEST(0, EXTRACT(EPOCH FROM (
                        LEAST(COALESCE(f.end_time, lm.last_ts) AT TIME ZONE 'America/Sao_Paulo', d.day_end)
                        - GREATEST(COALESCE(f.start_time, f.created_at) AT TIME ZONE 'America/Sao_Paulo', d.day_start)
                      )))
                    ) AS duration_seconds
                  FROM filtered f
                  JOIN last_msg_ts lm ON lm.chat_id = f.chat_id
                  JOIN LATERAL (
                    SELECT
                      day::timestamp + time '08:00' AS day_start,
                      day::timestamp + time '18:00' AS day_end
                    FROM generate_series(
                      date_trunc('day', COALESCE(f.start_time, f.created_at) AT TIME ZONE 'America/Sao_Paulo'),
                      date_trunc('day', COALESCE(f.end_time, lm.last_ts) AT TIME ZONE 'America/Sao_Paulo'),
                      interval '1 day'
                    ) AS day
                    WHERE EXTRACT(DOW FROM day) BETWEEN 1 AND 5
                  ) d ON TRUE
                  WHERE COALESCE(f.start_time, f.created_at) IS NOT NULL
                    AND COALESCE(f.end_time, lm.last_ts) IS NOT NULL
                  GROUP BY f.chat_id
                ),
                direct_metrics AS (
                  SELECT
                    f.chat_id,
                    dhb.handoff_seconds,
                    ddb.duration_seconds
                  FROM filtered f
                  LEFT JOIN direct_handoff_business dhb ON dhb.chat_id = f.chat_id
                  LEFT JOIN direct_duration_business ddb ON ddb.chat_id = f.chat_id
                )
                SELECT
                  f.attendant_name AS vendedor,
                  COUNT(*) FILTER (
                    WHERE LOWER(COALESCE(f.current_funnel_stage, '')) NOT IN ('finalizado', 'finished', 'closed', 'lixo')
                  ) AS contacts_received,
                  COUNT(*) FILTER (
                    WHERE COALESCE(sbv.budget_value, 0) > 0
                      AND NOT (f.reason_norm ~ '{support_reason_pattern}')
                  ) AS budgets_count,
                  COUNT(*) FILTER (
                    WHERE (
                        COALESCE(sbv.budget_value, 0) > 0
                        OR (
                          COALESCE(sbv.budget_value, 0) = 0
                          AND bv.max_budget_msg IS NOT NULL
                        )
                      )
                      AND NOT (f.reason_norm ~ '{support_reason_pattern}')
                  ) AS budgets_detected_count,
                  COALESCE(
                    SUM(
                      CASE
                        WHEN COALESCE(sbv.budget_value, 0) > 0
                          AND NOT (f.reason_norm ~ '{support_reason_pattern}')
                        THEN sbv.budget_value
                        ELSE 0
                      END
                    ),
                    0
                  ) AS budgets_sum,
                  COALESCE(
                    SUM(
                      CASE
                        WHEN f.reason_norm ~ '{support_reason_pattern}' THEN 0
                        WHEN COALESCE(sbv.budget_value, 0) > 0 THEN sbv.budget_value
                        ELSE COALESCE(bv.max_budget_msg, 0)
                      END
                    ),
                    0
                  ) AS budgets_sum_detected,
                  COUNT(*) FILTER (WHERE COALESCE(ms.outbound_count, 0) = 0) AS dead_contacts,
                  AVG(COALESCE(bd.business_seconds, dm.duration_seconds)) AS avg_duration_seconds,
                  AVG(COALESCE(bh.business_seconds, dm.handoff_seconds)) AS avg_handoff_seconds,
                  AVG(
                    NULLIF(
                      REGEXP_REPLACE(COALESCE(f.ai_agent_rating::text, ''), '[^0-9\\.]', '', 'g'),
                      ''
                    )::numeric
                  ) AS avg_score
                FROM filtered f
                LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
                LEFT JOIN structured_budget_values sbv ON sbv.chat_id = f.chat_id
                LEFT JOIN budget_values bv ON bv.chat_id_txt = f.chat_id::text
                LEFT JOIN business_duration bd ON bd.chat_id = f.chat_id
                LEFT JOIN business_handoff bh ON bh.chat_id = f.chat_id
                LEFT JOIN direct_metrics dm ON dm.chat_id = f.chat_id
                WHERE f.attendant_name IS NOT NULL
                  AND NOT ({sdr_attendant_exclude_sql})
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
                  AND NOT ({sdr_attendant_exclude_sql})
                GROUP BY f.attendant_name, score
                ORDER BY f.attendant_name, score;
            """

            cursor.execute(sdr_summary_query)
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

            cursor.execute(sdr_daily_query)
            sdr_daily_rows = cursor.fetchall()
            sdr_daily = [
                {
                    "day": row[0].isoformat() if row[0] else None,
                    "contacts": row[1],
                    "sales": row[2],
                    "tracking": row[3],
                    "sac": row[4],
                    "waiting": row[5],
                    "dead": row[6],
                }
                for row in sdr_daily_rows
            ]

            cursor.execute(sdr_transferred_daily_query)
            transferred_rows = cursor.fetchall()
            sdr_transferred_daily = [
                {
                    "day": row[0].isoformat() if row[0] else None,
                    "transferred": row[1],
                }
                for row in transferred_rows
            ]

            cursor.execute(sdr_members_query)
            sdr_member_rows = cursor.fetchall()
            sdr_members = [
                {
                    "nome": row[0],
                    "departamento": row[1] or "--",
                    "total_contacts": row[2] or 0,
                }
                for row in sdr_member_rows
            ]

            cursor.execute(vendor_summary_query)
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

            cursor.execute(vendor_scores_query)
            score_rows = cursor.fetchall()
            vendor_scores = {}
            for vendedor, score, total in score_rows:
                vendor_scores.setdefault(vendedor, []).append(
                    {"score": score, "total": total}
                )
            cursor.execute("COMMIT")

        _response_data = {
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
                "members": sdr_members,
            },
            "vendors": {
                "summary": vendors,
                "scores": vendor_scores,
            },
        }
        _cache_ttl = 60 if (vendedor and vendedor != "Todos") else 180
        cache.set(_cache_key, _response_data, timeout=_cache_ttl)
        return JsonResponse(_response_data)
    except Exception as exc:
        try:
            connection.cursor().execute("ROLLBACK")
        except Exception:
            pass
        return JsonResponse({"error": str(exc)}, status=500)


def dead_conversations_api(request):
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    vendedor = request.GET.get("vendedor")

    where_clauses = ["c.chat_id IS NOT NULL"]
    params = []
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

    unified_base_sql = """
      SELECT DISTINCT ON (chat_id)
        chat_id, contact_name, attendant_name,
        current_funnel_stage, start_time, end_time, created_at, updated_at
      FROM (
        SELECT
          sc.chat_id::text AS chat_id,
          sc.contact_name,
          sc.attendant_name,
          sc.status AS current_funnel_stage,
          sc.chat_created_at AS start_time,
          CASE WHEN sc.status IN ('finished', 'closed') THEN sc.chat_updated_at ELSE NULL END AS end_time,
          sc.inserted_at AS created_at,
          sc.refreshed_at AS updated_at,
          0 AS _src
        FROM smclick_chat sc
        WHERE sc.chat_id IS NOT NULL
        UNION ALL
        SELECT
          c.chat_id, c.contact_name, c.attendant_name,
          c.current_funnel_stage, c.start_time, c.end_time,
          c.created_at, c.updated_at,
          1 AS _src
        FROM conversations c
        WHERE c.chat_id IS NOT NULL
      ) u
      ORDER BY chat_id, _src ASC, COALESCE(start_time, created_at) DESC NULLS LAST
    """

    query = f"""
        WITH filtered AS (
          SELECT DISTINCT ON (c.chat_id) c.*
          FROM ({unified_base_sql}) c
          {where_sql}
          ORDER BY c.chat_id, COALESCE(c.start_time, c.created_at) DESC NULLS LAST
        ),
        message_stats AS (
          SELECT chat_id,
                 SUM(CASE WHEN outbound THEN 1 ELSE 0 END) AS outbound_count
          FROM (
            SELECT m.chat_id, (m.from_client = false) AS outbound
            FROM messages m JOIN filtered f ON f.chat_id = m.chat_id
            UNION ALL
            SELECT sm.chat_id::text,
                   (sm.from_me = true AND sm.sent_by_name IS NOT NULL) AS outbound
            FROM smclick_message sm JOIN filtered f ON f.chat_id = sm.chat_id::text
          ) _ms
          GROUP BY chat_id
        )
        SELECT
          f.chat_id,
          f.contact_name AS cliente_nome,
          f.attendant_name AS vendedor_nome,
          COALESCE(f.updated_at, f.created_at, f.start_time) AS last_seen
        FROM filtered f
        LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
        WHERE COALESCE(ms.outbound_count, 0) = 0
        ORDER BY last_seen DESC NULLS LAST
        LIMIT 300
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            columns = [col[0] for col in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
        return JsonResponse({"conversations": rows})
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


def index(request):
    return render(request, "dashboard/index.html", {"debug": settings.DEBUG})


def alerts_api(request):
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    vendedor = request.GET.get("vendedor")

    where_clauses = ["c.chat_id IS NOT NULL"]
    params = []
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

    unified_base_sql = """
      SELECT DISTINCT ON (chat_id)
        chat_id, contact_name, attendant_name,
        current_funnel_stage, start_time, end_time, created_at, updated_at, budget_value
      FROM (
        SELECT
          sc.chat_id::text AS chat_id, sc.contact_name, sc.attendant_name,
          sc.status AS current_funnel_stage,
          sc.chat_created_at AS start_time,
          CASE WHEN sc.status IN ('finished', 'closed') THEN sc.chat_updated_at ELSE NULL END AS end_time,
          sc.inserted_at AS created_at, sc.refreshed_at AS updated_at,
          sc.budget_value, 0 AS _src
        FROM smclick_chat sc WHERE sc.chat_id IS NOT NULL
        UNION ALL
        SELECT
          c.chat_id, c.contact_name, c.attendant_name,
          c.current_funnel_stage, c.start_time, c.end_time,
          c.created_at, c.updated_at, c.budget_value, 1 AS _src
        FROM conversations c WHERE c.chat_id IS NOT NULL
      ) u
      ORDER BY chat_id, _src ASC, COALESCE(start_time, created_at) DESC NULLS LAST
    """

    closed_stages = "('finished','closed','Finalizado','finalizado','Lixo','lixo')"

    query = f"""
      WITH filtered AS (
        SELECT DISTINCT ON (c.chat_id) c.*
        FROM ({unified_base_sql}) c
        {where_sql}
        ORDER BY c.chat_id, COALESCE(c.start_time, c.created_at) DESC NULLS LAST
      ),
      -- Handoff: first human vendor message per chat (sent_by_name populated OR *Name* pattern)
      handoff_ts AS (
        SELECT chat_id::text AS chat_id, MIN(event_time) AS ts
        FROM smclick_message
        WHERE from_me = true
          AND (
            sent_by_name IS NOT NULL
            OR (content_text IS NOT NULL AND content_text ~ '^\*[^*]+\*')
          )
        GROUP BY chat_id
      ),
      -- After handoff, any from_me=true message belongs to the vendor
      last_vendor AS (
        SELECT sm.chat_id::text AS chat_id, MAX(sm.event_time) AS ts
        FROM smclick_message sm
        JOIN handoff_ts ht ON ht.chat_id = sm.chat_id::text
        WHERE sm.from_me = true AND sm.event_time >= ht.ts
        GROUP BY sm.chat_id
      ),
      -- Last message after handoff (client or vendor)
      last_msg AS (
        SELECT DISTINCT ON (chat_id) chat_id, from_me, event_time
        FROM (
          SELECT sm.chat_id::text AS chat_id, sm.from_me, sm.event_time
          FROM smclick_message sm
          JOIN handoff_ts ht ON ht.chat_id = sm.chat_id::text
          WHERE sm.event_time >= ht.ts
        ) _lm
        ORDER BY chat_id, event_time DESC
      ),
      -- Last vendor media after handoff
      last_vendor_media AS (
        SELECT DISTINCT ON (chat_id) chat_id, media_ts
        FROM (
          SELECT sm.chat_id::text AS chat_id, sm.event_time AS media_ts
          FROM smclick_message sm
          JOIN handoff_ts ht ON ht.chat_id = sm.chat_id::text
          WHERE sm.from_me = true AND sm.event_time >= ht.ts
            AND sm.message_type IN ('image','video','document','ptt','audio')
        ) _lvm
        ORDER BY chat_id, media_ts DESC
      ),
      -- Vendor text message after last vendor media
      post_media_text AS (
        SELECT DISTINCT sm.chat_id::text AS chat_id
        FROM smclick_message sm
        JOIN last_vendor_media lm ON lm.chat_id = sm.chat_id::text
        JOIN handoff_ts ht ON ht.chat_id = sm.chat_id::text
        WHERE sm.from_me = true
          AND sm.event_time > lm.media_ts
          AND sm.event_time >= ht.ts
          AND sm.message_type NOT IN ('image','video','document','ptt','audio')
          AND sm.content_text IS NOT NULL AND LENGTH(sm.content_text) > 5
      ),
      a_sem_retorno AS (
        SELECT
          f.chat_id, f.contact_name AS cliente_nome, f.attendant_name AS vendedor_nome,
          ROUND(EXTRACT(EPOCH FROM (NOW() - COALESCE(lv.ts, f.created_at))) / 86400)::int AS extra_int
        FROM filtered f
        LEFT JOIN last_vendor lv ON lv.chat_id = f.chat_id
        WHERE f.current_funnel_stage NOT IN {closed_stages}
          AND COALESCE(lv.ts, f.created_at) < NOW() - INTERVAL '2 days'
      ),
      a_aguardando AS (
        SELECT f.chat_id, f.contact_name AS cliente_nome, f.attendant_name AS vendedor_nome,
               lm.event_time AS desde
        FROM filtered f
        JOIN last_msg lm ON lm.chat_id = f.chat_id
        WHERE lm.from_me = false
          AND f.current_funnel_stage NOT IN {closed_stages}
      ),
      a_midia_sem_info AS (
        SELECT f.chat_id, f.contact_name AS cliente_nome, f.attendant_name AS vendedor_nome,
               lm.media_ts
        FROM filtered f
        JOIN last_vendor_media lm ON lm.chat_id = f.chat_id
        WHERE f.chat_id NOT IN (SELECT chat_id FROM post_media_text)
          AND f.current_funnel_stage NOT IN {closed_stages}
      ),
      a_orcamento_sem_followup AS (
        SELECT f.chat_id, f.contact_name AS cliente_nome, f.attendant_name AS vendedor_nome,
               ROUND(EXTRACT(EPOCH FROM (NOW() - COALESCE(lv.ts, f.created_at))) / 86400)::int AS extra_int
        FROM filtered f
        LEFT JOIN last_vendor lv ON lv.chat_id = f.chat_id
        WHERE f.budget_value IS NOT NULL AND f.budget_value > 0
          AND f.current_funnel_stage NOT IN {closed_stages}
          AND COALESCE(lv.ts, f.created_at) < NOW() - INTERVAL '2 days'
      )

      SELECT * FROM (
        SELECT 'sem_retorno_2d' AS alert_type, chat_id, cliente_nome, vendedor_nome,
               extra_int::text AS extra
        FROM a_sem_retorno ORDER BY extra_int DESC
      ) _r1
      UNION ALL
      SELECT * FROM (
        SELECT 'aguardando_resposta', chat_id, cliente_nome, vendedor_nome,
               TO_CHAR(desde AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI')
        FROM a_aguardando ORDER BY desde ASC
      ) _r2
      UNION ALL
      SELECT * FROM (
        SELECT 'midia_sem_info', chat_id, cliente_nome, vendedor_nome,
               TO_CHAR(media_ts AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI')
        FROM a_midia_sem_info ORDER BY media_ts DESC
      ) _r3
      UNION ALL
      SELECT * FROM (
        SELECT 'orcamento_sem_followup', chat_id, cliente_nome, vendedor_nome,
               extra_int::text
        FROM a_orcamento_sem_followup ORDER BY extra_int DESC
      ) _r4
    """

    try:
        with connection.cursor() as cursor:
            cursor.execute(query, params)
            rows = cursor.fetchall()

        result = {
            "sem_retorno_2d": [],
            "aguardando_resposta": [],
            "midia_sem_info": [],
            "orcamento_sem_followup": [],
        }
        for alert_type, chat_id, cliente_nome, vendedor_nome, extra in rows:
            result[alert_type].append({
                "chat_id": chat_id,
                "cliente_nome": cliente_nome,
                "vendedor_nome": vendedor_nome,
                "extra": extra,
            })
        return JsonResponse(result)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)
