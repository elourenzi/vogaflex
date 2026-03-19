import hashlib
import json

from django.conf import settings
from django.core.cache import cache
from django.db import connection, transaction
from django.http import JsonResponse
from django.shortcuts import render
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST

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
          (sm.chat_id::text || '|' || sm.message_id) AS source_id,
          sm.chat_id::text AS chat_id,
          COALESCE(sm.event_time, sm.sent_at) AS evento_timestamp,
          NULLIF(BTRIM(COALESCE(sm.message_type, '')), '') AS msg_tipo,
          NULLIF(BTRIM(COALESCE(sm.content_original_text, sm.content_text, '')), '') AS msg_conteudo,
          CASE
            WHEN sm.from_me IS TRUE THEN FALSE
            WHEN sm.from_me IS FALSE THEN TRUE
            ELSE NULL
          END AS msg_from_client,
          CASE
            WHEN sm.message_status IS TRUE THEN NULL
            WHEN sm.message_status IS FALSE THEN COALESCE(NULLIF(BTRIM(sm.fail_reason), ''), 'false')
            ELSE NULL
          END AS msg_status_envio,
          NULLIF(BTRIM(COALESCE(sm.sent_by_name, '')), '') AS sent_by_name
        FROM smclick_message sm
        WHERE sm.chat_id IS NOT NULL
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
        WITH latest_message AS (
          SELECT DISTINCT ON (sm.chat_id)
            sm.chat_id::text AS chat_id,
            NULLIF(BTRIM(COALESCE(sm.message_type, '')), '') AS msg_tipo,
            NULLIF(BTRIM(COALESCE(sm.content_original_text, sm.content_text, '')), '') AS msg_conteudo,
            CASE WHEN sm.from_me IS TRUE THEN FALSE
                 WHEN sm.from_me IS FALSE THEN TRUE
                 ELSE NULL
            END AS msg_from_client,
            CASE WHEN sm.message_status IS TRUE THEN NULL
                 WHEN sm.message_status IS FALSE THEN COALESCE(NULLIF(BTRIM(sm.fail_reason), ''), 'false')
                 ELSE NULL
            END AS msg_status_envio,
            COALESCE(sm.event_time, sm.sent_at) AS evento_timestamp
          FROM smclick_message sm
          ORDER BY sm.chat_id, COALESCE(sm.event_time, sm.sent_at) DESC NULLS LAST
        ),
        conv AS (
          SELECT
            sc.chat_id::text AS chat_id,
            sc.protocol::text AS protocolo,
            sc.contact_name AS cliente_nome,
            sc.contact_phone AS cliente_telefone,
            sc.attendant_name AS vendedor_nome,
            sc.attendant_email AS vendedor_email,
            sc.status AS status_conversa,
            sc.current_stage AS etapa_funil,
            sc.department_name AS departamento,
            sc.current_stage AS coluna_kanban,
            sc.chat_created_at AS data_criacao_chat,
            CASE WHEN sc.status IN ('finished', 'closed') THEN sc.chat_updated_at ELSE NULL END AS data_fechamento,
            COALESCE(sc.budget_value, 0) AS valor_orcamento,
            sc.order_value AS valor_pedido,
            sc.loss_reason AS motivo_perda,
            sc.product AS produto_interesse,
            COALESCE(sc.last_event_at, sc.refreshed_at, sc.chat_updated_at) AS updated_at,
            sc.inserted_at AS created_at,
            CASE
              WHEN sc.status = 'screening' THEN 'Triagem'
              WHEN sc.status = 'waiting' THEN 'Aguardando'
              WHEN sc.status = 'active' THEN 'Em atendimento'
              WHEN sc.status IN ('finished', 'closed') THEN 'Finalizado'
              ELSE NULL
            END AS status_normalizado
          FROM smclick_chat sc
          WHERE sc.chat_id IS NOT NULL
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
          c.updated_at,
          c.created_at,
          lm.msg_tipo,
          lm.msg_conteudo,
          lm.msg_status_envio,
          lm.evento_timestamp,
          lm.msg_from_client
        FROM conv c
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

    query = """
        SELECT
          (sm.chat_id::text || '|' || sm.message_id) AS id,
          sm.chat_id::text AS chat_id,
          COALESCE(sm.event_time, sm.sent_at) AS evento_timestamp,
          NULLIF(BTRIM(COALESCE(sm.content_original_text, sm.content_text, '')), '') AS msg_conteudo,
          NULLIF(BTRIM(COALESCE(sm.message_type, '')), '') AS msg_tipo,
          CASE WHEN sm.from_me IS TRUE THEN FALSE
               WHEN sm.from_me IS FALSE THEN TRUE
               ELSE NULL
          END AS msg_from_client,
          CASE WHEN sm.message_status IS TRUE THEN NULL
               WHEN sm.message_status IS FALSE THEN COALESCE(NULLIF(BTRIM(sm.fail_reason), ''), 'false')
               ELSE NULL
          END AS msg_status_envio,
          NULLIF(BTRIM(COALESCE(sm.sent_by_name, '')), '') AS sent_by_name
        FROM smclick_message sm
        WHERE sm.chat_id::text = %s
          AND (sm.message_type IS NOT NULL OR sm.content_text IS NOT NULL)
        ORDER BY COALESCE(sm.event_time, sm.sent_at) ASC NULLS LAST, sm.message_id ASC
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
        WITH conv AS (
          SELECT
            sc.chat_id::text AS chat_id,
            sc.contact_name AS cliente_nome,
            sc.contact_phone AS cliente_telefone,
            sc.attendant_name AS vendedor_nome,
            COALESCE(sc.current_stage, sc.status) AS stage_raw,
            sc.status AS status_raw,
            sc.chat_updated_at AS updated_ts,
            sc.chat_created_at AS created_ts
          FROM smclick_chat sc
          WHERE sc.chat_id IS NOT NULL
        ),
        filtered_raw AS (
          SELECT *
          FROM conv c
          {where_sql}
        ),
        filtered AS (
          SELECT DISTINCT ON (COALESCE(NULLIF(f.cliente_telefone, ''), f.chat_id))
            f.*
          FROM filtered_raw f
          ORDER BY
            COALESCE(NULLIF(f.cliente_telefone, ''), f.chat_id),
            CASE
              WHEN f.stage_raw NOT IN ('waiting', 'screening', 'finished', 'closed', 'active', 'pre-finish')
                   AND f.stage_raw IS NOT NULL THEN 0
              WHEN f.stage_raw = 'active' THEN 1
              WHEN f.stage_raw = 'screening' THEN 2
              WHEN f.stage_raw = 'waiting' THEN 3
              WHEN f.stage_raw = 'finished' THEN 4
              WHEN f.stage_raw = 'closed' THEN 5
              WHEN f.stage_raw = 'pre-finish' THEN 5
              ELSE 6
            END,
            f.updated_ts DESC NULLS LAST
        ),
        chat_msg_flags AS (
          SELECT sm.chat_id::text AS chat_id,
            bool_or(sm.from_me = true AND sm.sent_by_name IS NOT NULL AND sm.sent_by_name != '') AS has_vendor_msg,
            bool_and(sm.from_me = true AND sm.message_type = 'template') AND NOT bool_or(sm.from_me = false) AS is_template_only
          FROM smclick_message sm
          JOIN filtered f ON f.chat_id = sm.chat_id::text
          GROUP BY sm.chat_id
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
            f.is_template_only,
            CASE
              WHEN stage_norm IN ('waiting', 'screening') AND f.has_vendor_msg THEN 'em_atendimento'
              WHEN stage_norm = 'waiting' THEN 'aguardando'
              WHEN stage_norm = 'screening' THEN 'triagem'
              WHEN stage_norm = 'active' THEN 'ativo'
              WHEN stage_norm IN ('em atendimento', 'andamento') THEN 'em_atendimento'
              WHEN stage_norm IN ('cadastro') THEN 'cadastro'
              WHEN stage_norm IN ('contato feito', '1a chamada') THEN 'chamada_1'
              WHEN lower(BTRIM(COALESCE(f.stage_raw, ''))) LIKE '1%%chamada' THEN 'chamada_1'
              WHEN stage_norm IN ('contato feito 2', '2a chamada') THEN 'chamada_2'
              WHEN lower(BTRIM(COALESCE(f.stage_raw, ''))) LIKE '2%%chamada' THEN 'chamada_2'
              WHEN stage_norm IN ('3a chamada') THEN 'chamada_3'
              WHEN lower(BTRIM(COALESCE(f.stage_raw, ''))) LIKE '3%%chamada' THEN 'chamada_3'
              WHEN stage_norm IN ('proposta enviada') THEN 'proposta_enviada'
              WHEN stage_norm IN ('pos-vendas', 'pos vendas', 'posvendas', 'pos-venda', 'pos venda', 'recompra',
                                  'aguardando entrega', 'aguardando coleta') THEN 'pos_vendas'
              WHEN stage_norm IN ('finished', 'closed', 'finalizado', 'pre-finish') THEN 'finalizado'
              WHEN stage_norm IN ('lixo', 'leads mortos') THEN 'lixo'
              WHEN stage_norm IN ('atualizados', 'atualizacao', 'tarefas') THEN 'ativo'
              WHEN stage_norm = '' THEN
                CASE
                  WHEN status_norm = 'active' THEN 'ativo'
                  WHEN status_norm = 'waiting' THEN 'aguardando'
                  ELSE 'triagem'
                END
              ELSE NULL
            END AS stage_key,
            CASE
              WHEN stage_norm IN ('waiting', 'screening') AND f.has_vendor_msg THEN 3
              WHEN stage_norm = 'waiting' THEN 1
              WHEN stage_norm = 'screening' THEN 2
              WHEN stage_norm IN ('em atendimento', 'andamento') THEN 3
              WHEN stage_norm = 'active' THEN 4
              WHEN stage_norm IN ('atualizados', 'atualizacao', 'tarefas') THEN 4
              WHEN stage_norm IN ('cadastro') THEN 5
              WHEN stage_norm IN ('contato feito', '1a chamada') THEN 6
              WHEN lower(BTRIM(COALESCE(f.stage_raw, ''))) LIKE '1%%chamada' THEN 6
              WHEN stage_norm IN ('contato feito 2', '2a chamada') THEN 7
              WHEN lower(BTRIM(COALESCE(f.stage_raw, ''))) LIKE '2%%chamada' THEN 7
              WHEN stage_norm IN ('3a chamada') THEN 8
              WHEN lower(BTRIM(COALESCE(f.stage_raw, ''))) LIKE '3%%chamada' THEN 8
              WHEN stage_norm IN ('proposta enviada') THEN 9
              WHEN stage_norm IN ('pos-vendas', 'pos vendas', 'posvendas', 'pos-venda', 'pos venda', 'recompra',
                                  'aguardando entrega', 'aguardando coleta') THEN 10
              WHEN stage_norm IN ('finished', 'closed', 'finalizado', 'pre-finish') THEN 11
              WHEN stage_norm IN ('lixo', 'leads mortos') THEN 12
              WHEN stage_norm = '' THEN
                CASE
                  WHEN status_norm = 'active' THEN 4
                  WHEN status_norm = 'waiting' THEN 1
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
              ) AS status_norm,
              COALESCE(cmf.is_template_only, false) AS is_template_only,
              COALESCE(cmf.has_vendor_msg, false) AS has_vendor_msg
            FROM filtered f
            LEFT JOIN chat_msg_flags cmf ON cmf.chat_id = f.chat_id
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
          c.updated_ts,
          c.is_template_only
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
            key: {"key": key, "label": label, "total": 0, "template_only_count": 0, "clients": []}
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
            if row.get("is_template_only"):
                bucket["template_only_count"] += 1
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
            where_clauses.append("c.current_funnel_stage = 'screening'")
        elif status == "Aguardando":
            where_clauses.append("c.current_funnel_stage = 'waiting'")
        elif status == "Em atendimento":
            where_clauses.append("c.current_funnel_stage = 'active'")
        elif status == "Finalizado":
            where_clauses.append("c.current_funnel_stage IN ('finished', 'closed')")
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
    base_sql = """
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
        sc.contact_phone,
        sc.current_stage,
        sc.product,
        sc.order_value,
        sc.loss_reason
      FROM smclick_chat sc
      WHERE sc.chat_id IS NOT NULL
    """
    filtered_base_sql = f"""
      SELECT DISTINCT ON (COALESCE(NULLIF(c.contact_phone, ''), c.chat_id))
        c.*
      FROM ({base_sql}) c
      {where_sql}
      ORDER BY
        COALESCE(NULLIF(c.contact_phone, ''), c.chat_id),
        CASE
          WHEN c.current_stage IS NOT NULL
               AND c.current_stage NOT IN ('waiting', 'screening', 'finished', 'closed', 'active', 'pre-finish')
               THEN 0
          WHEN c.current_funnel_stage = 'active' THEN 1
          WHEN c.current_funnel_stage = 'screening' THEN 2
          WHEN c.current_funnel_stage = 'waiting' THEN 3
          WHEN c.current_funnel_stage IN ('finished', 'closed') THEN 4
          ELSE 5
        END,
        c.updated_at DESC NULLS LAST
    """
    filtered_base_sql_no_vendor = f"""
      SELECT DISTINCT ON (COALESCE(NULLIF(c.contact_phone, ''), c.chat_id))
        c.*
      FROM ({base_sql}) c
      {where_sql_no_vendor}
      ORDER BY
        COALESCE(NULLIF(c.contact_phone, ''), c.chat_id),
        CASE
          WHEN c.current_stage IS NOT NULL
               AND c.current_stage NOT IN ('waiting', 'screening', 'finished', 'closed', 'active', 'pre-finish')
               THEN 0
          WHEN c.current_funnel_stage = 'active' THEN 1
          WHEN c.current_funnel_stage = 'screening' THEN 2
          WHEN c.current_funnel_stage = 'waiting' THEN 3
          WHEN c.current_funnel_stage IN ('finished', 'closed') THEN 4
          ELSE 5
        END,
        c.updated_at DESC NULLS LAST
    """
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
    # Bot events CTE: try pre-computed bot_transfers table, fallback to ILIKE scan
    _has_bot_transfers = False
    try:
        with connection.cursor() as _ck:
            _ck.execute("SELECT 1 FROM information_schema.tables WHERE table_name = 'bot_transfers' LIMIT 1")
            _has_bot_transfers = bool(_ck.fetchone())
    except Exception:
        pass

    if _has_bot_transfers:
        _bot_events_cte = """
            bot_events AS (
                SELECT bt.chat_id, bt.transfer_ts AS bot_transfer_ts
                FROM bot_transfers bt
                JOIN _tmp_filtered f ON f.chat_id = bt.chat_id
            )
        """
    else:
        _bot_events_cte = """
            bot_events AS (
                SELECT
                    sm.chat_id::text AS chat_id,
                    MIN(sm.event_time) AS bot_transfer_ts
                FROM smclick_message sm
                JOIN _tmp_filtered f ON f.chat_id = sm.chat_id::text
                WHERE sm.from_me = true AND sm.sent_by_name IS NULL
                  AND sm.content_text IS NOT NULL
                  AND (
                    sm.content_text ILIKE '%%atendimento ao nosso setor de vendas%%'
                    OR sm.content_text ILIKE '%%atendimento ao nosso time de vendas%%'
                    OR sm.content_text ILIKE '%%encaminhar ao nosso time de vendas%%'
                  )
                GROUP BY sm.chat_id
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
            AND f.current_funnel_stage IN ('finished', 'closed')
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
            sm.chat_id::text AS chat_id,
            MIN(sm.event_time) AS first_human_ts
          FROM smclick_message sm
          JOIN bot_events b ON b.chat_id = sm.chat_id::text
          WHERE sm.from_me = true
            AND sm.event_time > b.bot_transfer_ts
            AND (sm.sent_by_name IS NOT NULL OR (sm.content_text IS NOT NULL AND sm.content_text ~ '^\*[^*]+\*'))
          GROUP BY sm.chat_id
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

    _reclassify_case = """
            CASE
              WHEN f.current_funnel_stage IN ('screening', 'waiting')
                   AND EXISTS (
                     SELECT 1 FROM smclick_message sm
                     WHERE sm.chat_id::text = f.chat_id
                       AND sm.from_me = true
                       AND sm.sent_by_name IS NOT NULL
                       AND sm.sent_by_name != ''
                   )
              THEN 'Em atendimento'
              WHEN f.current_funnel_stage = 'screening' THEN 'Triagem'
              WHEN f.current_funnel_stage = 'waiting' THEN 'Aguardando'
              WHEN f.current_funnel_stage = 'active' THEN 'Em atendimento'
              WHEN f.current_funnel_stage IN ('finished', 'closed') THEN 'Finalizado'
              ELSE COALESCE(f.current_funnel_stage, 'Sem etapa')
            END
    """

    stage_count_query = f"""
        WITH filtered AS (
          {filtered_base_sql}
        ),
        normalized AS (
          SELECT {_reclassify_case} AS stage_name
          FROM filtered f
          WHERE f.current_funnel_stage IS NOT NULL
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
          SELECT {_reclassify_case} AS stage_name
          FROM filtered f
          WHERE f.attendant_name IS NOT NULL
        )
        SELECT stage_name, COUNT(*) AS total
        FROM normalized
        GROUP BY stage_name
        ORDER BY total DESC;
    """

    try:
        with transaction.atomic(), connection.cursor() as cursor:
            # ── Materialize filtered base into temp tables (once, not 10×) ──
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
                elif name == "em atendimento":
                    contacts_active += stage["total"]
                elif name in ("triagem", "aguardando"):
                    contacts_pending += stage["total"]
            contacts_other = max(
                contacts_total - contacts_finalized - contacts_active - contacts_pending, 0
            )

            # ── Contatos Recebidos: total + estratificação COM BOT / COM VENDEDOR ──
            contacts_interaction_query = """
                WITH vendor_msgs AS (
                  SELECT DISTINCT sm.chat_id::text AS chat_id
                  FROM smclick_message sm
                  JOIN _tmp_filtered f ON f.chat_id = sm.chat_id::text
                  WHERE sm.from_me = true
                    AND sm.sent_by_name IS NOT NULL
                ),
                template_only AS (
                  SELECT f.chat_id
                  FROM _tmp_filtered f
                  JOIN smclick_message sm ON sm.chat_id::text = f.chat_id
                  GROUP BY f.chat_id
                  HAVING COUNT(*) FILTER (WHERE sm.from_me = false) = 0
                     AND COUNT(*) FILTER (WHERE sm.from_me = true AND sm.message_type != 'template') = 0
                     AND COUNT(*) FILTER (WHERE sm.from_me = true AND sm.message_type = 'template') > 0
                )
                SELECT
                  COUNT(*) AS total,
                  COUNT(vm.chat_id) AS com_vendedor,
                  COUNT(*) - COUNT(vm.chat_id) - COUNT(tpl.chat_id) AS com_bot,
                  COUNT(tpl.chat_id) AS followup_sem_retorno
                FROM _tmp_filtered f
                LEFT JOIN vendor_msgs vm ON vm.chat_id = f.chat_id
                LEFT JOIN template_only tpl ON tpl.chat_id = f.chat_id
            """
            cursor.execute(contacts_interaction_query)
            ci_row = cursor.fetchone()
            contacts_interaction = {
                "total": ci_row[0] if ci_row else 0,
                "com_vendedor": ci_row[1] if ci_row else 0,
                "com_bot": ci_row[2] if ci_row else 0,
                "followup_sem_retorno": ci_row[3] if ci_row else 0,
            }

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
                      lower(COALESCE(f.loss_reason, '')),
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
                  SELECT sm.chat_id::text AS chat_id,
                         SUM(CASE WHEN sm.from_me THEN 1 ELSE 0 END) AS outbound_count,
                         SUM(CASE WHEN NOT sm.from_me THEN 1 ELSE 0 END) AS inbound_count
                  FROM smclick_message sm
                  JOIN filtered f ON f.chat_id = sm.chat_id::text
                  GROUP BY sm.chat_id
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
                    WHERE cl.stage_norm = 'waiting'
                       OR cl.owner_norm ~ '(waiting|em espera|aguardando)'
                  ) AS total_waiting,
                  COUNT(*) FILTER (
                    WHERE (
                        f.attendant_name IS NOT NULL
                        OR cl.owner_norm ~ '(vendas|venda|comercial)'
                    )
                      AND cl.stage_norm != 'waiting'
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
                      lower(COALESCE(f.loss_reason, '')),
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
                  SELECT sm.chat_id::text AS chat_id,
                         SUM(CASE WHEN sm.from_me THEN 1 ELSE 0 END) AS outbound_count
                  FROM smclick_message sm
                  JOIN filtered f ON f.chat_id = sm.chat_id::text
                  GROUP BY sm.chat_id
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
                      AND cl.stage_norm != 'waiting'
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
                    WHERE cl.stage_norm = 'waiting'
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

            _order_date_filter = ""
            if date_from:
                _order_date_filter += f" AND vo.created_at::date >= '{date_from}'::date"
            if date_to:
                _order_date_filter += f" AND vo.created_at::date <= '{date_to}'::date"

            if not include_tma_tme:
                vendor_summary_query = f"""
                    WITH filtered_base AS (
                      {filtered_base_sql}
                    ),
                    filtered AS (
                      SELECT fb.*,
                        translate(lower(COALESCE(fb.loss_reason, '')),
                          'áàâãäéèêëíìîïóòôõöúùûüç', 'aaaaaeeeeiiiiooooouuuuc'
                        ) AS reason_norm
                      FROM filtered_base fb
                    )
                    ,
                    checkout_chats AS (
                      SELECT DISTINCT sm.chat_id::text AS chat_id,
                        (regexp_matches(sm.content_text, 'checkout/finalizar\\?id=([0-9]+)'))[1] AS order_id
                      FROM smclick_message sm
                      JOIN filtered f ON f.chat_id = sm.chat_id::text
                      WHERE sm.from_me = true
                        AND sm.content_text ILIKE '%%vogaflex.com.br/checkout/finalizar%%'
                    ),
                    purchased_chats AS (
                      SELECT DISTINCT ck.chat_id
                      FROM checkout_chats ck
                      JOIN vogaflex_order vo ON vo.order_id = ck.order_id
                      WHERE vo.status != 'CANCELADO'
                        {_order_date_filter}
                    )
                    SELECT
                      f.attendant_name AS vendedor,
                      COUNT(*) FILTER (
                        WHERE f.current_funnel_stage NOT IN ('finished', 'closed')
                      ) AS contacts_received,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(f.current_stage, '')) = 'proposta enviada'
                      ) AS budgets_count,
                      COUNT(*) FILTER (
                        WHERE LOWER(COALESCE(f.current_stage, '')) = 'proposta enviada'
                          OR COALESCE(f.budget_value, 0) > 0
                      ) AS budgets_detected_count,
                      COALESCE(SUM(CASE WHEN f.budget_value > 0 AND f.budget_value <= 30000 THEN f.budget_value ELSE 0 END), 0) AS budgets_sum,
                      COALESCE(SUM(CASE WHEN f.budget_value > 0 AND f.budget_value <= 30000 THEN f.budget_value ELSE 0 END), 0) AS budgets_sum_detected,
                      0 AS dead_contacts,
                      0::float AS avg_duration_seconds,
                      0::float AS avg_handoff_seconds,
                      0::float AS avg_score,
                      COUNT(DISTINCT ck.chat_id) AS checkouts_count,
                      COUNT(DISTINCT pc.chat_id) AS purchases_count
                    FROM filtered f
                    LEFT JOIN checkout_chats ck ON ck.chat_id = f.chat_id
                    LEFT JOIN purchased_chats pc ON pc.chat_id = f.chat_id
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
                      lower(COALESCE(fb.loss_reason, '')),
                      'áàâãäéèêëíìîïóòôõöúùûüç',
                      'aaaaaeeeeiiiiooooouuuuc'
                    ) AS reason_norm
                  FROM filtered_base fb
                ),
                {_bot_events_cte},
                message_stats AS (
                  SELECT sm.chat_id::text AS chat_id,
                         SUM(CASE WHEN sm.from_me THEN 1 ELSE 0 END) AS outbound_count
                  FROM smclick_message sm
                  JOIN filtered f ON f.chat_id = sm.chat_id::text
                  GROUP BY sm.chat_id
                ),
                conversation_times AS (
                  SELECT
                    f.chat_id,
                    COALESCE(f.start_time, f.created_at) AS opened_ts,
                    f.end_time
                  FROM filtered f
                  WHERE COALESCE(f.start_time, f.created_at) IS NOT NULL
                    AND f.end_time IS NOT NULL
                    AND f.current_funnel_stage IN ('finished', 'closed')
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
                    sm.chat_id::text AS chat_id,
                    MIN(sm.event_time) AS first_human_ts
                  FROM smclick_message sm
                  JOIN bot_events b ON b.chat_id = sm.chat_id::text
                  WHERE sm.from_me = true
                    AND sm.event_time > b.bot_transfer_ts
                    AND (sm.sent_by_name IS NOT NULL OR (sm.content_text IS NOT NULL AND sm.content_text ~ '^\\*[^*]+\\*'))
                  GROUP BY sm.chat_id
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
                  SELECT
                    sm.chat_id::text AS chat_id,
                    MIN(sm.event_time) AS first_ts
                  FROM smclick_message sm
                  JOIN filtered f ON f.chat_id = sm.chat_id::text
                  WHERE sm.from_me = true
                    AND (sm.sent_by_name IS NOT NULL OR (sm.content_text IS NOT NULL AND sm.content_text ~ '^\\*[^*]+\\*'))
                  GROUP BY sm.chat_id
                ),
                last_msg_ts AS (
                  SELECT sm.chat_id::text AS chat_id, MAX(sm.event_time) AS last_ts
                  FROM smclick_message sm
                  JOIN filtered f ON f.chat_id = sm.chat_id::text
                  GROUP BY sm.chat_id
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
                ),
                checkout_chats AS (
                  SELECT DISTINCT sm.chat_id::text AS chat_id,
                    (regexp_matches(sm.content_text, 'checkout/finalizar\\?id=([0-9]+)'))[1] AS order_id
                  FROM smclick_message sm
                  JOIN filtered f ON f.chat_id = sm.chat_id::text
                  WHERE sm.from_me = true
                    AND sm.content_text ILIKE '%%vogaflex.com.br/checkout/finalizar%%'
                ),
                purchased_chats AS (
                  SELECT DISTINCT ck.chat_id
                  FROM checkout_chats ck
                  JOIN vogaflex_order vo ON vo.order_id = ck.order_id
                  WHERE vo.status != 'CANCELADO'
                    {_order_date_filter}
                )
                SELECT
                  f.attendant_name AS vendedor,
                  COUNT(*) FILTER (
                    WHERE f.current_funnel_stage NOT IN ('finished', 'closed')
                  ) AS contacts_received,
                  COUNT(*) FILTER (
                    WHERE LOWER(COALESCE(f.current_stage, '')) = 'proposta enviada'
                  ) AS budgets_count,
                  COUNT(*) FILTER (
                    WHERE LOWER(COALESCE(f.current_stage, '')) = 'proposta enviada'
                      OR COALESCE(f.budget_value, 0) > 0
                  ) AS budgets_detected_count,
                  COALESCE(
                    SUM(CASE WHEN f.budget_value > 0 AND f.budget_value <= 30000 THEN f.budget_value ELSE 0 END),
                    0
                  ) AS budgets_sum,
                  COALESCE(
                    SUM(CASE WHEN f.budget_value > 0 AND f.budget_value <= 30000 THEN f.budget_value ELSE 0 END),
                    0
                  ) AS budgets_sum_detected,
                  COUNT(*) FILTER (WHERE COALESCE(ms.outbound_count, 0) = 0) AS dead_contacts,
                  AVG(COALESCE(bd.business_seconds, dm.duration_seconds)) AS avg_duration_seconds,
                  AVG(COALESCE(bh.business_seconds, dm.handoff_seconds)) AS avg_handoff_seconds,
                  0::numeric AS avg_score,
                  COUNT(DISTINCT ck.chat_id) AS checkouts_count,
                  COUNT(DISTINCT pc.chat_id) AS purchases_count
                FROM filtered f
                LEFT JOIN message_stats ms ON ms.chat_id = f.chat_id
                LEFT JOIN business_duration bd ON bd.chat_id = f.chat_id
                LEFT JOIN business_handoff bh ON bh.chat_id = f.chat_id
                LEFT JOIN direct_metrics dm ON dm.chat_id = f.chat_id
                LEFT JOIN checkout_chats ck ON ck.chat_id = f.chat_id
                LEFT JOIN purchased_chats pc ON pc.chat_id = f.chat_id
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
                  'Sem score' AS score,
                  COUNT(*) AS total
                FROM filtered f
                WHERE f.attendant_name IS NOT NULL
                  AND NOT ({sdr_attendant_exclude_sql})
                GROUP BY f.attendant_name
                ORDER BY f.attendant_name;
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
                    "checkouts_count": row[10] if len(row) > 10 else 0,
                    "purchases_count": row[11] if len(row) > 11 else 0,
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
                "contacts_interaction": contacts_interaction,
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

    base_sql = """
      SELECT
        sc.chat_id::text AS chat_id,
        sc.contact_name,
        sc.contact_phone,
        sc.attendant_name,
        sc.status AS current_funnel_stage,
        sc.current_stage,
        sc.chat_created_at AS start_time,
        CASE WHEN sc.status IN ('finished', 'closed') THEN sc.chat_updated_at ELSE NULL END AS end_time,
        sc.inserted_at AS created_at,
        sc.refreshed_at AS updated_at
      FROM smclick_chat sc
      WHERE sc.chat_id IS NOT NULL
    """

    query = f"""
        WITH filtered AS (
          SELECT DISTINCT ON (COALESCE(NULLIF(c.contact_phone, ''), c.chat_id))
            c.*
          FROM ({base_sql}) c
          {where_sql}
          ORDER BY
            COALESCE(NULLIF(c.contact_phone, ''), c.chat_id),
            CASE
              WHEN c.current_stage IS NOT NULL
                   AND c.current_stage NOT IN ('waiting', 'screening', 'finished', 'closed', 'active', 'pre-finish')
                   THEN 0
              WHEN c.current_funnel_stage = 'active' THEN 1
              WHEN c.current_funnel_stage = 'screening' THEN 2
              WHEN c.current_funnel_stage = 'waiting' THEN 3
              WHEN c.current_funnel_stage IN ('finished', 'closed') THEN 4
              ELSE 5
            END,
            c.updated_at DESC NULLS LAST
        ),
        message_stats AS (
          SELECT sm.chat_id::text AS chat_id,
                 SUM(CASE WHEN sm.from_me = true AND sm.sent_by_name IS NOT NULL THEN 1 ELSE 0 END) AS outbound_count
          FROM smclick_message sm
          JOIN filtered f ON f.chat_id = sm.chat_id::text
          GROUP BY sm.chat_id
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

    base_sql = """
      SELECT
        sc.chat_id::text AS chat_id, sc.contact_name, sc.contact_phone, sc.attendant_name,
        sc.status AS current_funnel_stage,
        sc.chat_created_at AS start_time,
        CASE WHEN sc.status IN ('finished', 'closed') THEN sc.chat_updated_at ELSE NULL END AS end_time,
        sc.inserted_at AS created_at, sc.refreshed_at AS updated_at,
        sc.budget_value,
        sc.current_stage
      FROM smclick_chat sc WHERE sc.chat_id IS NOT NULL
    """

    closed_stages = "('finished','closed')"
    query = f"""
      WITH filtered AS (
        SELECT DISTINCT ON (COALESCE(NULLIF(c.contact_phone, ''), c.chat_id))
          c.*
        FROM ({base_sql}) c
        {where_sql}
        ORDER BY
          COALESCE(NULLIF(c.contact_phone, ''), c.chat_id),
          CASE
            WHEN c.current_stage IS NOT NULL
                 AND c.current_stage NOT IN ('waiting', 'screening', 'finished', 'closed', 'active', 'pre-finish')
                 THEN 0
            WHEN c.current_funnel_stage = 'active' THEN 1
            WHEN c.current_funnel_stage = 'screening' THEN 2
            WHEN c.current_funnel_stage = 'waiting' THEN 3
            WHEN c.current_funnel_stage IN ('finished', 'closed') THEN 4
            ELSE 5
          END,
          c.updated_at DESC NULLS LAST
      ),
      -- Handoff: first human vendor message per chat (sent_by_name populated OR *Name* pattern)
      handoff_ts AS (
        SELECT chat_id::text AS chat_id, MIN(event_time) AS ts
        FROM smclick_message
        WHERE from_me = true
          AND (
            sent_by_name IS NOT NULL
            OR (content_text IS NOT NULL AND content_text ~ '^\\*[^*]+\\*')
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
          f.chat_id, f.contact_name AS cliente_nome, f.contact_phone AS cliente_telefone,
          f.attendant_name AS vendedor_nome,
          ROUND(EXTRACT(EPOCH FROM (NOW() - lm.event_time)) / 86400)::int AS extra_int
        FROM filtered f
        JOIN last_msg lm ON lm.chat_id = f.chat_id
        WHERE lm.from_me = false
          AND f.current_funnel_stage NOT IN {closed_stages}
          AND lm.event_time < NOW() - INTERVAL '48 hours'
      ),
      a_aguardando AS (
        SELECT f.chat_id, f.contact_name AS cliente_nome, f.contact_phone AS cliente_telefone,
               f.attendant_name AS vendedor_nome, lm.event_time AS desde,
               CASE WHEN lm.event_time < NOW() - INTERVAL '24 hours' THEN true ELSE false END AS over_24h
        FROM filtered f
        JOIN last_msg lm ON lm.chat_id = f.chat_id
        WHERE lm.from_me = false
          AND f.current_funnel_stage NOT IN {closed_stages}
          AND lm.event_time >= NOW() - INTERVAL '48 hours'
      ),
      a_midia_sem_info AS (
        SELECT f.chat_id, f.contact_name AS cliente_nome, f.contact_phone AS cliente_telefone,
               f.attendant_name AS vendedor_nome, lm.media_ts
        FROM filtered f
        JOIN last_vendor_media lm ON lm.chat_id = f.chat_id
        WHERE f.chat_id NOT IN (SELECT chat_id FROM post_media_text)
          AND f.current_funnel_stage NOT IN {closed_stages}
      ),
      last_outbound AS (
        SELECT sm.chat_id::text AS chat_id, MAX(sm.event_time) AS ts
        FROM smclick_message sm
        WHERE sm.from_me = true
        GROUP BY sm.chat_id
      ),
      a_orcamento_sem_followup AS (
        SELECT f.chat_id, f.contact_name AS cliente_nome, f.contact_phone AS cliente_telefone,
               f.attendant_name AS vendedor_nome,
               ROUND(EXTRACT(EPOCH FROM (NOW() - COALESCE(lo.ts, f.created_at))) / 86400)::int AS extra_int
        FROM filtered f
        LEFT JOIN last_outbound lo ON lo.chat_id = f.chat_id
        WHERE LOWER(COALESCE(f.current_stage, '')) = 'proposta enviada'
          AND f.current_funnel_stage NOT IN {closed_stages}
          AND COALESCE(lo.ts, f.created_at) < NOW() - INTERVAL '2 days'
      )

      SELECT * FROM (
        SELECT 'sem_retorno_2d' AS alert_type, chat_id, cliente_nome, cliente_telefone, vendedor_nome,
               extra_int::text AS extra
        FROM a_sem_retorno ORDER BY extra_int DESC
      ) _r1
      UNION ALL
      SELECT * FROM (
        SELECT 'aguardando_24_48h', chat_id, cliente_nome, cliente_telefone, vendedor_nome,
               TO_CHAR(desde AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI')
        FROM a_aguardando WHERE over_24h = true ORDER BY desde ASC
      ) _r2a
      UNION ALL
      SELECT * FROM (
        SELECT 'aguardando_resposta', chat_id, cliente_nome, cliente_telefone, vendedor_nome,
               TO_CHAR(desde AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI')
        FROM a_aguardando WHERE over_24h = false ORDER BY desde ASC
      ) _r2
      UNION ALL
      SELECT * FROM (
        SELECT 'midia_sem_info', chat_id, cliente_nome, cliente_telefone, vendedor_nome,
               TO_CHAR(media_ts AT TIME ZONE 'America/Sao_Paulo', 'DD/MM HH24:MI')
        FROM a_midia_sem_info ORDER BY media_ts DESC
      ) _r3
      UNION ALL
      SELECT * FROM (
        SELECT 'orcamento_sem_followup', chat_id, cliente_nome, cliente_telefone, vendedor_nome,
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
            "aguardando_24_48h": [],
            "aguardando_resposta": [],
            "midia_sem_info": [],
            "orcamento_sem_followup": [],
        }
        for alert_type, chat_id, cliente_nome, cliente_telefone, vendedor_nome, extra in rows:
            result[alert_type].append({
                "chat_id": chat_id,
                "cliente_nome": cliente_nome,
                "cliente_telefone": cliente_telefone,
                "vendedor_nome": vendedor_nome,
                "extra": extra,
            })
        return JsonResponse(result)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


@csrf_exempt
def smclick_debug(request):
    """Temporary diagnostic endpoint to check data flow."""
    try:
        with connection.cursor() as cur:
            checks = {}

            def _safe(sql, label):
                try:
                    cur.execute(sql)
                    return cur.fetchone()
                except Exception as e:
                    checks[f"{label}_error"] = str(e)
                    return None

            # Buffer stats
            r = _safe(
                "SELECT COUNT(*), MAX(received_at), "
                "COUNT(*) FILTER (WHERE processed_at IS NULL) "
                "FROM smclick_ingest_buffer",
                "ingest_buffer",
            )
            if r:
                checks["ingest_buffer"] = {
                    "total": r[0],
                    "last_received": str(r[1]) if r[1] else None,
                    "pending": r[2],
                }

            # Event log
            r = _safe(
                "SELECT COUNT(*), MAX(received_at), "
                "COUNT(*) FILTER (WHERE applied_at IS NULL) "
                "FROM smclick_event_log",
                "event_log",
            )
            if r:
                checks["event_log"] = {
                    "total": r[0],
                    "last_received": str(r[1]) if r[1] else None,
                    "pending": r[2],
                }

            # Chats
            r = _safe(
                "SELECT COUNT(*), MAX(refreshed_at), MAX(last_event_at), "
                "COUNT(*) FILTER (WHERE refreshed_at >= NOW() - INTERVAL '24 hours') "
                "FROM smclick_chat",
                "smclick_chat",
            )
            if r:
                checks["smclick_chat"] = {
                    "total": r[0],
                    "last_refreshed": str(r[1]) if r[1] else None,
                    "last_event": str(r[2]) if r[2] else None,
                    "updated_24h": r[3],
                }

            # Messages
            r = _safe(
                "SELECT COUNT(*), MAX(event_time), MAX(last_seen_at), "
                "COUNT(*) FILTER (WHERE last_seen_at >= NOW() - INTERVAL '24 hours') "
                "FROM smclick_message",
                "smclick_message",
            )
            if r:
                checks["smclick_message"] = {
                    "total": r[0],
                    "last_event_time": str(r[1]) if r[1] else None,
                    "last_seen": str(r[2]) if r[2] else None,
                    "updated_24h": r[3],
                }

            # Pending event_log details
            r = _safe(
                "SELECT COUNT(*), MIN(received_at), MAX(received_at), "
                "COUNT(*) FILTER (WHERE chat_id IS NULL), "
                "COUNT(*) FILTER (WHERE payload IS NULL) "
                "FROM smclick_event_log WHERE applied_at IS NULL",
                "pending_details",
            )
            if r:
                checks["pending_details"] = {
                    "count": r[0],
                    "oldest": str(r[1]) if r[1] else None,
                    "newest": str(r[2]) if r[2] else None,
                    "no_chat_id": r[3],
                    "no_payload": r[4],
                }

            checks["server_now"] = str(cur.execute("SELECT NOW()") or cur.fetchone()[0])

        return JsonResponse(checks)
    except Exception as exc:
        return JsonResponse({"error": str(exc)}, status=500)


@csrf_exempt
def smclick_force_sync(request):
    """Trigger sync_smclick manually."""
    from django.core.management import call_command
    import io
    out = io.StringIO()
    try:
        call_command("sync_smclick", stdout=out)
        return JsonResponse({"ok": True, "output": out.getvalue()})
    except Exception as exc:
        return JsonResponse({"ok": False, "error": str(exc), "output": out.getvalue()}, status=500)


@csrf_exempt
def smclick_backfill(request):
    """Trigger backfill_smclick + sync_smclick."""
    from django.core.management import call_command
    import io
    date_from = request.GET.get("date_from")
    date_to = request.GET.get("date_to")
    skip_messages = request.GET.get("skip_messages", "0") == "1"
    out = io.StringIO()
    try:
        kwargs = {"stdout": out}
        if date_from:
            kwargs["date_from"] = date_from
        if date_to:
            kwargs["date_to"] = date_to
        if skip_messages:
            kwargs["skip_messages"] = True
        call_command("backfill_smclick", **kwargs)
        # Auto-run sync after backfill
        call_command("sync_smclick", stdout=out)
        return JsonResponse({"ok": True, "output": out.getvalue()})
    except Exception as exc:
        return JsonResponse({"ok": False, "error": str(exc), "output": out.getvalue()}, status=500)


@csrf_exempt
@require_POST
def smclick_webhook(request):
    """Direct webhook receiver for SmClick events.

    Replaces the n8n Webhook Buffer flow:
    1. Receives raw SmClick POST payload
    2. Deduplicates via sha256 hash
    3. Inserts into smclick_ingest_buffer
    4. Returns 200 immediately

    The sync_smclick management command (cron) processes the buffer.
    """
    try:
        body = request.body
        if not body:
            return JsonResponse({"ok": False, "error": "empty body"}, status=400)

        payload_text = body.decode("utf-8")
        payload_hash = hashlib.sha256(payload_text.encode("utf-8")).hexdigest()

        # Extract metadata from the JSON for indexed columns
        try:
            data = json.loads(payload_text)
        except (json.JSONDecodeError, ValueError):
            return JsonResponse({"ok": False, "error": "invalid json"}, status=400)

        event_name = data.get("event") or None
        event_time = data.get("event_time") or None
        infos = data.get("infos") or {}
        chat = infos.get("chat") or {}
        message = infos.get("message") or {}
        chat_id = chat.get("id") or None
        message_id = message.get("id") or None

        with connection.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO smclick_ingest_buffer (
                    payload_hash, payload, event_name, event_time,
                    chat_id, message_id
                ) VALUES (
                    %s, %s::jsonb, %s,
                    %s::timestamptz, %s::uuid, %s::uuid
                )
                ON CONFLICT (payload_hash) DO NOTHING
                RETURNING id
                """,
                [payload_hash, payload_text, event_name,
                 event_time, chat_id, message_id],
            )
            row = cursor.fetchone()

        return JsonResponse({
            "ok": True,
            "buffered": row is not None,
            "event": event_name,
            "chat_id": str(chat_id) if chat_id else None,
            "message_id": str(message_id) if message_id else None,
        })
    except Exception as exc:
        return JsonResponse({"ok": False, "error": str(exc)}, status=500)
