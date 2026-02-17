-- Rollback de estrutura criada nas mudancas de 2026-02-17
-- Objetivo: voltar para o estado anterior (sem chat_unified e sem ingest buffer),
-- preservando backup rapido dos dados antes de remover objetos.
--
-- Execute no DBeaver conectado ao banco correto.
-- Recomendado: rodar em janela de manutencao.

BEGIN;

-- 0) Sanidade: mostra banco/schema atual
SELECT current_database() AS db, current_schema() AS schema, current_user AS usr;

-- 1) Backup rapido (somente se as tabelas existirem)
DO $$
DECLARE
  ts TEXT := to_char(now(), 'YYYYMMDD_HH24MISS');
BEGIN
  IF to_regclass('public.chat_unified') IS NOT NULL THEN
    EXECUTE format(
      'CREATE TABLE public.chat_unified_bkp_%s AS TABLE public.chat_unified',
      ts
    );
  END IF;

  IF to_regclass('public.smclick_ingest_buffer') IS NOT NULL THEN
    EXECUTE format(
      'CREATE TABLE public.smclick_ingest_buffer_bkp_%s AS TABLE public.smclick_ingest_buffer',
      ts
    );
  END IF;
END $$;

-- 2) Remove objetos criados na tentativa de unificacao/ingest
DROP TABLE IF EXISTS public.chat_unified CASCADE;
DROP TABLE IF EXISTS public.smclick_ingest_buffer CASCADE;

-- 3) Verificacao final de existencia das tabelas base (originais do app)
-- Ajuste esta lista se necessario.
SELECT
  to_regclass('public.conversations')         AS conversations,
  to_regclass('public.messages')              AS messages,
  to_regclass('public.semclick_conversations') AS semclick_conversations,
  to_regclass('public.semclick_messages')     AS semclick_messages,
  to_regclass('public.smclick_raw_events')    AS smclick_raw_events;

COMMIT;

-- 4) Pos-rollback: contagens para conferir rapidamente
SELECT 'conversations' AS tabela, COUNT(*) AS total FROM public.conversations
UNION ALL
SELECT 'messages', COUNT(*) FROM public.messages
UNION ALL
SELECT 'semclick_conversations', COUNT(*) FROM public.semclick_conversations
UNION ALL
SELECT 'semclick_messages', COUNT(*) FROM public.semclick_messages
UNION ALL
SELECT 'smclick_raw_events', COUNT(*) FROM public.smclick_raw_events
ORDER BY 1;
