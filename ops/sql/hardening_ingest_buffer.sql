-- Hardening para fila de ingestao SmClick
-- Objetivo: reduzir travas/conexoes em pico e evitar perda de dados.
-- Data: 2026-02-17
--
-- Como executar no DBeaver:
-- 1) Rode a secao "A" sozinha (SET LOGGED), com Auto-commit ON.
-- 2) Rode as secoes "B..E" em seguida.

-- ============================================================================
-- A) Durabilidade da fila (executar sozinho)
-- ============================================================================
-- IMPORTANTE:
-- - Se a tabela estiver UNLOGGED, pode perder dados em crash/restart.
-- - Este comando pode reescrever a tabela e segurar lock por um tempo.
-- - Rode em janela de menor trafego.
ALTER TABLE public.smclick_ingest_buffer SET LOGGED;

-- ============================================================================
-- B) Sanidade
-- ============================================================================
SELECT
  current_database() AS db,
  current_schema() AS schema,
  current_user AS usr,
  now() AS ts;

DO $$
BEGIN
  IF to_regclass('public.smclick_ingest_buffer') IS NULL THEN
    RAISE EXCEPTION 'Tabela ausente: public.smclick_ingest_buffer';
  END IF;
END $$;

-- ============================================================================
-- C) Indices para fila (claim + stale lock)
-- ============================================================================
-- Acelera claim: WHERE processed_at IS NULL AND processing_at IS NULL ORDER BY id
CREATE INDEX IF NOT EXISTS smclick_buf_pending_partial_idx
  ON public.smclick_ingest_buffer (id)
  WHERE processed_at IS NULL AND processing_at IS NULL;

-- Acelera limpeza de stale lock
CREATE INDEX IF NOT EXISTS smclick_buf_processing_partial_idx
  ON public.smclick_ingest_buffer (processing_at)
  WHERE processed_at IS NULL;

ANALYZE public.smclick_ingest_buffer;

-- ============================================================================
-- D) Retencao da fila (execute 1x ao dia via scheduler)
-- ============================================================================
-- Mantem historico curto para evitar crescimento infinito.
DELETE FROM public.smclick_ingest_buffer
WHERE processed_at IS NOT NULL
  AND processed_at < NOW() - INTERVAL '7 days';

ANALYZE public.smclick_ingest_buffer;

-- ============================================================================
-- E) Verificacao rapida
-- ============================================================================
SELECT
  COUNT(*) AS total_rows,
  COUNT(*) FILTER (WHERE processed_at IS NULL AND processing_at IS NULL) AS pending_rows,
  COUNT(*) FILTER (WHERE processed_at IS NULL AND processing_at IS NOT NULL) AS in_progress_rows,
  COUNT(*) FILTER (WHERE processed_at IS NOT NULL) AS done_rows,
  COALESCE(MAX(received_at), NOW()) AS last_received_at,
  COALESCE(MAX(processed_at), NOW()) AS last_processed_at
FROM public.smclick_ingest_buffer;

-- ============================================================================
-- F) Opcional (recomendado) - timeouts por role de n8n
-- ============================================================================
-- Troque <N8N_DB_ROLE> pelo usuario real do n8n.
-- Exemplo:
-- ALTER ROLE n8n_app SET statement_timeout = '30s';
-- ALTER ROLE n8n_app SET lock_timeout = '3s';
-- ALTER ROLE n8n_app SET idle_in_transaction_session_timeout = '15s';
--
-- ALTER ROLE <N8N_DB_ROLE> SET statement_timeout = '30s';
-- ALTER ROLE <N8N_DB_ROLE> SET lock_timeout = '3s';
-- ALTER ROLE <N8N_DB_ROLE> SET idle_in_transaction_session_timeout = '15s';

