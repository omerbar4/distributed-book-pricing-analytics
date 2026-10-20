#!/usr/bin/env bash
# End-to-end demo: ingest -> simulate observations -> PySpark job -> PostgreSQL -> report.
#
# Runs inside the pipeline container (`make demo`) or on the host (`make demo-local`).
# Environment knobs:
#   PROCESSING_DATE  date to process (default: today, UTC)
#   SUBJECTS         space-separated Open Library subjects
#   INGEST_LIMIT     works per subject (default 40)
#   SKIP_INGEST=1    reuse already-landed raw responses (offline re-runs)
set -euo pipefail

PROCESSING_DATE="${PROCESSING_DATE:-$(date -u +%F)}"
SUBJECTS="${SUBJECTS:-science_fiction fantasy history technology business}"
INGEST_LIMIT="${INGEST_LIMIT:-40}"

step() { printf '\n==> %s\n' "$*" >&2; }

if [[ "${SKIP_INGEST:-0}" == "1" ]]; then
  step "1/5 Skipping ingestion (SKIP_INGEST=1): reusing landed raw responses"
else
  step "1/5 Ingest Open Library metadata: ${SUBJECTS} (limit ${INGEST_LIMIT} each)"
  subject_args=()
  for subject in ${SUBJECTS}; do subject_args+=(--subject "${subject}"); done
  # A partial failure (e.g. one subject hit a 5xx) is reported but not fatal; the
  # generator fails clearly if nothing landed at all.
  python -m src.ingestion.fetch_books "${subject_args[@]}" \
    --limit "${INGEST_LIMIT}" --ingestion-date "${PROCESSING_DATE}" \
    || step "WARNING: some subjects failed to ingest; continuing with what landed"
fi

step "2/5 Generate SIMULATED demo price/inventory observations (seeded)"
python -m src.generators.generate_observations --processing-date "${PROCESSING_DATE}"

step "3/5 Run PySpark pipeline (bronze -> silver -> gold)"
spark-submit src/jobs/book_pricing_pipeline.py --processing-date "${PROCESSING_DATE}"

step "4/5 Load PostgreSQL (idempotent replace of ${PROCESSING_DATE})"
python -m src.loaders.postgres_loader --processing-date "${PROCESSING_DATE}"

step "5/5 Report"
python -m src.loaders.report --processing-date "${PROCESSING_DATE}"
