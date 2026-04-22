#!/bin/bash
# ==============================================================================
# INDUSTRIAL REGULATORY COMPLIANCE SYNC (GOLDEN LAYER ALIGNED)
# ==============================================================================
# Orchestrates official legislative ingestion into the .workspace staging area.
# ==============================================================================

set -e

# Detect project root
SCRIPTPATH="$( cd "$(dirname "$0")" >/dev/null 2>&1 ; pwd -P )"
PROJECT_ROOT="$(dirname "$(dirname "$SCRIPTPATH")")"

echo "--------------------------------------------------------"
echo "INITIATING REGULATORY COMPLIANCE SYNC (GOLDEN LAYER)"
echo "Project Root: $PROJECT_ROOT"

# BINGO #35: Environment Integrity Check (Fail-Fast)
# We ensure the infrastructure environment is initialized before triggering ingestion.
if [[ ! -f "$PROJECT_ROOT/infrastructure-setup/pyproject.toml" ]]; then
    echo "[CRITICAL] Infrastructure environment not found at $PROJECT_ROOT/infrastructure-setup/"
    exit 1
fi

echo "--------------------------------------------------------"

# Ensure staging infrastructure exists
mkdir -p "$PROJECT_ROOT/.workspace/compliance-data/raw-xml"
mkdir -p "$PROJECT_ROOT/.workspace/compliance-data/processed"

# Inject src into PYTHONPATH for uv run context
export PYTHONPATH="$PROJECT_ROOT/infrastructure-setup/src:$PYTHONPATH"

# Target regulation (default to full sync)
TARGET_SLUG=${1:-""}

EXIT_CODE=0
if [ -n "$TARGET_SLUG" ]; then
    echo "Syncing target regulation: $TARGET_SLUG"
    if ! uv run python -m infrastructure_setup.compliance_ingestion.law_processor --slug "$TARGET_SLUG"; then
        EXIT_CODE=1
    fi
else
    echo "Syncing full regulatory registry..."
    if ! uv run python -m infrastructure_setup.compliance_ingestion.law_processor; then
        EXIT_CODE=1
    fi
fi

echo "--------------------------------------------------------"
if [ $EXIT_CODE -eq 0 ]; then
    echo "INGESTION COMPLETED: .workspace staging area updated."
else
    echo "CRITICAL FAILURE: Compliance pipeline aborted."
fi
echo "--------------------------------------------------------"

exit $EXIT_CODE
