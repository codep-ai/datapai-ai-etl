#!/usr/bin/env bash
# sync.sh — Deploy datapai-ai-etl to EC2
#
# This repo is AI-driven ETL research / PoC. Not shipped in the platform
# product. Synced to EC2 as a backup, not as a runtime dependency.
#
# Usage:
#   bash sync.sh            Additive deploy (safe default — no --delete)
#   bash sync.sh --dry-run  Show what the additive deploy would send
#
# --delete is banned for routine deploys (see feedback_no_rsync_delete.md).

set -euo pipefail

SSH_KEY="~/.ssh/Linux-CodeCambat.pem"
SRC="${HOME}/git/datapai-ai-etl/"
DST="ec2-user@platform.datap.ai:/home/ec2-user/git/datapai-ai-etl/"

EXCLUDES=(
    --exclude .git/
    --exclude __pycache__/
    --exclude .DS_Store
)

MODE="${1:-deploy}"

case "$MODE" in
    --dry-run|-n)
        echo "=== DRY RUN (additive — no transfer, no --delete) ==="
        rsync -avzn --progress -e "ssh -i $SSH_KEY" "${EXCLUDES[@]}" "$SRC" "$DST"
        ;;
    deploy|"")
        echo "=== DEPLOY (additive) ==="
        rsync -avz --progress -e "ssh -i $SSH_KEY" "${EXCLUDES[@]}" "$SRC" "$DST"
        ;;
    *)
        echo "Usage: bash sync.sh [--dry-run]"
        exit 2
        ;;
esac
