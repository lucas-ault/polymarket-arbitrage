#!/usr/bin/env bash
set -euo pipefail

REPO_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$REPO_DIR"

echo "==> Repo: $REPO_DIR"

# Pull only when tree is clean (avoid clobbering local config changes).
if git diff --quiet && git diff --cached --quiet; then
  echo "==> Pulling latest changes from origin/main..."
  git pull --ff-only origin main
else
  echo "==> Working tree has local changes; skipping git pull."
  echo "    (Commit/stash manually if you want to update code.)"
fi

# Ensure virtual environment exists.
if [[ ! -d ".venv" ]]; then
  echo "==> Creating virtual environment..."
  python3 -m venv .venv
fi

echo "==> Activating virtual environment..."
source .venv/bin/activate

echo "==> Installing dependencies..."
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "==> Starting bot + dashboard..."
exec python run_with_dashboard.py --config config.observation.yaml --host 0.0.0.0 --port 8888
