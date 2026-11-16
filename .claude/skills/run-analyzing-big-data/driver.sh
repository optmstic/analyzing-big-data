#!/usr/bin/env bash
# Driver for the project notebook: sets up a local PySpark + Delta Lake environment and runs every cell of
# notebooks/ABD_Project_Portugal_Air_Connectivity.py through local_test/run_local.py.
# Usage (from the repository root):
#   .claude/skills/run-analyzing-big-data/driver.sh setup          # create the virtualenv (once)
#   .claude/skills/run-analyzing-big-data/driver.sh classic [dir]  # run on classic Spark
#   .claude/skills/run-analyzing-big-data/driver.sh connect [dir]  # run through Spark Connect (as Databricks serverless)
#   .claude/skills/run-analyzing-big-data/driver.sh all            # both modes
# Output: full log in <dir>/run.log, charts as PNG files in <dir>/out/figure_*.png
set -euo pipefail
VENV="${ABD_VENV:-$HOME/.venvs/abd}"
ROOT="$(cd "$(dirname "$0")/../../.." && pwd)"
PY="$VENV/bin/python"

setup() {
  command -v java >/dev/null || { echo "Java 17+ is required (apt-get install -y openjdk-21-jre-headless)"; exit 1; }
  python3 -m venv "$VENV"
  # The system setuptools is too old to build pyspark from source: upgrade the build tools first
  "$VENV/bin/pip" install -q -U pip setuptools wheel
  "$VENV/bin/pip" install -q "pyspark[connect]==4.0.1" delta-spark==4.0.0 pandas pyarrow matplotlib
  "$PY" -c "import pyspark; print('pyspark', pyspark.__version__)"
}

run() {  # $1 = classic|connect, $2 = work dir
  local work="${2:-/tmp/abd_local_$1}"
  [ -x "$PY" ] || { echo "run '$0 setup' first"; exit 1; }
  if [ "$1" = connect ]; then export ABD_CONNECT=1; else unset ABD_CONNECT; fi
  mkdir -p "$work"
  ( cd "$ROOT" && "$PY" local_test/run_local.py "$work/out" > "$work/run.log" 2>&1 ) \
    || { echo "FAILED, last lines of $work/run.log:"; grep -vE "WARN|Stage" "$work/run.log" | tail -25; exit 1; }
  echo "== $1: $work/run.log, figures in $work/out/"
  grep -E "^Spark [0-9]|^All quality checks passed|^Streaming result identical|LOCAL TEST PASSED" "$work/run.log"
}

case "${1:-all}" in
  setup) setup ;;
  classic) run classic "${2:-}" ;;
  connect) run connect "${2:-}" ;;
  all) run classic; run connect ;;
  *) sed -n '2,10p' "$0"; exit 2 ;;
esac
