#!/usr/bin/env bash
# Paper-only release flow for the shared Contabo VM.
#
# Every command runs in /opt/solanadvij against the "solanadvij" Compose
# project only. It never lists, stops, or prunes anything outside that project.
# Any failed gate leaves the bot stopped and prints the blocker.
#
#   scripts/vm_release.sh preflight SHA        exact checkout, .env, NTP, CI, compose
#   scripts/vm_release.sh build SHA            image at SHA, no signer modules, IDL drift
#   scripts/vm_release.sh db                   migrate, then check the stream can start
#   scripts/vm_release.sh soak MINUTES [calibration]
#                                              record-mode Helius/Jupiter quote-only soak
#   scripts/vm_release.sh funnel [HOURS]       calibration funnel and the frozen decision
#   scripts/vm_release.sh marks                reserve vs Jupiter marks and the frozen rule
#   scripts/vm_release.sh sensitivity SINCE    re-price closed trades (delay, bps, size, fees)
#   scripts/vm_release.sh freeze START [GAP]   protocol for START..+15d..+30d
#   scripts/vm_release.sh start                paper bot, readiness, monitor
#   scripts/vm_release.sh restart-drill PROTOCOL
#                                              restart before the window, measure the gap
#   scripts/vm_release.sh status [PROTOCOL]    readiness, recent gate, sample progress
#   scripts/vm_release.sh stop                 stop the bot and its monitor
set -euo pipefail

PROJECT_DIR=/opt/solanadvij
PROJECT=solanadvij
API=http://127.0.0.1:8080
ARTIFACTS=artifacts
COHORT_CONFIG=configs/default.yaml
CALIBRATION_CONFIG=configs/calibration.yaml
TELEGRAM_OFF='{"enabled":false,"daily_report_time":"00:00","include_all_time_with_daily":false}'

cd "$PROJECT_DIR"

compose() { docker compose -p "$PROJECT" --env-file .env "$@"; }
env_value() { sed -n "s/^$1=//p" .env | tail -n 1; }
blocked() {
  printf 'BLOCKED: %s\n' "$*" >&2
  exit 1
}
warn() { printf 'WARNING: %s\n' "$*" >&2; }
in_image() { compose run --rm --no-deps -T --entrypoint python sniper-bot "$@"; }
cohort_config_in_env() {
  local configured
  configured="$(env_value CONFIG_PATH)"
  [[ -z "$configured" || "$configured" == "$COHORT_CONFIG" ]] ||
    blocked "CONFIG_PATH in .env must be $COHORT_CONFIG for the statistical cohort"
}
stop_bot() { compose stop monitor sniper-bot >/dev/null 2>&1 || true; }

wait_ready() {
  local deadline=$((SECONDS + $1))
  while ((SECONDS < deadline)); do
    if curl -fsS "$API/health/ready" >/dev/null 2>&1; then
      return 0
    fi
    sleep 5
  done
  return 1
}

require_sha() {
  [[ "${1:-}" =~ ^[0-9a-f]{40}$ ]] || blocked "a 40-character release SHA is required"
}

ci_green() {
  # The release SHA must have passed every quality gate. Without a token the
  # operator confirms it and passes CI_VERIFIED_SHA explicitly.
  local sha="$1"
  if [[ -n "${GITHUB_TOKEN:-}" ]]; then
    local failing
    failing="$(curl -fsS -H "Authorization: Bearer $GITHUB_TOKEN" \
      "https://api.github.com/repos/TolikB/solanadvij/commits/$sha/check-runs?per_page=100" |
      python3 -c 'import json,sys
runs=json.load(sys.stdin)["check_runs"]
names={r["name"] for r in runs if r["conclusion"]=="success"}
missing={"durability","test"}-names
print(",".join(sorted(missing)))')"
    [[ -z "$failing" ]] || blocked "CI is not green for $sha: $failing"
  else
    [[ "${CI_VERIFIED_SHA:-}" == "$sha" ]] ||
      blocked "set GITHUB_TOKEN, or CI_VERIFIED_SHA=$sha after checking quality-gates"
  fi
}

preflight() {
  local sha="$1"
  require_sha "$sha"
  [[ "$(git rev-parse HEAD)" == "$sha" ]] || blocked "checkout is $(git rev-parse HEAD), expected $sha"
  [[ -z "$(git status --porcelain --untracked-files=no)" ]] || blocked "tracked files differ from $sha"
  [[ -f .env ]] || blocked ".env is missing"
  [[ "$(stat -c %a .env)" == "600" ]] || blocked ".env must be mode 600"
  [[ "$(env_value APP_REVISION)" == "$sha" ]] || blocked "APP_REVISION in .env must be $sha"
  [[ "$(env_value APP_MODE)" == "paper" ]] || blocked "APP_MODE in .env must be paper"
  timedatectl show -p NTPSynchronized --value | grep -qx yes ||
    blocked "system clock is not NTP-synchronized"
  ci_green "$sha"
  compose config --quiet
  host_warnings
  echo "preflight=ok revision=$sha"
}

host_warnings() {
  # Read-only: host settings belong to every project on this VM, so they are
  # reported, never changed. A reboot during the OOS half longer than the
  # frozen equity gap fails the statistical gate.
  if [[ "$(systemctl is-enabled docker 2>/dev/null || true)" != "enabled" ]]; then
    warn "Docker does not start at boot; after a VM reboot the bot stays down"
  fi
  if [[ -f /var/run/reboot-required ]]; then
    warn "a reboot is pending; schedule it before the collection window, not during it"
  fi
  if apt-config dump Unattended-Upgrade::Automatic-Reboot 2>/dev/null | grep -qi '"true"'; then
    warn "unattended upgrades may reboot the VM automatically during the window"
  fi
}

build() {
  local sha="$1"
  preflight "$sha"
  compose build migrate sniper-bot
  [[ "$(compose run --rm --no-deps -T --entrypoint cat sniper-bot /app/REVISION)" == "$sha" ]] ||
    blocked "image revision does not match $sha"
  compose run --rm --no-deps -T --entrypoint python sniper-bot -c '
import importlib.util as util
blocked = ("anchorpy", "eth_account", "solana", "solders", "web3")
present = [name for name in blocked if util.find_spec(name) is not None]
assert not present, f"signer-capable modules in production image: {present}"
'
  # The audit reads the checkout's manifests, so run it on the source tree with
  # the image's interpreter rather than relying on the host Python.
  compose run --rm --no-deps -T -v "$PROJECT_DIR:/src:ro" --entrypoint python \
    sniper-bot /src/scripts/audit_no_live.py
  mkdir -p "$ARTIFACTS"
  local drift=0
  in_image scripts/check_idl_drift.py >"$ARTIFACTS/idl-drift.json" || drift=$?
  cat "$ARTIFACTS/idl-drift.json"
  case "$drift" in
    0) ;;
    1) blocked "a consumed Pump/PumpSwap event changed incompatibly upstream; update the vendored IDL" ;;
    *) blocked "upstream IDLs could not be fetched to rule out layout drift" ;;
  esac
  echo "build=ok revision=$sha paper_only=true idl=compatible"
}

db() {
  compose up -d postgres
  compose run --rm migrate
  mkdir -p "$ARTIFACTS"
  if ! compose run --rm --no-deps -T --entrypoint python sniper-bot \
    scripts/preflight_db.py | tee "$ARTIFACTS/preflight-db.json"; then
    blocked "existing events keep the stream disabled at startup; see $ARTIFACTS/preflight-db.json"
  fi
  compose up -d --no-deps backup
  echo "db=ok"
}

soak() {
  local minutes="${1:-30}" profile="${2:-}"
  local sha result config="$COHORT_CONFIG"
  [[ "$minutes" =~ ^[0-9]+$ && "$minutes" -gt 0 ]] || blocked "soak length in minutes is required"
  case "$profile" in
    "") ;;
    calibration) config="$CALIBRATION_CONFIG" ;;
    *) blocked "unknown soak profile: $profile" ;;
  esac
  sha="$(env_value APP_REVISION)"
  mkdir -p "$ARTIFACTS"
  result="$ARTIFACTS/soak-record-${profile:-cohort}-$sha-$(date -u +%Y%m%dT%H%M%SZ).json"
  # Record mode fills nothing; it streams Helius and quotes Jupiter only. The
  # calibration profile runs the loosest threshold rung for the funnel.
  CONFIG_PATH="$config" APP_MODE=record TELEGRAM="$TELEGRAM_OFF" \
    compose up -d --no-deps --force-recreate sniper-bot
  if ! wait_ready 420; then
    compose logs --no-color --tail 200 sniper-bot >"$ARTIFACTS/soak-startup.log" 2>&1 || true
    stop_bot
    blocked "record-mode bot never became ready; see $ARTIFACTS/soak-startup.log"
  fi
  if ! compose exec -T sniper-bot python scripts/soak_check.py \
    --duration "$((minutes * 60))" --interval 15 --label record-soak >"$result"; then
    stop_bot
    cat "$result"
    blocked "soak gate failed; see $result"
  fi
  stop_bot
  cat "$result"
  echo "soak=ok result=$result"
}

funnel() {
  local hours="${1:-24}"
  local result
  mkdir -p "$ARTIFACTS"
  result="$ARTIFACTS/funnel-$(date -u +%Y%m%dT%H%M%SZ).json"
  if ! in_image scripts/calibration_funnel.py --hours "$hours" >"$result"; then
    cat "$result"
    blocked "the calibration funnel could not decide; see $result"
  fi
  cat "$result"
  python3 -c 'import json,sys
report=json.load(open(sys.argv[1]))
print("funnel=ok decision=%s changes=%s" % (report["decision"], json.dumps(report["config_changes"])))' "$result"
}

marks() {
  local result
  mkdir -p "$ARTIFACTS"
  result="$ARTIFACTS/marks-$(date -u +%Y%m%dT%H%M%SZ).json"
  in_image scripts/mark_divergence.py /app/data/mark_comparisons.ndjson >"$result"
  cat "$result"
  python3 -c 'import json,sys
report=json.load(open(sys.argv[1]))
print("marks=ok decision=%s pairs=%s" % (report["decision"], report["pairs"]))' "$result"
}

sensitivity() {
  local since="${1:-}" result
  [[ -n "$since" ]] || blocked "SINCE (UTC ISO timestamp) is required"
  mkdir -p "$ARTIFACTS"
  result="$ARTIFACTS/sensitivity-$(date -u +%Y%m%dT%H%M%SZ).json"
  in_image scripts/fill_sensitivity.py --since "$since" >"$result"
  cat "$result"
  echo "sensitivity=ok result=$result"
}

freeze() {
  local start="${1:-}" gap="${2:-}"
  [[ -n "$start" ]] || blocked "collection start (UTC ISO timestamp) is required"
  cohort_config_in_env
  local expected
  local gap_args=()
  if [[ -n "$gap" ]]; then
    gap_args=(--max-equity-gap "$gap")
  fi
  expected="$(compose run --rm --no-deps -T --entrypoint python sniper-bot \
    scripts/freeze_statistical_protocol.py collection-env --collection-start "$start")"
  grep -qxF "$expected" .env || blocked "put this line in .env first: $expected"
  mkdir -p "$ARTIFACTS/acceptance"
  compose run --rm --no-deps -T --entrypoint python sniper-bot \
    scripts/freeze_statistical_protocol.py freeze --collection-start "$start" "${gap_args[@]}" \
    >"$ARTIFACTS/acceptance/statistical-protocol.json"
  sha256sum "$ARTIFACTS/acceptance/statistical-protocol.json"
  echo "freeze=ok publish $ARTIFACTS/acceptance/statistical-protocol.json before $start"
}

start() {
  [[ "$(env_value APP_MODE)" == "paper" ]] || blocked "APP_MODE in .env must be paper"
  cohort_config_in_env
  compose up -d --no-deps --force-recreate sniper-bot
  if ! wait_ready 420; then
    compose logs --no-color --tail 200 sniper-bot >"$ARTIFACTS/start.log" 2>&1 || true
    stop_bot
    blocked "paper bot never became ready; see $ARTIFACTS/start.log"
  fi
  curl -fsS "$API/health/ready" | python3 -c 'import json,sys
state=json.load(sys.stdin)
assert state["mode"]=="paper", state
print("start=ok mode=%s status=%s strategy_version=%s config_hash=%s"
      % (state["mode"], state["status"], state["strategy_version"], state["config_hash"]))'
  compose up -d --no-deps monitor
}

latest_mark_epoch() {
  in_image scripts/latest_equity_mark.py |
    python3 -c 'import json,sys; value=json.load(sys.stdin)["epoch"]; print(0 if value is None else int(value))'
}

restart_drill() {
  local protocol="${1:-}"
  [[ -f "$protocol" ]] || blocked "the frozen protocol file is required"
  local start_at limit
  read -r start_at limit < <(python3 -c 'import json,sys
from datetime import datetime
protocol=json.load(open(sys.argv[1]))
start=datetime.fromisoformat(protocol["collection_started_at"].replace("Z","+00:00"))
print(int(start.timestamp()), protocol["maximum_equity_mark_gap_seconds"])' "$protocol")
  # The drill itself must never cost coverage: finish well before the window.
  (($(date -u +%s) + 900 < start_at)) ||
    blocked "restart drill must run at least 15 minutes before the collection window"
  curl -fsS "$API/health/ready" >/dev/null || blocked "the paper bot must be running and ready"
  local before after deadline
  before="$(latest_mark_epoch)"
  ((before > 0)) || blocked "no equity mark exists yet"
  compose stop sniper-bot
  compose up -d --no-deps sniper-bot
  deadline=$((SECONDS + limit))
  after="$before"
  while ((after <= before && SECONDS < deadline)); do
    sleep 5
    after="$(latest_mark_epoch)"
  done
  ((after > before)) || blocked "no equity mark within ${limit}s of the restart"
  wait_ready 420 || blocked "the bot did not become ready after the restart"
  local gap=$((after - before))
  ((gap * 2 <= limit)) ||
    blocked "a restart left a ${gap}s equity gap, more than half of the frozen ${limit}s"
  echo "restart_drill=ok gap=${gap}s limit=${limit}s"
}

status() {
  compose ps
  curl -fsS "$API/health/ready" || true
  echo
  compose logs --no-color --tail 1 monitor 2>/dev/null || true
  if [[ -n "${1:-}" ]]; then
    compose run --rm --no-deps -T -v "$PROJECT_DIR/$1:/tmp/protocol.json:ro" \
      --entrypoint python sniper-bot scripts/collection_progress.py --protocol /tmp/protocol.json
  fi
}

command="${1:-}"
shift || true
case "$command" in
  preflight) preflight "${1:-}" ;;
  build) build "${1:-}" ;;
  db) db ;;
  soak) soak "${1:-30}" "${2:-}" ;;
  funnel) funnel "${1:-24}" ;;
  marks) marks ;;
  sensitivity) sensitivity "${1:-}" ;;
  freeze) freeze "${1:-}" "${2:-}" ;;
  start) start ;;
  restart-drill) restart_drill "${1:-}" ;;
  status) status "${1:-}" ;;
  stop) stop_bot && echo "stopped" ;;
  *) sed -n '2,21p' "$0" && exit 2 ;;
esac
