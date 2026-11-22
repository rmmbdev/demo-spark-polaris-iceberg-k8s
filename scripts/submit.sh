#!/usr/bin/env bash
# Submit one run of a job as a SparkApplication and wait for the result.
# This is what an orchestrator (Airflow) does for every scheduled run:
# render the template with a unique run name and an image tag, create it,
# poll its state, and fail the task if the job failed.
#
#   scripts/submit.sh <job> [job args...]
#   scripts/submit.sh generate-quotes --rows 2000000 --days 7
#   scripts/submit.sh quotes-daily --day 2026-08-02
#   WAIT=0 scripts/submit.sh maintenance        # fire and forget
#
# Env: TAG = image tag you built (default 0.1), WAIT (1), TIMEOUT seconds (1800).
set -euo pipefail
cd "$(dirname "$0")/.."

JOB="${1:?usage: scripts/submit.sh <job> [args...]}"; shift
TEMPLATE="k8s/spark-apps/$JOB.yaml"
[ -f "$TEMPLATE" ] || { echo "no template: $TEMPLATE" >&2; exit 2; }

TAG="${TAG:-0.1}"
export IMAGE="spark-jobs:$TAG"
export RUN_NAME="$JOB-$(date -u +%Y%m%d-%H%M%S)"
DRIVER_ARGS=$(python3 -c 'import json, sys; print(json.dumps(sys.argv[1:]))' "$@")
export DRIVER_ARGS

envsubst '${RUN_NAME} ${IMAGE} ${DRIVER_ARGS}' < "$TEMPLATE" | kubectl create -f -
echo "submitted $RUN_NAME with image $IMAGE"
[ "${WAIT:-1}" = "1" ] || exit 0

state() {
  kubectl get sparkapp "$RUN_NAME" \
    -o jsonpath='{.status.currentState.currentStateSummary}' 2>/dev/null || true
}
succeeded() {
  # Did any attempt reach Succeeded? (A retried run may fail first.)
  kubectl get sparkapp "$RUN_NAME" -o json | python3 -c '
import json, sys
h = json.load(sys.stdin).get("status", {}).get("stateTransitionHistory", {})
sys.exit(0 if any(v.get("currentStateSummary") == "Succeeded" for v in h.values()) else 1)'
}

deadline=$(( $(date +%s) + ${TIMEOUT:-1800} ))
last="" ; stuck=0
while :; do
  s=$(state)
  if [ "$s" != "$last" ]; then echo "$(date +%T)  $s"; last=$s; stuck=0; else stuck=$((stuck + 5)); fi
  case "$s" in
    Succeeded|ResourceReleased|TerminatedWithoutReleaseResources) break ;;
    # A failure state that doesn't move for 2 min: no restart is coming.
    Failed|SchedulingFailure|DriverStartTimedOut|DriverReadyTimedOut|ExecutorsStartTimedOut|DriverEvicted)
      [ "$stuck" -lt 120 ] || break ;;
  esac
  [ "$(date +%s)" -lt "$deadline" ] || { echo "timed out waiting for $RUN_NAME" >&2; exit 1; }
  sleep 5
done

if succeeded; then
  echo "$RUN_NAME: SUCCEEDED"
else
  echo "$RUN_NAME: FAILED  (kubectl logs $RUN_NAME-0-driver; kubectl describe sparkapp $RUN_NAME)" >&2
  exit 1
fi
