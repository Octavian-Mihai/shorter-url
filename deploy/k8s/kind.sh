#!/usr/bin/env bash
# Local Kubernetes demo on kind.
#   ./deploy/k8s/kind.sh up      create cluster, build+load image, deploy everything
#   ./deploy/k8s/kind.sh smoke   run scripts/smoke.sh against the cluster
#   ./deploy/k8s/kind.sh rolling rolling-restart the API under traffic, expect 0 failures
#   ./deploy/k8s/kind.sh down    delete the cluster
set -euo pipefail
cd "$(dirname "$0")/../.."
CLUSTER=shortener
NS=shortener
KCTL="kubectl --context kind-$CLUSTER"

up() {
  kind get clusters 2>/dev/null | grep -qx "$CLUSTER" || kind create cluster --config deploy/k8s/kind-config.yaml
  docker build -q -f deploy/Dockerfile -t shortener:dev .
  kind load docker-image shortener:dev --name "$CLUSTER"

  $KCTL apply -f deploy/k8s/base/namespace.yaml
  # Single source of truth: build the ConfigMaps from the repo's own files.
  $KCTL -n $NS create configmap migrations --from-file=migrations/ --dry-run=client -o yaml | $KCTL apply -f -
  $KCTL -n $NS create configmap prometheus-config \
    --from-file=prometheus.yml=deploy/k8s/prometheus-k8s.yml \
    --from-file=alerts.yml=deploy/prometheus/alerts.yml --dry-run=client -o yaml | $KCTL apply -f -
  $KCTL -n $NS create configmap grafana-dashboards \
    --from-file=deploy/grafana/dashboards/ --dry-run=client -o yaml | $KCTL apply -f -

  # metrics-server feeds the HPA. kind's kubelets use self-signed certs, hence --kubelet-insecure-tls.
  $KCTL apply -f https://github.com/kubernetes-sigs/metrics-server/releases/download/v0.7.2/components.yaml >/dev/null
  $KCTL -n kube-system patch deploy metrics-server --type=json \
    -p '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--kubelet-insecure-tls"}]' >/dev/null 2>&1 || true

  $KCTL apply -k deploy/k8s/base
  # Roll the pods if the image was rebuilt under the same tag.
  $KCTL -n $NS rollout restart deploy/api deploy/consumer >/dev/null

  for r in statefulset/postgres statefulset/kafka deploy/redis deploy/api deploy/consumer deploy/prometheus deploy/grafana; do
    $KCTL -n $NS rollout status "$r" --timeout=300s
  done
  echo
  echo "API:     http://localhost:8081   (docs: /docs)"
  echo "Grafana: http://localhost:3300"
  echo "Try:     ./deploy/k8s/kind.sh smoke"
}

# Rolling-restart the API while a client hammers it; any non-302 is a failure.
# Demonstrates maxUnavailable=0 + readiness probes + the preStop drain delay.
rolling() {
  local slug fails=0 total=0 code
  slug=$(curl -fsS -X POST http://localhost:8081/v1/links -H 'X-API-Key: dev-key-change-me' \
    -H 'Content-Type: application/json' -d '{"url":"https://example.com/rolling"}' | sed -n 's/.*"slug":"\([^"]*\)".*/\1/p')
  $KCTL -n $NS rollout restart deploy/api >/dev/null
  $KCTL -n $NS rollout status deploy/api --timeout=180s >/dev/null 2>&1 &
  local watcher=$!
  while kill -0 "$watcher" 2>/dev/null; do
    code=$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "http://localhost:8081/$slug"); rc=$?
    total=$((total+1)); [ "$code" = 302 ] || { fails=$((fails+1)); echo "$(date +%T) non-302: http=$code curl_exit=$rc"; }
    sleep 0.02
  done
  echo "rolling restart: $total requests, $fails failures"
  [ "$fails" = 0 ] && [ "$total" -gt 50 ]
}

smoke() { BASE=http://localhost:8081 ./scripts/smoke.sh; }
down()  { kind delete cluster --name "$CLUSTER"; }

"${1:-up}"
