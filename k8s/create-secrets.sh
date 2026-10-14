#!/usr/bin/env bash
# The platform's admin / service secrets for a TEAM stack — random values, created once.
# Nothing here is published: the platform manager reads a value when needed, e.g.
#   kubectl -n de-stack get secret de-stack-secrets -o jsonpath='{.data.polaris-root-secret}' | base64 -d
# Idempotent: existing keys are never changed; keys added in a newer version of this script are
# added to an existing Secret (so re-running never rotates anything).
#   NAMESPACE=de-stack KUBECTL="kubectl" bash k8s/create-secrets.sh
set -euo pipefail
NAMESPACE="${NAMESPACE:-de-stack}"
KUBECTL="${KUBECTL:-kubectl}"
NAME="${SECRET_NAME:-de-stack-secrets}"

value() {                       # no `| head` anywhere: it would trip pipefail
  case "$1" in
    airflow-fernet-key)   openssl rand -base64 32 | tr '+/' '-_' ;;   # Airflow Fernet key format
    jupyterhub-crypt-key) openssl rand -hex 32 ;;                     # 64 hex chars
    vault-unseal-key)     openssl rand -hex 32 ;;                     # 64 hex chars (OpenBao static seal)
    *)                    openssl rand -hex 16 ;;                     # 32 chars (also an AES-256 key)
  esac
}
KEYS="postgres-password pg-airflow-password pg-polaris-password pg-keycloak-password pg-superset-password
  polaris-root-secret polaris-persona-secret trino-lab-secret storage-root-password
  keycloak-admin-password manager-password sqlpad-admin-password superset-admin-password airflow-admin-password
  oidc-home-secret oidc-jupyterhub-secret oidc-airflow-secret oidc-superset-secret oidc-sqlpad-secret
  oidc-storage-secret oidc-polaris-secret oauth2-cookie-secret airflow-fernet-key airflow-secret-key
  superset-secret-key jupyterhub-crypt-key hub-admin-token pg-vault-password vault-unseal-key vault-admin-token"

if existing=$($KUBECTL -n "$NAMESPACE" get secret "$NAME" -o jsonpath='{.data}' 2>/dev/null) && [ -n "$existing" ]; then
  added=0
  for key in $KEYS; do
    case "$existing" in *"\"$key\""*) continue ;; esac
    $KUBECTL -n "$NAMESPACE" patch secret "$NAME" --type merge -p "{\"stringData\":{\"$key\":\"$(value "$key")\"}}" >/dev/null
    added=$((added + 1))
  done
  echo "secret $NAME exists — $added new key(s) added, existing ones left as is"; exit 0
fi
args=()
for key in $KEYS; do args+=("--from-literal=$key=$(value "$key")"); done
$KUBECTL -n "$NAMESPACE" create secret generic "$NAME" "${args[@]}" >/dev/null
echo "secret $NAME created ($(echo $KEYS | wc -w | tr -d ' ') random values)"
