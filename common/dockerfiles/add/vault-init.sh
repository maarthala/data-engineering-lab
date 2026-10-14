#!/bin/sh
# One-shot setup of the lab's key vault (OpenBao), run after every start; safe to re-run.
#   first start: initialise → with the one-time root token: the lab-admin policy and the admin
#                token (id = $VAULT_ADMIN_TOKEN, what home-api / the Hub / Airflow use) → root
#                token revoked
#   every start: (as lab-admin) the key/value store `secret/` and the token role `member`
# Members get a token from the Hub at each Jupyter start: role member, policy member-<name>,
# which reaches only secret/<name>/… (labvault.py).
set -e
export BAO_ADDR="${BAO_ADDR:-http://vault:8200}"
: "${VAULT_ADMIN_TOKEN:?VAULT_ADMIN_TOKEN is not set}"

for i in $(seq 1 60); do                       # status: 0 unsealed · 2 sealed · 1 unreachable
  rc=0; bao status >/dev/null 2>&1 || rc=$?
  [ "$rc" = 1 ] || break
  sleep 2
done

if bao status 2>/dev/null | grep -q "Initialized *false"; then
  echo "vault-init: first start — initialising"
  init=$(bao operator init -recovery-shares=1 -recovery-threshold=1 -format=json)
  root=$(echo "$init" | sed -n 's/.*"root_token": *"\([^"]*\)".*/\1/p')
  for i in $(seq 1 30); do bao status >/dev/null 2>&1 && break; sleep 1; done   # unsealed (static seal)
  BAO_TOKEN="$root" bao policy write lab-admin - <<'EOF'
# the lab's own services: every member's secrets, their policies and tokens — nothing else
path "secret/*"                  { capabilities = ["create", "read", "update", "delete", "list"] }
path "sys/mounts/secret"         { capabilities = ["create", "read", "update"] }
path "sys/policies/acl/member-*" { capabilities = ["create", "read", "update", "delete"] }
path "auth/token/roles/member"   { capabilities = ["create", "read", "update"] }
path "auth/token/create/member"  { capabilities = ["create", "update"] }
path "auth/token/revoke-accessor" { capabilities = ["update"] }
EOF
  BAO_TOKEN="$root" bao token create -id="$VAULT_ADMIN_TOKEN" -policy=lab-admin -no-default-policy \
    -orphan -ttl=87600h -display-name=lab-admin >/dev/null
  BAO_TOKEN="$root" bao token revoke -self >/dev/null
  echo "vault-init: admin token created, root token revoked"
fi

export BAO_TOKEN="$VAULT_ADMIN_TOKEN"
bao read sys/mounts/secret >/dev/null 2>&1 || bao secrets enable -path=secret -version=2 kv
# member tokens: only member-* policies, no parent (they outlive nothing), one week at most
bao write auth/token/roles/member allowed_policies_glob="member-*" orphan=true renewable=false \
  token_ttl=168h token_explicit_max_ttl=168h >/dev/null
echo "vault-init: ready (secret/ store, member token role)"
