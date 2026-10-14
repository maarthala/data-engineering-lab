#!/bin/sh
# One-shot setup of the lab's key vault (OpenBao), run after every start; safe to re-run.
#   first start: initialise → with the one-time root token: the lab-admin policy and the admin
#                token (id = $VAULT_ADMIN_TOKEN, what home-api / the Hub / Airflow use) → root
#                token revoked
#   every start: (as lab-admin) its own policy (so a newer one reaches an existing vault), the
#                key/value store `secret/`, the token role `member` and — with
#                VAULT_OIDC_CLIENT_SECRET — "Sign in with the lab account" (Keycloak, client vault)
# Members get a token from the Hub at each Jupyter start: role member, policy member-<name>,
# which reaches only secret/<name>/… (labvault.py). Signed in to the UI with Keycloak they get
# lab-member: the same folder, picked by their username. Managers (Keycloak group) get lab-manager.
set -e
export BAO_ADDR="${BAO_ADDR:-http://vault:8200}"
: "${VAULT_ADMIN_TOKEN:?VAULT_ADMIN_TOKEN is not set}"

# the lab's own services: every member's secrets, their policies, tokens and the sign-in — nothing
# else (it may rewrite this policy itself, so vault-init can update it on an existing vault)
admin_policy() {
  cat <<'EOF'
path "secret/*"                     { capabilities = ["create", "read", "update", "delete", "list"] }
path "sys/mounts/secret"            { capabilities = ["create", "read", "update"] }
path "sys/policies/acl/lab-admin"   { capabilities = ["read", "update"] }
path "sys/policies/acl/lab-member"  { capabilities = ["create", "read", "update"] }
path "sys/policies/acl/lab-manager" { capabilities = ["create", "read", "update"] }
path "sys/policies/acl/member-*"    { capabilities = ["create", "read", "update", "delete"] }
path "auth/token/roles/member"      { capabilities = ["create", "read", "update"] }
path "auth/token/create/member"     { capabilities = ["create", "update"] }
path "auth/token/revoke-accessor"   { capabilities = ["update"] }
path "sys/auth/oidc"                { capabilities = ["create", "read", "update", "sudo"] }
path "auth/oidc/config"             { capabilities = ["create", "read", "update"] }
path "auth/oidc/role/*"             { capabilities = ["create", "read", "update"] }
path "identity/group/name/managers" { capabilities = ["create", "read", "update"] }
path "identity/group-alias"         { capabilities = ["create", "update"] }
path "identity/lookup/entity"       { capabilities = ["update"] }
path "identity/entity/id/*"         { capabilities = ["delete"] }
EOF
}

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
  admin_policy | BAO_TOKEN="$root" bao policy write lab-admin -
  BAO_TOKEN="$root" bao token create -id="$VAULT_ADMIN_TOKEN" -policy=lab-admin -no-default-policy \
    -orphan -ttl=87600h -display-name=lab-admin >/dev/null
  BAO_TOKEN="$root" bao token revoke -self >/dev/null
  echo "vault-init: admin token created, root token revoked"
fi

export BAO_TOKEN="$VAULT_ADMIN_TOKEN"
admin_policy | bao policy write lab-admin - >/dev/null
bao read sys/mounts/secret >/dev/null 2>&1 || bao secrets enable -path=secret -version=2 kv
# member tokens: only member-* policies, no parent (they outlive nothing), one week at most
bao write auth/token/roles/member allowed_policies_glob="member-*" orphan=true renewable=false \
  token_ttl=168h token_explicit_max_ttl=168h >/dev/null
echo "vault-init: ready (secret/ store, member token role)"

[ -n "$VAULT_OIDC_CLIENT_SECRET" ] || exit 0

# ---- "Sign in with the lab account" in the UI (Keycloak realm de-lab, client vault) ----
# The vault must fetch Keycloak's discovery document from the PUBLIC issuer address (it checks
# they match); the vault server forwards that address to Keycloak inside its container.
: "${VAULT_OIDC_ISSUER:?VAULT_OIDC_ISSUER is not set}" "${VAULT_OIDC_REDIRECTS:?VAULT_OIDC_REDIRECTS is not set}"
bao read sys/auth/oidc >/dev/null 2>&1 || bao auth enable -description="lab account (Keycloak)" oidc
acc=$(bao read -field=accessor sys/auth/oidc)
bao write auth/oidc/config oidc_discovery_url="$VAULT_OIDC_ISSUER" oidc_client_id=vault \
  oidc_client_secret="$VAULT_OIDC_CLIENT_SECRET" default_role=member >/dev/null
# everyone: their own folder secret/<username>/… (the sign-in's username, filled in per token);
# the UI also writes each secret's metadata (versions kept) and reads the store's settings
bao write auth/oidc/role/member role_type=oidc user_claim=preferred_username groups_claim=groups \
  oidc_scopes=profile bound_audiences=vault allowed_redirect_uris="$VAULT_OIDC_REDIRECTS" \
  token_policies=lab-member token_ttl=8h token_max_ttl=8h >/dev/null
me="{{identity.entity.aliases.$acc.name}}"
bao policy write lab-member - >/dev/null <<EOF
path "secret/data/$me/*"     { capabilities = ["create", "read", "update", "delete"] }
path "secret/metadata/$me/*" { capabilities = ["create", "read", "update", "list", "delete"] }
path "secret/config"         { capabilities = ["read"] }
path "secret/data/_lab/*"     { capabilities = ["deny"] }
path "secret/metadata/_lab/*" { capabilities = ["deny"] }
EOF
# managers (Keycloak group managers): every member's folder, not the lab's own records
bao policy write lab-manager - >/dev/null <<'EOF'
path "secret/data/*"          { capabilities = ["create", "read", "update", "delete"] }
path "secret/metadata/*"      { capabilities = ["create", "read", "update", "list", "delete"] }
path "secret/config"          { capabilities = ["read"] }
path "secret/data/_lab/*"     { capabilities = ["deny"] }
path "secret/metadata/_lab/*" { capabilities = ["deny"] }
EOF
bao write identity/group/name/managers type=external policies=lab-manager >/dev/null
gid=$(bao read -field=id identity/group/name/managers)
bao read -field=alias identity/group/name/managers | grep -q "name:managers" ||
  bao write identity/group-alias name=managers mount_accessor="$acc" canonical_id="$gid" >/dev/null
echo "vault-init: sign-in with the lab account ready (Keycloak client vault)"
