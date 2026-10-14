# OpenBao (open-source Vault) — the lab's key vault: every member's secrets ("My secrets").
# Same file on the local stack and on k8s; the differences come from the environment:
#   BAO_PG_CONNECTION_URL  the stack's Postgres (database vault) — no volume of its own
#   BAO_UNSEAL_KEY         64 hex characters; unlocks the vault at every start (static seal),
#                          so a restart needs nobody to type unseal keys
# vault-init.sh sets it up on first start (key/value store, admin token, member token role).
ui = true
disable_mlock = true
api_addr = "http://vault:8200"

# the admin token (home-api, the Hub, Airflow) lives for years; member tokens set their own TTL
default_lease_ttl = "168h"
max_lease_ttl     = "87600h"

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = true
}

storage "postgresql" {}

seal "static" {
  current_key_id = "lab-1"
  current_key    = "env://BAO_UNSEAL_KEY"
}
