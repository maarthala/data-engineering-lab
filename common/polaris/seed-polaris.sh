#!/usr/bin/env bash
# Seed the GOVERNED Polaris catalog for the training course: the `polaris_lake`
# catalog on object storage, medallion namespaces, and the analyst/engineer/lead RBAC
# (per-persona, activated via their Keycloak `principal_roles` claim).
#
# Run once by the operator after Polaris is up (persistence in Postgres, so this
# survives restarts). Idempotent-ish: re-creates 409 (ignored).
set -euo pipefail

B="${POLARIS_URL:-http://localhost:8185}"
# Logins: the admin (root) and the three demo personas. Defaults = the laptop stack's published
# ones; a team stack passes its private values (k8s Secret de-stack-secrets).
ROOT="${POLARIS_ROOT_CREDENTIAL:-root:s3cr3t}"
PERSONA_SECRET="${POLARIS_PERSONA_SECRET:-}"      # empty → each persona's secret = its name
M="$B/api/management/v1"; C="$B/api/catalog/v1"
RT=$(curl -s -m 10 "$B/api/catalog/v1/oauth/tokens" --user "$ROOT" -H 'Polaris-Realm: POLARIS' \
      -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL | python3 -c "import sys,json;print(json.load(sys.stdin)['access_token'])")
H=(-H "Authorization: Bearer $RT" -H "Content-Type: application/json" -H "Polaris-Realm: POLARIS")
post() { curl -s -m 10 "${H[@]}" -X POST "$1" -d "$2" -o /dev/null -w "%{http_code} "; }
put()  { curl -s -m 10 "${H[@]}" -X PUT  "$1" -d "$2" -o /dev/null -w "%{http_code} "; }
grant(){ put "$M/catalogs/polaris_lake/catalog-roles/$1/grants" \
         "{\"grant\":{\"type\":\"namespace\",\"namespace\":[\"$2\"],\"privilege\":\"$3\"}}"; }

echo "catalog:   $(post $M/catalogs '{"catalog":{"name":"polaris_lake","type":"INTERNAL","properties":{"default-base-location":"s3://demo-bucket/warehouse","polaris.config.drop-with-purge.enabled":"true"},"storageConfigInfo":{"storageType":"S3","allowedLocations":["s3://demo-bucket/warehouse"],"endpoint":"http://storage:9000","stsEndpoint":"http://storage:9000","roleArn":"arn:aws:iam::000000000000:role/lab-vending","kmsUnavailable":true,"pathStyleAccess":true,"region":"us-east-1"}}}')"
echo "namespaces:$(for ns in bronze silver gold crm marketplace; do post $C/polaris_lake/namespaces "{\"namespace\":[\"$ns\"]}"; done)"
echo "principals:$(for p in analyst engineer lead; do post $M/principals "{\"principal\":{\"name\":\"$p\"}}"; done)"
# Pin known client credentials so personas log in cleanly (clientId=secret=name).
# No IdP needed — principals authenticate with client id/secret (API + Console).
echo "creds:     $(for p in analyst engineer lead; do post $M/principals/$p/reset "{\"clientId\":\"$p\",\"clientSecret\":\"${PERSONA_SECRET:-$p}\"}"; done)"
echo "p-roles:   $(for r in analyst_role engineer_role lead_role; do post $M/principal-roles "{\"principalRole\":{\"name\":\"$r\"}}"; done)"
echo "assign:    $(for pr in analyst:analyst_role engineer:engineer_role lead:lead_role; do put $M/principals/${pr%%:*}/principal-roles "{\"principalRole\":{\"name\":\"${pr##*:}\"}}"; done)"
echo "c-roles:   $(for r in analyst_role engineer_role lead_role; do post $M/catalogs/polaris_lake/catalog-roles "{\"catalogRole\":{\"name\":\"${r}_cr\"}}"; put $M/principal-roles/$r/catalog-roles/polaris_lake "{\"catalogRole\":{\"name\":\"${r}_cr\"}}"; done)"

# Admin content access: root (service_admin) runs the ingest/build pipeline, which
# creates + writes tables with credential vending (CREATE_TABLE_..._WITH_WRITE_DELEGATION).
# service_admin manages the catalog but needs CATALOG_MANAGE_CONTENT for data ops.
echo "admin:     $(post $M/catalogs/polaris_lake/catalog-roles '{"catalogRole":{"name":"admin_cr"}}'; put $M/catalogs/polaris_lake/catalog-roles/admin_cr/grants '{"grant":{"type":"catalog","privilege":"CATALOG_MANAGE_CONTENT"}}'; put $M/principal-roles/service_admin/catalog-roles/polaris_lake '{"catalogRole":{"name":"admin_cr"}}')"

# medallion grants (per persona)
echo "analyst:   $(for pr in TABLE_READ_DATA TABLE_LIST; do grant analyst_role_cr gold $pr; done)"
echo "engineer:  $(for pr in TABLE_READ_DATA TABLE_LIST; do grant engineer_role_cr gold $pr; done; for pr in TABLE_READ_DATA TABLE_LIST TABLE_WRITE_DATA TABLE_CREATE; do grant engineer_role_cr silver $pr; done)"
echo "lead:      $(for ns in bronze silver gold; do for pr in TABLE_READ_DATA TABLE_LIST TABLE_WRITE_DATA TABLE_CREATE; do grant lead_role_cr $ns $pr; done; done)"
echo "Done."
