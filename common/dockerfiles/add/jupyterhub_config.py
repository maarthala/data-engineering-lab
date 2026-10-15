"""JupyterHub (local stack + k8s): Keycloak login, one Jupyter container/pod per learner,
and every learner's `iceberg` catalog pointed at their OWN Polaris lakehouse.

Spawn hook (every server start): make sure <name>_lake exists (lakehouse.provision),
give principal <name> a fresh random client secret (lakehouse.reset_secret) and hand it
ONLY to that learner's server as LAKE_* env; the IPython startup (00-spark.py)
applies it to the Spark session. Same for the key vault: a fresh token that reaches only their
own secrets (VAULT_TOKEN, labvault.member_token) for key_vault(). HUB_SPAWNER=docker (local) | kubernetes (k8s).

Keycloak hostnames: the browser goes to the PUBLIC login URL, the Hub calls Keycloak's
token/userinfo on the internal address (as oauth2-proxy does) — no hosts-file tricks.
"""
import os

import labvault
from lakehouse import bucket_name, lake_name, provision, reset_secret, storage_key, team_bucket, team_catalog, team_of

env = os.environ
c = get_config()  # noqa: F821

# ---- login: Keycloak (realm de-lab, client jupyterhub) -----------------------------
KC_PUBLIC = env["KEYCLOAK_PUBLIC_URL"].rstrip("/")      # http://localhost:8180 | http://auth.de.lan
KC_INTERNAL = env["KEYCLOAK_INTERNAL_URL"].rstrip("/")  # http://keycloak:8080
REALM = env.get("KEYCLOAK_REALM", "de-lab")
c.JupyterHub.authenticator_class = "generic-oauth"
c.GenericOAuthenticator.client_id = env.get("OAUTH_CLIENT_ID", "jupyterhub")
c.GenericOAuthenticator.client_secret = env["OAUTH_CLIENT_SECRET"]
c.GenericOAuthenticator.oauth_callback_url = env["HUB_PUBLIC_URL"].rstrip("/") + "/hub/oauth_callback"
c.GenericOAuthenticator.authorize_url = f"{KC_PUBLIC}/realms/{REALM}/protocol/openid-connect/auth"
c.GenericOAuthenticator.token_url = f"{KC_INTERNAL}/realms/{REALM}/protocol/openid-connect/token"
c.GenericOAuthenticator.userdata_url = f"{KC_INTERNAL}/realms/{REALM}/protocol/openid-connect/userinfo"
c.GenericOAuthenticator.scope = ["openid", "profile"]
c.GenericOAuthenticator.username_claim = "preferred_username"
c.GenericOAuthenticator.login_service = "Epireum's Data Engineering Lab"
c.Authenticator.auto_login = True    # straight to the lab sign-in (already signed in → straight in)
c.GenericOAuthenticator.allow_all = True                        # everyone in the realm
c.GenericOAuthenticator.manage_groups = True                     # Keycloak groups → Hub groups
c.GenericOAuthenticator.auth_state_groups_key = "oauth_user.groups"
c.GenericOAuthenticator.admin_groups = {"managers"}          # Hub admin page
c.Authenticator.auto_login = True                               # straight to Keycloak (SSO → silent)
# Follow the Keycloak session: keep the tokens and re-check them every minute. After a
# logout / switching user on the home page, the old token is rejected → the Hub signs
# you in again as whoever is logged in to Keycloak now (not the previous user).
c.GenericOAuthenticator.enable_auth_state = True
c.Authenticator.auth_refresh_age = 60
c.Authenticator.refresh_pre_spawn = True

# ---- one container (local: Docker) / pod (k8s: KubeSpawner) per learner --------------
# Both run the same single-user image; its entrypoint starts jupyterhub-singleuser with
# root = /home/jovyan/work, the learner's persistent volume.
SPAWNER = env.get("HUB_SPAWNER", "docker")
c.JupyterHub.hub_ip = "0.0.0.0"
c.JupyterHub.hub_connect_ip = env.get("HUB_CONNECT_IP", "jupyterhub")
c.Spawner.mem_limit = env.get("SINGLEUSER_MEM", "1G")
c.Spawner.cpu_limit = float(env.get("SINGLEUSER_CPU", "1"))       # one learner can't take the whole node
c.Spawner.cmd = ["bash", "/usr/local/bin/jupyter-entrypoint.sh"]
c.Spawner.default_url = "/lab"
if SPAWNER == "kubernetes":
    c.JupyterHub.spawner_class = "kubespawner.KubeSpawner"
    c.KubeSpawner.namespace = env.get("POD_NAMESPACE", "de-stack")
    c.KubeSpawner.image = env.get("SINGLEUSER_IMAGE", "ghcr.io/maarthala/de-stack/jupyter:latest")
    c.KubeSpawner.image_pull_policy = env.get("SINGLEUSER_PULL_POLICY", "IfNotPresent")
    c.KubeSpawner.pod_name_template = "jupyter-{username}"
    c.KubeSpawner.uid = 1000                                    # jovyan
    c.KubeSpawner.fs_gid = 1000
    c.KubeSpawner.mem_guarantee = "256M"
    c.KubeSpawner.cpu_guarantee = 0.05
    # the learner's work volume — kept when the pod stops
    c.KubeSpawner.storage_pvc_ensure = True
    c.KubeSpawner.delete_pvc = True                             # …and dropped with the Hub user (wipe / delete)
    c.KubeSpawner.pvc_name_template = "claim-{username}"
    c.KubeSpawner.storage_capacity = env.get("SINGLEUSER_STORAGE", "1Gi")
    c.KubeSpawner.storage_class = env.get("SINGLEUSER_STORAGE_CLASS", "microk8s-hostpath")
    c.KubeSpawner.volumes = [{"name": "work", "persistentVolumeClaim": {"claimName": "claim-{username}"}},
                             # the lab's own code (ConfigMap lab-code) over the copies in the image,
                             # so code changes need no image rebuild; 0755 for the entrypoint
                             {"name": "lab-code", "configMap": {"name": "lab-code", "defaultMode": 0o755}}]
    c.KubeSpawner.volume_mounts = [{"name": "work", "mountPath": "/home/jovyan/work"}] + [
        {"name": "lab-code", "mountPath": path, "subPath": key, "readOnly": True} for key, path in (
            ("00-spark.py", "/home/jovyan/.ipython/profile_default/startup/00-spark.py"),
            ("jupyter_server_config.py", "/etc/jupyter/jupyter_server_config.py"),
            ("lab_sync.py", "/etc/jupyter/lab_sync.py"),
            ("jupyter-entrypoint.sh", "/usr/local/bin/jupyter-entrypoint.sh"))]
    # hostpath volumes come up root-owned — hand them to jovyan before Jupyter starts
    c.KubeSpawner.init_containers = [{
        "name": "work-perms", "image": c.KubeSpawner.image, "command": ["sh", "-c", "chown 1000:1000 /home/jovyan/work"],
        "securityContext": {"runAsUser": 0}, "volumeMounts": [{"name": "work", "mountPath": "/home/jovyan/work"}]}]
    c.KubeSpawner.extra_pod_config = {"enableServiceLinks": False}  # a "jupyter" Service would inject JUPYTER_PORT
    c.KubeSpawner.start_timeout = 300                           # first pull/unpack on the node can be slow
else:
    from dockerspawner import DockerSpawner

    class LabDockerSpawner(DockerSpawner):
        async def delete_forever(self):
            """The Hub user is deleted (Manage members → wipe / delete): drop their work volume
            too, like KubeSpawner's delete_pvc."""
            from jupyterhub.utils import maybe_future
            await maybe_future(super().delete_forever())
            name = self.format_volume_name("jupyterhub-user-{username}", self)
            try:
                await self.docker("remove_volume", name)
            except Exception as e:  # noqa: BLE001 — already gone
                self.log.warning("remove volume %s: %s", name, e)

    c.JupyterHub.spawner_class = LabDockerSpawner
    c.DockerSpawner.image = env.get("SINGLEUSER_IMAGE", "local-jupyter:latest")
    c.DockerSpawner.network_name = env.get("DOCKER_NETWORK", "local_sparknet")
    c.DockerSpawner.use_internal_ip = True
    c.DockerSpawner.remove = True                               # containers are disposable…
    c.DockerSpawner.volumes = {"jupyterhub-user-{username}": "/home/jovyan/work"}  # …work isn't
    c.DockerSpawner.name_template = "jupyter-{username}"

# env every learner server gets (Spark Connect, object store, notebook auto-push). NOT the
# Hub's own AWS keys (the storage root): each learner gets their own-bucket key in pre_spawn.
PASS = ["SPARK_REMOTE", "AWS_DEFAULT_REGION",
        "AWS_REGION", "AWS_S3_ENDPOINT", "GIT_AUTOPUSH", "GIT_REPO_URL", "GIT_BRANCH",
        "GIT_USERNAME", "GIT_TOKEN"]
c.Spawner.environment = {k: env[k] for k in PASS if env.get(k)}


async def pre_spawn(spawner):
    """Learner's own lakehouse + a fresh Polaris secret, handed only to their server."""
    name = lake_name(spawner.user.name)
    provision(name, manager=any(g.name == "managers" for g in spawner.user.groups))
    secret = reset_secret(name)
    s3_key, s3_secret = storage_key(name)      # own-bucket-only key: drive, sync, pandas/boto3
    spawner.environment.update({
        "LAKE_USER": name,
        "LAKE_WAREHOUSE": f"{name}_lake",
        "LAKE_CREDENTIAL": f"{name}:{secret}",
        "LAKE_BUCKET": bucket_name(name),
        "LAKE_S3_KEY": s3_key,
        "LAKE_S3_SECRET": s3_secret,
        # the standard names too, so pandas / s3fs / boto3 use the learner's key by default
        "AWS_ACCESS_KEY_ID": s3_key,
        "AWS_SECRET_ACCESS_KEY": s3_secret,
        # %%sql materialized-view pipeline files go to the learner's own bucket too
        "MV_PIPELINE_STORAGE": f"s3a://{bucket_name(name)}/pipelines/mv",
        "GIT_AUTHOR_NAME": spawner.user.name,
        "GIT_AUTHOR_EMAIL": f"{spawner.user.name}@de.lan",
    })
    try:                                       # their team's lakehouse → `team` in Spark
        team = team_of(name)
        spawner.environment["TEAM_CATALOG"] = team_catalog(team) if team else ""
        spawner.environment["TEAM_BUCKET"] = team_bucket(team) if team else ""
    except Exception as e:  # noqa: BLE001 — Jupyter still starts with their own lake
        spawner.log.warning("team of %s: %s", name, e)
    if labvault.enabled():                     # "My secrets": their own vault folder only
        try:
            labvault.import_env_file(name)     # once: the old My variables file
            spawner.environment.update({"VAULT_ADDR": labvault.VAULT,
                                        "VAULT_TOKEN": labvault.member_token(name)})
        except Exception as e:  # noqa: BLE001 — Jupyter still starts; key_vault() says why
            spawner.log.warning("vault token for %s: %s", name, e)
    spawner.log.info("lakehouse %s_lake ready for %s", name, spawner.user.name)


c.Spawner.pre_spawn_hook = pre_spawn

# ---- housekeeping -------------------------------------------------------------------
c.JupyterHub.db_url = "sqlite:////srv/jupyterhub/data/jupyterhub.sqlite"
c.JupyterHub.cookie_secret_file = "/srv/jupyterhub/data/jupyterhub_cookie_secret"
# stop idle learner servers after an hour (frees memory; work is in their volume)
c.JupyterHub.load_roles = [{
    "name": "idle-culler",
    "scopes": ["list:users", "read:users:activity", "read:servers", "delete:servers"],
    "services": ["idle-culler"],
}]
c.JupyterHub.services = [{
    "name": "idle-culler",
    # localhost: on k8s the hub pod can't reach itself through its own Service address
    "command": ["python3", "-m", "jupyterhub_idle_culler", "--timeout=3600",
                "--url=http://localhost:8081/hub/api"],
}]
# the landing page's "Manage members" (home-api): stop a learner's server and delete them
# from the Hub — their work volume goes with them (wipe / delete)
if env.get("HUB_ADMIN_TOKEN"):
    c.JupyterHub.load_roles.append({"name": "lab-admin", "services": ["lab-admin"],
                                    "scopes": ["admin:users", "admin:servers"]})
    c.JupyterHub.services.append({"name": "lab-admin", "api_token": env["HUB_ADMIN_TOKEN"]})
