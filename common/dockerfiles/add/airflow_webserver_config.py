"""Airflow UI login (FAB auth manager) — sign in with the lab account (Keycloak).

On when KEYCLOAK_ENABLED=1, otherwise Airflow's normal database login. Users are created
on first sign-in; Keycloak groups map to Airflow roles and are re-synced every login
(managers → Admin, learners → Learner + their own lab_<user> role + lab_team_<team>). Learners have no email, so the username is the
identity (placeholder <user>@de.lan for FAB's unique-email column).

Endpoints come from Keycloak's discovery document fetched on the INTERNAL address:
Keycloak returns the browser-facing authorize URL plus internal token/keys URLs, and
tokens carry the public issuer — no hosts-file tricks (same as Superset / SQLPad).
"""
import os

from flask_appbuilder.const import AUTH_DB, AUTH_OAUTH

AUTH_TYPE = AUTH_DB
WTF_CSRF_ENABLED = True

if os.environ.get("KEYCLOAK_ENABLED") == "1":
    from airflow.providers.fab.auth_manager.security_manager.override import (
        FabAirflowSecurityManagerOverride,
    )
    from airflow.providers.fab.auth_manager.views.auth_oauth import CustomAuthOAuthView
    from flask import request
    from flask_appbuilder import expose

    class LabAuthOAuthView(CustomAuthOAuthView):
        """Straight to the lab sign-in (the only provider) instead of a "Sign in with keycloak"
        page — already signed in to the lab, the user lands in Airflow. /auth/login/?local=1 shows it."""

        @expose("/login/")
        @expose("/login/<provider>")
        def login(self, provider=None):
            if provider is None and request.args.get("local") != "1":
                provider = "keycloak"
            return super().login(provider)

    _KC = os.environ.get("KEYCLOAK_INTERNAL_URL", "http://keycloak:8080").rstrip("/") + "/realms/de-lab"

    AUTH_TYPE = AUTH_OAUTH
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Learner"
    AUTH_ROLES_MAPPING = {"managers": ["Admin"], "learners": ["Learner"]}
    AUTH_ROLES_SYNC_AT_LOGIN = True
    OAUTH_PROVIDERS = [{
        "name": "keycloak",
        "icon": "fa-key",
        "token_key": "access_token",
        "remote_app": {
            "client_id": os.environ.get("KEYCLOAK_CLIENT_ID", "airflow"),
            "client_secret": os.environ.get("KEYCLOAK_CLIENT_SECRET", "airflow-secret"),
            "server_metadata_url": f"{_KC}/.well-known/openid-configuration",
            "api_base_url": f"{_KC}/protocol/openid-connect/",
            "client_kwargs": {"scope": "openid profile"},
        },
    }]

    # Learner = the built-in User role WITHOUT the global "DAGs" permissions: a learner sees and
    # runs only the DAGs shared with their own role lab_<user> (their <user>_* DAGs, set by the
    # dag policy in airflow_local_settings.py) plus the platform DAGs (read-only).
    LEARNER_ROLE = "Learner"
    _GLOBAL_DAG = {("can_read", "DAGs"), ("can_edit", "DAGs"), ("can_delete", "DAGs")}

    class LabSecurityManager(FabAirflowSecurityManagerOverride):
        """Username = identity; Keycloak groups → role mapping; learners also get lab_<user>."""
        authoauthview = LabAuthOAuthView

        def _learner_role(self):
            role = self.find_role(LEARNER_ROLE) or self.add_role(LEARNER_ROLE)
            # User's permissions minus the global DAG ones — topped up on every sign-in (the DAG
            # processor may have created the role first, holding only per-DAG grants)
            have = {(p.action.name, p.resource.name) for p in role.permissions}
            for perm in self.find_role("User").permissions:
                key = (perm.action.name, perm.resource.name)
                if key not in _GLOBAL_DAG and key not in have:
                    self.add_permission_to_role(role, perm)
            return role

        def sync_perm_for_dag(self, dag_id, access_control=None):
            """The DAG policy names lab_<user> / Learner in access_control; create a missing role
            here (trusted process — DAG parsing has no database access) instead of failing the DAG."""
            for name in (access_control or {}):
                if self.find_role(name) is None:
                    self.add_role(name)
            return super().sync_perm_for_dag(dag_id, access_control)

        def auth_user_oauth(self, userinfo):
            self._learner_role()                         # exists before the role sync needs it
            user = super().auth_user_oauth(userinfo)
            if user and not any(r.name == "Admin" for r in user.roles):
                # their own lab_<user> (their DAGs) + lab_team_<team> (their team's DAGs, from
                # the Keycloak group team-<team>) — the same names the dag policy shares DAGs with
                want = [f"lab_{user.username.replace('-', '_')}"] + [
                    "lab_team_" + g[len("team-"):] for g in userinfo.get("role_keys", []) if g.startswith("team-")]
                roles = [r for r in user.roles if not r.name.startswith("lab_team_") or r.name in want]
                for name in want:
                    if not any(r.name == name for r in roles):
                        roles.append(self.find_role(name) or self.add_role(name))
                if {r.name for r in roles} != {r.name for r in user.roles}:
                    user.roles = roles
                    self.update_user(user)
            return user

        def get_oauth_user_info(self, provider, resp):
            me = self.appbuilder.sm.oauth_remotes[provider].get("userinfo").json()
            user = me["preferred_username"]
            return {
                "username": user,
                "first_name": me.get("given_name") or user,
                "last_name": me.get("family_name") or "",
                "email": me.get("email") or f"{user}@de.lan",
                "role_keys": me.get("groups", []),
            }

        oauth_user_info = get_oauth_user_info

    SECURITY_MANAGER_CLASS = LabSecurityManager
