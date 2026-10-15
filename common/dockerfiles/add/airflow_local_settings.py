"""Airflow cluster policy: learner DAGs (bundle "learners", synced from <user>-lake/files/src/dags/)
must have a dag_id starting with <user>_ — one shared Airflow, so names can't clash. A team's DAGs
(team-<team>-lake → folder team-<team>/) start with team_<team>_ and are shared with the role
lab_team_<team>, which everyone in the team holds (airflow_webserver_config.py, labadmin.set_team).
A DAG that breaks the rule shows up as an import error naming the expected prefix.

Visibility: a learner DAG is readable / editable / runnable only by its owner's role lab_<user>
(learners have no global DAG access — see airflow_webserver_config.py); platform DAGs are
readable by every learner.
"""
from airflow.exceptions import AirflowClusterPolicyViolation

BUNDLE = "/learners/"


def dag_policy(dag):
    loc = str(dag.fileloc or "")
    if BUNDLE not in loc:
        dag.access_control = {**(dag.access_control or {}), "Learner": {"DAGs": {"can_read"}}}
        return
    user = loc.split(BUNDLE, 1)[1].split("/", 1)[0]
    prefix = user.replace("-", "_") + "_"
    if not dag.dag_id.startswith(prefix):
        raise AirflowClusterPolicyViolation(
            f"DAG id {dag.dag_id!r} must start with {prefix!r} "
            f"({'the team' if user.startswith('team-') else 'your username'}) — rename it, "
            f"e.g. dag_id='{prefix}{dag.dag_id}'")
    dag.access_control = {f"lab_{user.replace('-', '_')}": {
        "DAGs": {"can_read", "can_edit", "can_delete"},
        "DAG Runs": {"can_read", "can_create", "can_delete"}}}
    if user not in dag.tags:
        dag.tags.add(user) if isinstance(dag.tags, set) else dag.tags.append(user)


def _create_missing_roles():
    """The DAG processor syncs DAG permissions with Airflow's built-in ApplessAirflowSecurityManager
    (not the configured one), and it rejects a DAG whose access_control names a role that doesn't
    exist yet — e.g. lab_<user> before that learner first signs in to Airflow. Create such roles
    first. This module is also loaded by Airflow's own (trusted) processes, which is where the sync
    runs; DAG parsing itself has no database access."""
    try:
        from airflow.providers.fab.www.security_appless import ApplessAirflowSecurityManager as manager
    except Exception:  # noqa: BLE001 — FAB not installed / not loadable here
        return
    sync = manager.sync_perm_for_dag
    if getattr(sync, "_lab", False):
        return

    def sync_perm_for_dag(self, dag_id, access_control=None):
        for name in (access_control or {}):
            if self.find_role(name) is None:
                self.add_role(name)
        return sync(self, dag_id, access_control)

    sync_perm_for_dag._lab = True
    manager.sync_perm_for_dag = sync_perm_for_dag


_create_missing_roles()
