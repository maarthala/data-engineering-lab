"""Airflow cluster policy: learner DAGs (bundle "learners", synced from <user>-lake/files/src/dags/)
must have a dag_id starting with <user>_ — one shared Airflow, so names can't clash. A team's DAGs
(team-<team>-lake → folder team-<team>/) start with team_<team>_ and are shared with the role
lab_team_<team>, which everyone in the team holds (airflow_webserver_config.py, labadmin.set_team).
A DAG that breaks the rule shows up as an import error naming the expected prefix.

Visibility: a learner DAG is readable / editable / runnable only by its owner's role lab_<user>
(learners have no global DAG access — see airflow_webserver_config.py); platform DAGs are
readable by every learner. (The members' DAG processor has no FAB login, so it can't grant —
airflow_lab_guard.py syncs these permissions from Airflow's trusted side.)

k8s (LAB_MEMBER_EXECUTOR=KubernetesExecutor): a learner DAG's tasks run in their own pod in the
jobs namespace, never inside Airflow. pod_mutation_hook builds that pod from the trusted template
(LAB_JOB_POD_TEMPLATE) — nothing of the DAG's own executor_config survives (it could name another
image, volumes or env) — and adds the owner's logins (lab_jobs.job_env).
"""
import json
import os

from airflow.exceptions import AirflowClusterPolicyViolation

BUNDLE = "/learners/"
MEMBER_EXECUTOR = os.environ.get("LAB_MEMBER_EXECUTOR", "")
JOB_POD_TEMPLATE = os.environ.get("LAB_JOB_POD_TEMPLATE", "")
WORKLOAD = ["python", "-m", "airflow.sdk.execution_time.execute_workload", "--json-string"]


def owner_prefix(owner):
    """The dag_id prefix of a DAG folder: ravi-k → ravi_k_ · team-sales → team_sales_."""
    return owner.replace("-", "_") + "_"


def learner_access(owner):
    """access_control of the DAGs in learners/<owner>/: only the owner's role (a team's: the
    team's role lab_team_<team>, which everyone in the team holds)."""
    return {f"lab_{owner.replace('-', '_')}": {
        "DAGs": {"can_read", "can_edit", "can_delete"},
        "DAG Runs": {"can_read", "can_create", "can_delete"}}}


def dag_policy(dag):
    loc = str(dag.fileloc or "")
    if BUNDLE not in loc:
        dag.access_control = {**(dag.access_control or {}), "Learner": {"DAGs": {"can_read"}}}
        return
    user = loc.split(BUNDLE, 1)[1].split("/", 1)[0]
    prefix = owner_prefix(user)
    if not dag.dag_id.startswith(prefix):
        raise AirflowClusterPolicyViolation(
            f"DAG id {dag.dag_id!r} must start with {prefix!r} "
            f"({'the team' if user.startswith('team-') else 'your username'}) — rename it, "
            f"e.g. dag_id='{prefix}{dag.dag_id}'")
    dag.access_control = learner_access(user)
    if user not in dag.tags:
        dag.tags.add(user) if isinstance(dag.tags, set) else dag.tags.append(user)


def task_policy(task):
    """k8s: a learner DAG's tasks run on the members' executor (their own pod)."""
    if MEMBER_EXECUTOR and BUNDLE in str(getattr(task.dag, "fileloc", "") or ""):
        task.executor = MEMBER_EXECUTOR


def pod_mutation_hook(pod):
    """A member task's pod (KubernetesExecutor, label airflow-worker): rebuilt from the trusted
    template, the owner's logins added. Its identity comes from the workload the scheduler made
    (DAG folder + dag_id prefix must agree); what runs is the owner's own DAG file (the pod syncs
    only the owner's folder, with the owner's key). KubernetesPodOperator pods (platform DAGs)
    stay as written."""
    if "airflow-worker" not in (pod.metadata.labels or {}):
        return
    from kubernetes.client import models as k8s
    from airflow.providers.cncf.kubernetes.pod_generator import PodGenerator
    args = (pod.spec.containers or [k8s.V1Container(name="x")])[0].args or []
    if not JOB_POD_TEMPLATE or len(args) != len(WORKLOAD) + 1 or list(args[:-1]) != WORKLOAD:
        raise AirflowClusterPolicyViolation("member task pod: not a task workload")
    w = json.loads(args[-1])
    ti, rel = w["ti"], w.get("dag_rel_path") or ""
    owner = rel.split("/", 1)[0]
    if (w.get("bundle_info") or {}).get("name") != "learners" or "/" not in rel \
            or not ti["dag_id"].startswith(owner_prefix(owner)):
        raise AirflowClusterPolicyViolation(f"member task pod: {ti['dag_id']} is not a DAG of {owner!r}")
    import lab_jobs
    base = PodGenerator.deserialize_model_file(JOB_POD_TEMPLATE)
    c = base.spec.containers[0]
    c.name, c.args = "base", list(args)
    c.env = [e for e in (c.env or []) if not e.name.startswith(("LAB_JOB_", "AWS_ACCESS", "AWS_SECRET", "VAULT_TOKEN"))] + [
        k8s.V1EnvVar(name=k, value=v) for k, v in lab_jobs.job_env(owner).items()] + [
        k8s.V1EnvVar(name="AIRFLOW_IS_K8S_EXECUTOR_POD", value="True")]
    pod.spec = base.spec
    pod.metadata.namespace = base.metadata.namespace or pod.metadata.namespace
    keep = {"dag_id": ti["dag_id"], "task_id": ti["task_id"], "run_id": ti["run_id"],
            "try_number": str(ti["try_number"])}
    if ti.get("map_index", -1) >= 0:
        keep["map_index"] = str(ti["map_index"])
    pod.metadata.annotations = {**{k: v for k, v in (pod.metadata.annotations or {}).items()
                                   if k in ("logical_date", "execution_date")}, **keep}


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
