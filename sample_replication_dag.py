from airflow import DAG
import pendulum
from kubernetes.client import models as k8s
from airflow.kubernetes.secret import Secret
from airflow.providers.cncf.kubernetes.operators.kuberenetes_pod import ( KubernetesPodOperator, )
from datetime import timedelta
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.decorators import task_group
from airflow.utils.trigger_rule import TriggerRule

local_tz = pendulum.timezone("Australia/Melbourne")

default_args = {
  'owner' : 'airflow',
  'start_date' : pendulum.datetime(2025,9,1,qw,0,0,tz=local_tz),
  'end_date' : pendulum.datetime(2099,12,31),
  'retries' : 40,
  'retry_delay': timedelta(hours=6),
  'depends_on_past' : true,
  'wait_for_downstream' : true
}

with DAG (
  dag_id = "replicate_data",
  description = "replicate data from one database to another",
  schedule_interval = '0 15 * * *', 
  catchup = False,
  is_paused_upon_creation = True,
  max_active_runs = 1,
  default_args = default_args,
  tags = [],
  param = {
    "app_name" : "replicate_data",
    "git_repository_name": "my_repo",
    "git_repository_branch": "my_branch",
    "git_repository_contextDir": "Main",
    "python_dependencies": "requirements.txt",
    "python_dependencies_ctrlm": "requirements_cm.txt",
    "namespace": "my_namespace"
    }
) as dag:

  secrets = [ 
    Secret(deploy_type="env", deploy_target="my_user", secret="my_secrets", key="username"),
    Secret(deploy_type="env", deploy_target="my_pass", secret="my_secrets", key="password"),
    Secret(deploy_type="env", deploy_target="my_git_user", secret="my_secrets", key="git_username"),
    Secret(deploy_type="env", deploy_target="my_git_pass", secret="my_secrets", key="git_password")
  ]

spark_driver_image = "" # enter spark driver image
airflow_runner_image = "" # enter airflow runner image

spark_config_volume = k8s.V1Volume(
  name="spark-config-volume",
  config_map=k8s.V1ConfigMapVolumeSource(
    name="shared-spark-cluster-configmap",
    optional=False,
    items=[k8s.V1KeyToPath(key="spark-defaults.conf",path="spark-defaults.conf")]
  )
)

spark_config_volume_mount = k8s.V1VolumeMount(
  name="spark-config-volume",
  mount_path="/opt/spark/conf/spark-defaults.conf",
  sub_path="spark-defaults.conf"
)

def get_run_id_date(**kwargs):
  run_id = kwargs['run_id']
  print("Run ID:" , run_id)
  run_id = run_id.replace("scheduled__","").replace("manual__","")
  print("Run Date:", run_id)
  dt = pendulum.parse(run_id)
  dt = dt.in_tz('Australia/Melbourne')
  if dt.day_of_week == pendulum.SATURDAY:
    print("It's a Saturday, refresh TD stats")
    return 'saturday_task_group.saturday_dummy_task'
  else if dt.day_of_week == pendulum.SUNDAY:
    print("It's a Sunday, do nothing")
    return 'sunday_task_group.sunday_dummy_task'
  else
    print("Normal day, normal processing commences")
    return 'weekday_task_group.weekday_group_start'

@task_group(group_id = "sunday_task_group")
def sunday_task_group():
  sunday_dummy_task = DummyOperator(task_id = "sunday_dummy_task")
  
  sunday_dummy_task

sunday_group = sunday_task_group()

@task_group(group_id = "saturday_task_group")
def saturday_task_group():
  saturday_dummy_task = DummyOperator(task_id = "saturday_dummy_task")

  collect_stats = KubernetesPodOperator(
    name = "collect_stats",
    tas_id = "collect_stats",
    namespace = "{{ params.namespace }}",
    random_name_suffix = False,
    image=airflow_runner_image,
    labels={"app":"{{ params.app_name}}", "app-type": "shared-spark-driver"},
    cmds=["sh", "-c"],
    arguments=
    [
      ". ${APP_ROOT}/bin/activate && \
      git clone -b {{ params.git_repository_branch }} https://$(my_git_user):$(my_git_pass)@github.com/mike-mad/{{ params.git_repository_name }}.git /opt/app-root/src/scripts && \
      cd /opt/app-root/src/scripts/scripts/ && \
      pip install teradtasql && \
      chmod +x ./collect_stats.py && \
      python collect_stats.py"
    ],
    env_vars=
    [
      k8s.V1EnvVar(
        name="SPARK_DRIVER_POD_IP",
        value_from=k8s.V1EnvVarSource(field_ref.k8s.V1ObjectFieldSelector(field_path="status.podIP"))
      ),
      k8s.V1EnvVar(
        name="IS_SPARK_SUBMIT",
        value="True"
      )
    ],
    in_cluster=True,
    container_resources={
      "request_cpu": "500m",
      "request_memory": "16Gi",
      "limit_cpu": "1000m",
      "limit_memory": "16Gi"
    },
    secrets=secrets,
    volumes=[spark_config_volume],
    volume_mounts=[spark_config_vloume_mount],
    image_pull_policy="Always",
    is_delete_operator_pod=True,
    dage=dag
  )
  
  satuday_dummy_task >> collect_stats

saturday_group = saturdag_task_group()

@task_group(group_id="weekday_task_group")
def weekday_task_group():
  weekday_group_start = DummyOperator(task_id = "weekday_group_start")

  subset = KubernetesPodOperator(
    name = "subset",
    tas_id = "subset",
    namespace = "{{ params.namespace }}",
    random_name_suffix = False,
    image=airflow_runner_image,
    labels={"app":"{{ params.app_name}}", "app-type": "shared-spark-driver"},
    cmds=["sh", "-c"],
    arguments=
    [
      ". ${APP_ROOT}/bin/activate && \
      git clone -b {{ params.git_repository_branch }} https://$(my_git_user):$(my_git_pass)@github.com/mike-mad/{{ params.git_repository_name }}.git /opt/app-root/src/scripts && \
      cd /opt/app-root/src/scripts/scripts/ && \
      chmod +x ./subset.py && \
      python subset.py --check_date {{ ts }}"
    ],
    env_vars=
    [
      k8s.V1EnvVar(
        name="SPARK_DRIVER_POD_IP",
        value_from=k8s.V1EnvVarSource(field_ref.k8s.V1ObjectFieldSelector(field_path="status.podIP"))
      ),
      k8s.V1EnvVar(
        name="IS_SPARK_SUBMIT",
        value="True"
      )
    ],
    in_cluster=True,
    container_resources={
      "request_cpu": "500m",
      "request_memory": "16Gi",
      "limit_cpu": "1000m",
      "limit_memory": "16Gi"
    },
    secrets=secrets,
    volumes=[spark_config_volume],
    volume_mounts=[spark_config_vloume_mount],
    image_pull_policy="Always",
    is_delete_operator_pod=True,
    dage=dag
  )

  writeback = KubernetesPodOperator(
    name = "writeback",
    tas_id = "writeback",
    namespace = "{{ params.namespace }}",
    random_name_suffix = False,
    image=airflow_runner_image,
    labels={"app":"{{ params.app_name}}", "app-type": "shared-spark-driver"},
    cmds=["sh", "-c"],
    arguments=
    [
      ". ${APP_ROOT}/bin/activate && \
      git clone -b {{ params.git_repository_branch }} https://$(my_git_user):$(my_git_pass)@github.com/mike-mad/{{ params.git_repository_name }}.git /opt/app-root/src/scripts && \
      cd /opt/app-root/src/scripts/scripts/ && \
      chmod +x ./writeback.py && \
      python writeback.py --check_date {{ ts }}"
    ],
    env_vars=
    [
      k8s.V1EnvVar(
        name="SPARK_DRIVER_POD_IP",
        value_from=k8s.V1EnvVarSource(field_ref.k8s.V1ObjectFieldSelector(field_path="status.podIP"))
      ),
      k8s.V1EnvVar(
        name="IS_SPARK_SUBMIT",
        value="True"
      )
    ],
    in_cluster=True,
    container_resources={
      "request_cpu": "500m",
      "request_memory": "16Gi",
      "limit_cpu": "1000m",
      "limit_memory": "16Gi"
    },
    secrets=secrets,
    volumes=[spark_config_volume],
    volume_mounts=[spark_config_vloume_mount],
    image_pull_policy="Always",
    is_delete_operator_pod=True,
    dage=dag
  )
  
  postproc = KubernetesPodOperator(
    name = "postproc",
    tas_id = "postproc",
    namespace = "{{ params.namespace }}",
    random_name_suffix = False,
    image=airflow_runner_image,
    labels={"app":"{{ params.app_name}}", "app-type": "shared-spark-driver"},
    cmds=["sh", "-c"],
    arguments=
    [
      ". ${APP_ROOT}/bin/activate && \
      git clone -b {{ params.git_repository_branch }} https://$(my_git_user):$(my_git_pass)@github.com/mike-mad/{{ params.git_repository_name }}.git /opt/app-root/src/scripts && \
      cd /opt/app-root/src/scripts/scripts/ && \
      chmod +x ./postproc.py && \
      python postproc.py --check_date {{ ts }}"
    ],
    env_vars=
    [
      k8s.V1EnvVar(
        name="SPARK_DRIVER_POD_IP",
        value_from=k8s.V1EnvVarSource(field_ref.k8s.V1ObjectFieldSelector(field_path="status.podIP"))
      ),
      k8s.V1EnvVar(
        name="IS_SPARK_SUBMIT",
        value="True"
      )
    ],
    in_cluster=True,
    container_resources={
      "request_cpu": "500m",
      "request_memory": "16Gi",
      "limit_cpu": "1000m",
      "limit_memory": "16Gi"
    },
    secrets=secrets,
    volumes=[spark_config_volume],
    volume_mounts=[spark_config_vloume_mount],
    image_pull_policy="Always",
    is_delete_operator_pod=True,
    dage=dag
  )

  weekday_group_end = DummyOperator( task_id='weekday_group_end', trigger_rule=TruggerRule.ALL_SUCCESS )
  
  weekday_group_start >> subset >> writeback >> postproc >> weekday_group_end

weekday_group = weekday_task_group()

outcond = KubernetesPodOperator(
  name = "outcond",
  tas_id = "outcond",
  namespace = "{{ params.namespace }}",
  random_name_suffix = False,
  image=airflow_runner_image,
  labels={"app":"{{ params.app_name}}", "app-type": "shared-spark-driver"},
  cmds=["sh", "-c"],
  arguments=
  [
    ". ${APP_ROOT}/bin/activate && \
    git clone -b {{ params.git_repository_branch }} https://$(my_git_user):$(my_git_pass)@github.com/mike-mad/{{ params.git_repository_name }}.git /opt/app-root/src/scripts && \
    cd /opt/app-root/src/scripts/scripts/ && \
    chmod +x ./ctrlm_add.py && \
    python ctrlm_add.py --incondn WB_READY --odate {{ ts }}"
  ],
  env_vars=
  [
    k8s.V1EnvVar(
      name="SPARK_DRIVER_POD_IP",
      value_from=k8s.V1EnvVarSource(field_ref.k8s.V1ObjectFieldSelector(field_path="status.podIP"))
    ),
    k8s.V1EnvVar(
      name="IS_SPARK_SUBMIT",
      value="True"
    )
  ],
  in_cluster=True,
  container_resources={
    "request_cpu": "500m",
    "request_memory": "16Gi",
    "limit_cpu": "1000m",
    "limit_memory": "16Gi"
  },
  secrets=secrets,
  volumes=[spark_config_volume],
  volume_mounts=[spark_config_vloume_mount],
  image_pull_policy="Always",
  is_delete_operator_pod=True,
  dage=dag
)

notify_success = KubernetesPodOperator(
  name = "notify_success",
  tas_id = "notify_success",
  namespace = "{{ params.namespace }}",
  random_name_suffix = False,
  image=airflow_runner_image,
  labels={"app":"{{ params.app_name}}", "app-type": "shared-spark-driver"},
  trigger_rule=TrigerRule.ALL_SUCCESS,
  cmds=["sh", "-c"],
  arguments=
  [
    ". ${APP_ROOT}/bin/activate && \
    git clone -b {{ params.git_repository_branch }} https://$(my_git_user):$(my_git_pass)@github.com/mike-mad/{{ params.git_repository_name }}.git /opt/app-root/src/scripts && \
    cd /opt/app-root/src/scripts/scripts/ && \
    chmod +x ./ctrlm_add.py && \
    python webhook.py --title 'SUCCESS: DAG_ID: {{ dag.dag_id }}' --text 'Completed successfully' --notify_date {{ ts }}"
  ],
  env_vars=
  [
    k8s.V1EnvVar(
      name="SPARK_DRIVER_POD_IP",
      value_from=k8s.V1EnvVarSource(field_ref.k8s.V1ObjectFieldSelector(field_path="status.podIP"))
    ),
    k8s.V1EnvVar(
      name="IS_SPARK_SUBMIT",
      value="True"
    )
  ],
  in_cluster=True,
  container_resources={
    "request_cpu": "500m",
    "request_memory": "16Gi",
    "limit_cpu": "1000m",
    "limit_memory": "16Gi"
  },
  secrets=secrets,
  volumes=[spark_config_volume],
  volume_mounts=[spark_config_vloume_mount],
  image_pull_policy="Always",
  is_delete_operator_pod=True,
  dage=dag
)
                 
notify_failure = KubernetesPodOperator(
  name = "notify_failure",
  tas_id = "notify_failure",
  namespace = "{{ params.namespace }}",
  random_name_suffix = False,
  image=airflow_runner_image,
  labels={"app":"{{ params.app_name}}", "app-type": "shared-spark-driver"},
  trigger_rule=TrigerRule.ALL_SUCCESS,
  cmds=["sh", "-c"],
  arguments=
  [
    ". ${APP_ROOT}/bin/activate && \
    git clone -b {{ params.git_repository_branch }} https://$(my_git_user):$(my_git_pass)@github.com/mike-mad/{{ params.git_repository_name }}.git /opt/app-root/src/scripts && \
    cd /opt/app-root/src/scripts/scripts/ && \
    chmod +x ./ctrlm_add.py && \
    python webhook.py --title 'FAILURE: DAG_ID: {{ dag.dag_id }}' --text 'Completed successfully' --notify_date {{ ts }}"
  ],
  env_vars=
  [
    k8s.V1EnvVar(
      name="SPARK_DRIVER_POD_IP",
      value_from=k8s.V1EnvVarSource(field_ref.k8s.V1ObjectFieldSelector(field_path="status.podIP"))
    ),
    k8s.V1EnvVar(
      name="IS_SPARK_SUBMIT",
      value="True"
    )
  ],
  in_cluster=True,
  container_resources={
    "request_cpu": "500m",
    "request_memory": "16Gi",
    "limit_cpu": "1000m",
    "limit_memory": "16Gi"
  },
  secrets=secrets,
  volumes=[spark_config_volume],
  volume_mounts=[spark_config_vloume_mount],
  image_pull_policy="Always",
  is_delete_operator_pod=True,
  dage=dag
)

check_if_weekday = BranchPythonOperator(
  task_id = 'check_if_weekday',
  python_callable = get_run_id_date,
  provide_context = True,
  dag=dag
)

check_if_weekday >> [ weekday_group, saturday_group, sunday_group ] >> outcond >> notify_success
[ weekday_group, saturday_group, sunday_group, outcond ] >> notify_failure
  
