from airflow import DAG
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
import pendulum
from kubernetes.client import models as k8s

# --- 1. CẤU HÌNH CONFIGMAP ---
config_volume = k8s.V1Volume(
    name='config-vol',
    config_map=k8s.V1ConfigMapVolumeSource(
        name='app-config',
        items=[k8s.V1KeyToPath(key="app_config.yaml", path="app_config.yaml")]
    )
)

config_mount = k8s.V1VolumeMount(
    name='config-vol',
    mount_path='/app/configs/app_config.yaml',
    sub_path='app_config.yaml',
    read_only=True
)

# --- 2. CẤU HÌNH SECRET ---
secret_env_source = k8s.V1EnvFromSource(
    secret_ref=k8s.V1SecretEnvSource(name="my-db-secret") 
)

# --- 3. CẤU HÌNH DAG ---
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
}

with DAG(
    'flight_full_pipeline',
    default_args=default_args,
    description='Full Flow: Kafka -> MinIO -> Warehouse',
    schedule='0 * * * *', 
    start_date=pendulum.today('UTC').add(days=-1),
    catchup=False,
    tags=['production', 'bigdata'],
) as dag:

    # TASK 1: INGESTION
    ingestion_task = KubernetesPodOperator(
        task_id='ingest_kafka_to_minio',
        name='ingestion-worker',
        namespace='bigdata',
        image='flight-prediction:v3',
        image_pull_policy='Never',
        cmds=["python", "-m", "src.jobs.ingestion_job"],
        
        volumes=[config_volume],
        volume_mounts=[config_mount],
        env_from=[secret_env_source],
        
        # --- SỬA TẠI ĐÂY (resources -> container_resources) ---
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "500Mi", "cpu": "500m"},
            limits={"memory": "1Gi", "cpu": "1000m"}
        ),
        # ------------------------------------------------------

        on_finish_action="delete_pod", 
        get_logs=True
    )

    # TASK 2: ETL
    etl_task = KubernetesPodOperator(
        task_id='etl_minio_transformation',
        name='etl-worker',
        namespace='bigdata',
        image='flight-prediction:v3',
        image_pull_policy='Never',
        cmds=["python", "-m", "src.jobs.etl_job"],
        
        volumes=[config_volume],
        volume_mounts=[config_mount],
        env_from=[secret_env_source],
        
        # --- SỬA TẠI ĐÂY (resources -> container_resources) ---
        container_resources=k8s.V1ResourceRequirements(
            requests={"memory": "500Mi", "cpu": "500m"},
            limits={"memory": "1Gi", "cpu": "1000m"}
        ),
        # ------------------------------------------------------

        on_finish_action="delete_pod",
        get_logs=True
    )

    ingestion_task >> etl_task