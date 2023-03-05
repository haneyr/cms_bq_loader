import sys
from datetime import datetime
from google.cloud import storage
from google.cloud import batch_v1

def create_container_job(project_id: str, region: str, job_name: str, env_dict: dict, task_count: int) -> batch_v1.Job:

    client = batch_v1.BatchServiceClient()

    # Define what will be done as part of the job.
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = "gcr.io/cms-transparency-project/batch-processor-ubuntu"

    task = batch_v1.TaskSpec()
    task.runnables = [runnable]
    #Pass environment variable dictionary to the task
    task.environment.variables = env_dict


    # We can specify what resources are requested by each task.
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = 2000  # in milliseconds per cpu-second. This means the task requires 2 whole CPUs.
    resources.memory_mib = 4096  # in MiB
    task.compute_resource = resources

    task.max_retry_count = 1
    task.max_run_duration = "14400s"

    # Tasks are grouped inside a job using TaskGroups.
    # Currently, it's possible to have only one task group.
    group = batch_v1.TaskGroup()
    group.parallelism = 8
    group.task_count = task_count
    group.task_spec = task
    policy = batch_v1.AllocationPolicy.InstancePolicy()
    policy.machine_type = "e2-standard-4"
    instances = batch_v1.AllocationPolicy.InstancePolicyOrTemplate()
    instances.policy = policy
    allocation_policy = batch_v1.AllocationPolicy()
    allocation_policy.instances = [instances]

    job = batch_v1.Job()
    job.task_groups = [group]
    job.allocation_policy = allocation_policy
    job.labels = {"env": "testing", "type": "container"}
    job.logs_policy = batch_v1.LogsPolicy()
    job.logs_policy.destination = batch_v1.LogsPolicy.Destination.CLOUD_LOGGING

    create_request = batch_v1.CreateJobRequest()
    create_request.job = job
    create_request.job_id = job_name
    create_request.parent = f"projects/{project_id}/locations/{region}"

    return client.create_job(create_request)

def download_blob(bucket_name, blob_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    contents = blob.download_as_string()
    return contents

def create_env_vars(contents):
    i = 0
    env_vars = {} 
    for line in contents.splitlines():
        url = line.decode()
        env_vars[f"URL{i}"] = url
        i += 1
    return env_vars, i

def main(argv):
    project_id = "cms-transparency-project"
    region = "us-central1"
    now = datetime.now()
    bucket_name = "cms-json-trigger-bucket"
    blob_name = sys.argv[1]
    contents = download_blob(bucket_name,blob_name) 
    env_dict, task_count = create_env_vars(contents)
    job_name = "job"+now.strftime("%H-%M-%S-%f")
    create_container_job(project_id, region, job_name, env_dict, task_count)
    print(f"Batch job {job_name} created from {blob_name} file list")

if __name__ == "__main__":
   main(sys.argv[1:])
