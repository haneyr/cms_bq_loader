import sys
from datetime import datetime
import random
from google.cloud import storage
from google.cloud import batch_v1

def create_container_job(project_id: str, region: str, job_name: str, url) -> batch_v1.Job:

    client = batch_v1.BatchServiceClient()

    # Define what will be done as part of the job.
    runnable = batch_v1.Runnable()
    runnable.container = batch_v1.Runnable.Container()
    runnable.container.image_uri = "gcr.io/google-containers/busybox"
    runnable.container.entrypoint = "/bin/sh"
    runnable.container.commands = ["-c", "echo My download url is ${URL}"]

    # Jobs can be divided into tasks. In this case, we have only one task.
    task = batch_v1.TaskSpec()
    task.runnables = [runnable]
    #Pass environment variables to the task
    task.environment.variables = {"URL":f"{url}"}

    # We can specify what resources are requested by each task.
    resources = batch_v1.ComputeResource()
    resources.cpu_milli = 2000  # in milliseconds per cpu-second. This means the task requires 2 whole CPUs.
    resources.memory_mib = 1024  # in MiB
    task.compute_resource = resources

    task.max_retry_count = 2
    task.max_run_duration = "3600s"

    # Tasks are grouped inside a job using TaskGroups.
    # Currently, it's possible to have only one task group.
    group = batch_v1.TaskGroup()
    group.task_count = 2
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

def main(argv):
    project_id = "cms-transparency-project"
    region = "us-central1"
    now = datetime.now()
    bucket_name = "cms-json-trigger-bucket"
    blob_name = sys.argv[1]
    print(blob_name)
    contents = download_blob(bucket_name,blob_name) 
    for line in contents.splitlines():
        random_num = random.randint(1,1000)
        job_name = "job"+"-"+str(random_num)+"-"+now.strftime("%H-%M-%S-%f")
        url = line.decode()
        create_container_job(project_id, region, job_name, url)

if __name__ == "__main__":
   main(sys.argv[1:])
