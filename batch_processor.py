import os
import ijson
import requests
import gzip
import shutil

from google.cloud import storage
from google.cloud import bigquery
from time import sleep
from threading import Thread
from queue import Queue
from urllib.parse import urlparse

def file_download(file_url,gcsfs_path,downloadKey,filename):
    response = requests.get(url=file_url,allow_redirects=True,).content
    with open(f"{gcsfs_path}/{filename}",'wb') as out:
        out.write(response)

def gunzip_file(gcsfs_path,filename):
    with gzip.open(f"{gcsfs_path}/{filename}",'rb') as f_in:
        output_file = os.path.splitext(filename)[0]
        with open(f"{gcsfs_path}/{output_file}",'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    return f"{gcsfs_path}/{output_file}"

def parse_filename(url):
    parsedUrl = urlparse(url)
    return os.path.basename(parsedUrl.path)

def load_json_to_bq(table_id, bucket_uri):
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
    autodetect=True, source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    )
    uri = f"gs://{bucket_uri}*.json"
    load_job = client.load_table_from_uri(
    uri, table_id, job_config=job_config
    )  # Make an API request.
    load_job.result()  # Waits for the job to complete.
    destination_table = client.get_table(table_id)
    print("Loaded {} rows.".format(destination_table.num_rows))

def producer(queue, source_file_name):
    record_buffer = []
    json_target_items = 'in_network.item'
    counter = 0
    buffer_size = 25
    file_part_index = 1
    file_input_buffer = 1024000 #10737418240 #10737418240 #1024000 209715200
    
    print('setting backend')
    backend = ijson.get_backend('yajl2_c')
    print("opening file")
    with open(source_file_name, 'rb') as data:
        print('starting to iterate items')
        for obj in backend.items(data, json_target_items,use_float=True,buf_size=file_input_buffer):
            counter +=  1
            record_buffer.append(str(obj))

            if counter == buffer_size:
                print(f"adding record array to queue.")
                queue.put((file_part_index, record_buffer))
                print("record array added to queue")
                record_buffer = []
                file_part_index += 1
                counter = 0

    if counter > 0:
        print('Flushing remaining buffer records.')
        queue.put((file_part_index, record_buffer))
        print('remaining buffer records added to queue. All items in file have been added.')
    
    queue.put(None)
    print('Producer: Done')

def consumer(queue, output_path, target_file_name, worker_id, bucket_name):
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    while True:
        chunk = queue.get()
        if chunk is None:
            print(f'Queue is empty. Shutting worker {worker_id} down...')
            #no more items add signal back for other consumers
            queue.put(None)
            break
        print(f'Worker {worker_id} writing file: {chunk[0]}')
        
        if (len(chunk[1]) == 0):
            print('chunk size was zero. no idea why. skipping write...')
            queue.task_done()
            continue
            
        blob = bucket.blob(f'{output_path}{target_file_name}_{chunk[0]}.json')
        #try:
        blob.upload_from_string("\n".join(chunk[1]),timeout=3600,)          
        #except:
        #with open(f'{target_file_name}_{chunk[0]}.json', 'wt', encoding='utf-8') as targetFile:
        #        for line in chunk[1]:
        #            targetFile.write(f"{line}\n")
        queue.task_done()
        print(f'chunk processed by worker {worker_id}. File Created: {output_path}{target_file_name}_{chunk[0]}.json')

def main():
    #file_url = "https://uhc-tic-mrf.azureedge.net/public-mrf/2023-02-01/2023-02-01_ALL-SAVERS-INSURANCE-COMPANY_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json.gz"
    #Get the URL env var passed to the batch job
    file_url = os.environ.get('URL')
    filename = parse_filename(file_url)
    tablename = os.path.splitext(os.path.splitext(filename)[0])[0]
    gcsfs_path = "/mnt/disks/bucket/"
    project_id = "cms-transparency-project"
    region = "us-central1"
    bq_dataset = "my_dataset"
    bucket_name = "cms-json-trigger-bucket"
    downloadKey = f"{tablename}/downloaded"
    num_file_writers = 20 #  5for small 70 for big
    queue_size = 1500 # -1 for unlimited
    queue = Queue(queue_size)
    queue_saturation_time = 200  # in seconds 60 for small 1800 for big
    input_filename = '/mnt/disks/bucket/2023-02-01_ALL-SAVERS-INSURANCE-COMPANY_Insurer_PPO---NDC_PPO-NDC_in-network-rates.json'
    output_path = f"{tablename}/load_to_bq/"
    output_filename = "in-network-rates" 
    print(f"Streaming {filename} from {file_url} to {downloadKey}")
    file_download(file_url,gcsfs_path,downloadKey,filename)
    output_file = gunzip_file(gcsfs_path,filename)
    print('Starting file splitter...')    
    p = Thread(target=producer, args=(queue,input_filename,))
    p.start()
    
    print(f'Pausing for {queue_saturation_time/60} minutes to allow queue to saturate...')
    sleep(queue_saturation_time)
    print(f'Starting {num_file_writers} file writers... ')
    workers = [Thread(target=consumer, args=(queue,output_path,output_filename,i,bucket_name)) for i in range(num_file_writers)]
    for w in workers:
        w.start()
        

    p.join()
    
    for w in workers:
        w.join()
    load_json_to_bq(f"{project_id}.{bq_dataset}.{tablename}",f"{bucket_name}/{output_path}")
main()
