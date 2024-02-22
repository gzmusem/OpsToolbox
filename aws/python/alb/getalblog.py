import boto3
import argparse
import gzip
from io import BytesIO
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers

def get_last_log_time(es, es_index):
    try:
        # Query to get the latest log entry
        body = {
            "size": 1,
            "sort": [{"log.timestamp": {"order": "desc"}}],
            "_source": ["log.timestamp"]
        }
        result = es.search(index=es_index, body=body)
        if result['hits']['hits']:
            last_log_time_str = result['hits']['hits'][0]['_source']['log']['timestamp']
            return datetime.strptime(last_log_time_str, '%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        print(f"Error getting last log time: {e}")
    return None

def get_alb_logs(bucket_name, base_prefix, es_host, es_index, es_user, es_pass):
    s3_client = boto3.client('s3')
    es = Elasticsearch([es_host], http_auth=(es_user, es_pass))

    current_year = datetime.now().year
    current_month = datetime.now().month
    month_prefix = f"{base_prefix}{current_year}/{str(current_month).zfill(2)}/"  # Ensure month is zero-padded

    last_log_time = get_last_log_time(es, es_index)
    if last_log_time:
        print(f"Last log time: {last_log_time}")
    else:
        print("No previous logs found. Starting from the beginning of the month.")

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=month_prefix)

    actions = []  # Prepare a list to hold bulk actions for Elasticsearch

    for page in page_iterator:
        if "Contents" in page:
            for obj in page['Contents']:
                log_file = obj['Key']
                print(f"Processing ALB log: {log_file}")
                obj_data = s3_client.get_object(Bucket=bucket_name, Key=log_file)
                with gzip.GzipFile(fileobj=BytesIO(obj_data['Body'].read())) as gzipfile:
                    for line in gzipfile:
                        log_entry_data = line.decode('utf-8')
                        log_timestamp = datetime.strptime(log_entry_data.split()[1], '%Y-%m-%dT%H:%M:%S')
                        if not last_log_time or log_timestamp > last_log_time:
                            log_entry = {'_index': es_index, '_source': {'log': log_entry_data, 'timestamp': log_timestamp.strftime('%Y-%m-%dT%H:%M:%S')}}
                            actions.append(log_entry)
    
    # Bulk index the logs into Elasticsearch
    if actions:
        helpers.bulk(es, actions)
        print(f"Indexed {len(actions)} logs to Elasticsearch index {es_index}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从S3中获取ALB日志并存储到Elasticsearch")
    parser.add_argument("--bucket", required=True, help="S3桶名")
    parser.add_argument("--base_prefix", required=True, help="日志文件基础前缀")
    parser.add_argument("--es_host", required=True, help="Elasticsearch主机地址")
    parser.add_argument("--es_index", required=True, help="Elasticsearch索引名")
    parser.add_argument("--es_user", required=True, help="Elasticsearch用户名")
    parser.add_argument("--es_pass", required=True, help="Elasticsearch密码")

    args = parser.parse_args()

    get_alb_logs(args.bucket, args.base_prefix, args.es_host, args.es_index, args.es_user, args.es_pass)




