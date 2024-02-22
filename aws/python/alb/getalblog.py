"""
使用说明:

本脚本用于从指定的S3存储桶中获取Application Load Balancer(ALB)的日志，并将这些日志存储到Elasticsearch中。
在运行此脚本之前，请确保您已经配置好AWS CLI，并且您的AWS账户有权限访问S3和Elasticsearch服务。

运行环境要求:
- Python 3.8+
- 需要安装的库: boto3, argparse, elasticsearch

参数说明:
- bucket_name: 存储ALB日志的S3存储桶名称
- base_prefix: S3存储桶中ALB日志的基础前缀
- es_host: Elasticsearch服务的主机地址
- es_index: 存储日志的Elasticsearch索引名称
- es_user: 访问Elasticsearch服务的用户名
- es_pass: 访问Elasticsearch服务的密码

使用示例:
python getalblog.py --bucket_name 'your_bucket_name' --base_prefix 'your_base_prefix' --es_host 'your_es_host' --es_index 'your_es_index' --es_user 'your_es_user' --es_pass 'your_es_pass'

注意:
- 请确保Elasticsearch索引已经创建，并且其映射与ALB日志的格式相匹配。
- 此脚本默认处理当前月份的日志。如果需要处理其他月份的日志，请在base_prefix中指定相应的年份和月份。
"""


import boto3
import argparse
import gzip
from io import BytesIO
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers


def ensure_index_exists(es, es_index):
    """
    确保Elasticsearch索引存在，如果不存在则创建。
    """
    # 检查索引是否存在
    if not es.indices.exists(index=es_index):
        # 定义索引的映射
        index_mappings = {
            "mappings": {
                "properties": {
                    "log": {
                        "properties": {
                            "timestamp": {"type": "date"},
                            "elb_status_code": {"type": "keyword"},
                            "target_status_code": {"type": "keyword"},
                            "request_processing_time": {"type": "float"},
                            "target_processing_time": {"type": "float"},
                            "response_processing_time": {"type": "float"},
                            "elb_request_method": {"type": "keyword"},
                            "request_url": {"type": "text"},
                            "request_protocol": {"type": "keyword"},
                            "user_agent": {"type": "text"},
                            "ssl_cipher": {"type": "keyword"},
                            "ssl_protocol": {"type": "keyword"},
                            "target_group_arn": {"type": "keyword"},
                            "trace_id": {"type": "keyword"},
                            "domain_name": {"type": "keyword"},
                            "chosen_cert_arn": {"type": "keyword"},
                            "matched_rule_priority": {"type": "keyword"},
                            "request_creation_time": {"type": "date"},
                            "actions_executed": {"type": "keyword"},
                            "redirect_url": {"type": "text"},
                            "error_reason": {"type": "text"}
                        }
                    }
                }
            }
        }
        # 创建索引
        es.indices.create(index=es_index, body=index_mappings)
        print(f"索引 '{es_index}' 已创建。")
    else:
        print(f"索引 '{es_index}' 已存在。")


def get_last_log_time(es, es_index):
    try:
        # 调整为直接使用参数，而不是 'body'
        result = es.search(
            index=es_index,
            size=1,
            sort=[{"log.timestamp": {"order": "desc"}}],
            _source=["log.timestamp"]
        )
        if result['hits']['hits']:
            last_log_time_str = result['hits']['hits'][0]['_source']['log']['timestamp']
            return datetime.strptime(last_log_time_str, '%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        print(f"获取最后日志时间出错: {e}")
    return None

def get_alb_logs(bucket_name, base_prefix, es_host, es_index, es_user, es_pass):
    s3_client = boto3.client('s3')
    es = Elasticsearch([es_host], http_auth=(es_user, es_pass))

    ensure_index_exists( es, es_index )

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




