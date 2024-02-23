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
import re
import json
from io import BytesIO
from datetime import datetime, timedelta
from elasticsearch import Elasticsearch, helpers



import re

def parse_log_line(line):
    pattern = re.compile(r'([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) "([^ ]*) (.*) (- |[^ ]*)" "([^"]*)" ([A-Z0-9-_]+) ([A-Za-z0-9.-]*) ([^ ]*) "([^"]*)" "([^"]*)" "([^"]*)" ([-.0-9]*) ([^ ]*) "([^"]*)" "([^"]*)" "([^ ]*)" "([^\s]+?)" "([^\s]+)" "([^ ]*)" "([^ ]*)"')
    match = pattern.match(line)
    
    if match:
        data = {
            "type":match.group(1),
            "timestamp": match.group(2),
            "app": match.group(3) + ":" + match.group(4),
            "client": match.group(5)+ ":" + match.group(6),
            "target": match.group(7),
            "request_processing_time": match.group(8),
            "target_processing_time": match.group(9),
            "response_processing_time": match.group(10),
            "elb_status_code": match.group(11),
            "target_status_code": match.group(12),
            "received_bytes": match.group(13),
            "sent_bytes": match.group(14),
            "request": match.group(15),
            "user_agent": match.group(16),
            "ssl_cipher": match.group(17),
            "ssl_protocol": match.group(18),
            "target_group_arn": match.group(19),
            "trace_id": match.group(20),
            "domain": match.group(21),
            "certificate_arn": match.group(22),
            "forwarded": match.group(23),
            "redirect_url": match.group(24),
            "server_ip": match.group(25),
            "server_port": match.group(26),
            "protocol": match.group(27),
        }
        return data
    else:
        return None


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
                        "type": "text"  # 确保log字段被映射为text类型
                    },
                    "timestamp": {
                        "type": "date"  # 确保timestamp字段被映射为date类型
                    }
                    # 根据需要添加其他字段的映射
                }
            }
        }
        # 创建索引，直接传递映射参数
        es.indices.create(index=es_index, mappings=index_mappings["mappings"])
        print(f"索引 '{es_index}' 已创建。")
    else:
        print(f"索引 '{es_index}' 已存在。")


def get_last_log_time(es, es_index):
    try:
        # 调整为直接使用参数，而不是 'body'
        result = es.search(
            index=es_index,
            size=1,
            sort=[{"timestamp": {"order": "desc"}}],  # 直接使用 "timestamp"
            _source=["timestamp"]
        )
        if result['hits']['hits']:
            last_log_time_str = result['hits']['hits'][0]['_source']['log']['timestamp']
            return datetime.strptime(last_log_time_str, '%Y-%m-%dT%H:%M:%S')
    except Exception as e:
        print(f"获取最后日志时间出错: {e}")
    return None

def get_alb_logs(bucket_name, base_prefix, log_path_prefix, es_host, es_index, es_user, es_pass):
    s3_client = boto3.client('s3')
    es = Elasticsearch([es_host], http_auth=(es_user, es_pass))

    ensure_index_exists(es, es_index)

    current_year = datetime.now().year
    current_month = datetime.now().month
    month_prefix = f"{base_prefix}/{log_path_prefix}/{current_year}/{str(current_month).zfill(2)}/"

    last_log_time = get_last_log_time(es, es_index)
    if last_log_time:
        print(f"Last log time: {last_log_time}")
    else:
        print("No previous logs found. Starting from the beginning of the month.")

    paginator = s3_client.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=month_prefix)

    actions = []  # 准备一个列表来保存 Elasticsearch 的批量操作

    for page in page_iterator:
        if "Contents" in page:
            for obj in page['Contents']:
                log_file = obj['Key']
                print(f"Processing ALB log: {log_file}")
                obj_data = s3_client.get_object(Bucket=bucket_name, Key=log_file)
                with gzip.GzipFile(fileobj=BytesIO(obj_data['Body'].read())) as gzipfile:
                    for line in gzipfile:
                        print(line)
                        log_entry_data = line.decode('utf-8')
                        log_data = parse_log_line(log_entry_data)
                        print(log_data)
                        if log_data:  # 确保日志行被成功解析
                            # 假设原始的 timestamp 是以 '%Y-%m-%dT%H:%M:%S.%fZ' 格式提供的
                            original_timestamp = datetime.strptime(log_data["timestamp"], '%Y-%m-%dT%H:%M:%S.%fZ')
                            # 调整 timestamp 格式为 ISO 8601 格式（或其他所需格式）
                            adjusted_timestamp = original_timestamp.strftime('%Y-%m-%dT%H:%M:%SZ')
                            log_data["timestamp"] = adjusted_timestamp  # 更新字典中的 timestamp
                            
                            # 检查 timestamp 是否晚于最后一条日志的时间
                            if not last_log_time or original_timestamp > last_log_time:
                                log_entry = {
                                    '_index': es_index,
                                    '_source': log_data
                                }
                                actions.append(log_entry)
    
    
    # 将日志批量索引到 Elasticsearch
    if actions:
        helpers.bulk(es, actions)
        print(f"Indexed {len(actions)} logs to Elasticsearch index {es_index}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="从S3中获取ALB日志并存储到Elasticsearch")
    parser.add_argument("--bucket", required=True, help="S3桶名")
    parser.add_argument("--base_prefix", required=True, help="日志文件基础前缀")
    parser.add_argument("--log_path_prefix", required=True, help="日志路径前缀")
    parser.add_argument("--es_host", required=True, help="Elasticsearch主机地址")
    parser.add_argument("--es_index", required=True, help="Elasticsearch索引名")
    parser.add_argument("--es_user", required=True, help="Elasticsearch用户名")
    parser.add_argument("--es_pass", required=True, help="Elasticsearch密码")

    args = parser.parse_args()

    get_alb_logs(args.bucket, args.base_prefix, args.log_path_prefix, args.es_host, args.es_index, args.es_user, args.es_pass)




