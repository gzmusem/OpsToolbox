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
from datetime import datetime



def parse_log_line(line):
    pattern = re.compile(r'([^ ]*) ([^ ]*) ([^ ]*) ([^ ]*):([0-9]*) ([^ ]*)[:-]([0-9]*) ([-.0-9]*) ([-.0-9]*) ([-.0-9]*) (|[-0-9]*) (-|[-0-9]*) ([-0-9]*) ([-0-9]*) "([^ ]*) (.*) (- |[^ ]*)" "([^"]*)" ([A-Z0-9-_]+) ([A-Za-z0-9.-]*) ([^ ]*) "([^"]*)" "([^"]*)" "([^"]*)" ([-.0-9]*) ([^ ]*) "([^"]*)" "([^"]*)" "([^ ]*)" "([^\s]+?)" "([^\s]+)" "([^ ]*)" "([^ ]*)"')
    match = pattern.match(line)
    
    if match:
        data = {
            "type": match.group(1),
            "timestamp": match.group(2),
            "elb": match.group(3),
            "client_ip": match.group(4),
            "client_port": match.group(5),
            "target_ip": match.group(6),
            "target_port": match.group(7),
            "request_processing_time": match.group(8),
            "target_processing_time": match.group(9),
            "response_processing_time": match.group(10),
            "elb_status_code": match.group(11),
            "target_status_code": match.group(12),
            "received_bytes": match.group(13),
            "sent_bytes": match.group(14),
            "request_verb": match.group(15),
            "request_url": match.group(16),
            "request_proto": match.group(17),
            "user_agent": match.group(18),
            "ssl_cipher": match.group(19),
            "ssl_protocol": match.group(20),
            "target_group_arn": match.group(21),
            "trace_id": match.group(22),
            "domain_name": match.group(23),
            "chosen_cert_arn": match.group(24),
            "matched_rule_priority": match.group(25),
            "request_creation_time": match.group(26),
            "actions_executed": match.group(27),
            "redirect_url": match.group(28),
            "lambda_error_reason": match.group(29),
            "target_port_list": match.group(30),
            "target_status_code_list": match.group(31),
            "classification": match.group(32),
            "classification_reason": match.group(33)
        }
        return data
    else:
        return None

def create_index_with_date(es, base_index_name):
    # 获取当前日期
    current_date = datetime.now().strftime("%Y.%m.%d")
    
    # 构建带有日期的索引名称
    index_name = f"{base_index_name}-{current_date}"
    
    # 创建索引
    es.indices.create(index=index_name)
    
    return index_name


def get_earliest_alb_log_date(bucket_name, prefix):
    # 创建 S3 客户端，不传递 AWS 访问密钥和密钥
    s3_client = boto3.client('s3')

    # 列出对象
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

    # 获取列表中的对象，并按创建时间排序
    objects = response.get('Contents', [])
    objects.sort(key=lambda x: x['LastModified'])  # 按创建时间排序

    # 获取排序后列表中的第一个对象
    first_object = objects[0] if objects else None
    # 提取日期信息并找到最早的日期
    earliest_date = None

    if first_object:
        key = first_object['Key']

        print(key)

        # 修改这里的提取方式，确保日期在正确的位置
        yy = key.split('/')[5]  # 提取日期部分
        mm = key.split('/')[6]
        dd = key.split('/')[7]
        date_components = yy + "-" + mm + "-" + dd  # 拆分年、月、日


        # 构造日期字符串，确保年份有四位
        date_obj = None

        # 调整日期字符串构造逻辑
        date_obj = datetime.strptime(date_components, '%Y-%m-%d')
        print(date_obj)

        if earliest_date is None or date_obj < earliest_date:
            earliest_date = date_obj

    return earliest_date



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

        # 构建索引名称
    index_name = f"{es_index}-"

    # 检查是否存在以base_index_name开头的索引
    existing_indices = [index for index in es.indices.get('*') if index.startswith(es_index)]
    
    if existing_indices:
        # 存在索引，查询最后的日志时间
        last_log_time = get_last_log_time(es, existing_indices[-1])
        if last_log_time:
            print(f"Last log time in index '{existing_indices[-1]}': {last_log_time}")
        else:
            print("Error: Unable to get last log time.")
    else:
        # 不存在索引，从S3查询最早的开始时间
        last_log_time = get_earliest_alb_log_date(bucket_name,f"{base_prefix}/{log_path_prefix}/")
        if last_log_time:
            # 根据最早时间创建索引
            index_name += last_log_time.strftime("%Y.%m.%d")
            create_index_with_date(es, index_name)
        else:
            print("Error: Unable to get earliest log time from S3.")


    current_year = last_log_time.year
    current_month = last_log_time.month
    month_prefix = f"{base_prefix}/{log_path_prefix}/{current_year}/{str(current_month).zfill(2)}/"


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




