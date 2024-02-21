import boto3
import argparse

def copy_all_ip_sets(source_region, destination_region, source_scope, destination_scope):
    source_waf_client = boto3.client('wafv2', region_name=source_region)
    destination_waf_client = boto3.client('wafv2', region_name=destination_region)

    # 获取源区域的所有 IP 集合
    source_ip_sets = source_waf_client.list_ip_sets(Scope=source_scope)['IPSets']

    for source_ip_set in source_ip_sets:
        source_ip_set_name = source_ip_set['Name']
        source_ip_set_id = source_ip_set['Id']

        # 获取源 IP 集合的详细信息
        source_ip_set_details = source_waf_client.get_ip_set(Name=source_ip_set_name, Id=source_ip_set_id, Scope=source_scope)
        ip_addresses = source_ip_set_details['IPSet']['Addresses']
        ip_address_version = source_ip_set_details['IPSet']['IPAddressVersion']

        # 检查目标区域是否已存在同名的 IP 集合
        destination_ip_sets = destination_waf_client.list_ip_sets(Scope=destination_scope)['IPSets']
        destination_ip_set = next((ip_set for ip_set in destination_ip_sets if ip_set['Name'] == source_ip_set_name), None)
        

        if destination_ip_set:
            # 如果目标 IP 集合已存在，更新它
            destination_ip_set_id = destination_ip_set['Id']
            lock_token = destination_waf_client.get_ip_set(Name=source_ip_set_name, Id=destination_ip_set_id, Scope=destination_scope)['LockToken']
            destination_waf_client.update_ip_set(
                Name=source_ip_set_name,
                Id=destination_ip_set_id,
                Scope=destination_scope,
                Addresses=ip_addresses,
                LockToken=lock_token
            )
            print(f"IP 集合 '{source_ip_set_name}' 已在 {destination_region} 中更新。")
        else:
            # 如果目标 IP 集合不存在，则创建它
            destination_waf_client.create_ip_set(
                Name=source_ip_set_name,
                Scope=destination_scope,
                IPAddressVersion=ip_address_version,  # 根据实际情况选择 'IPV4' 或 'IPV6'
                Addresses=ip_addresses,
                Description=f"Copied from {source_region}"
            )
            print(f"IP 集合 '{source_ip_set_name}' 已在 {destination_region} 中创建。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="将一个区域的所有 AWS WAFv2 IP 集合复制到另一个区域")
    parser.add_argument("--sr", required=True, help="源区域")
    parser.add_argument("--dr", required=True, help="目标区域")
    parser.add_argument("--ss", required=True, help="源 Scope ('REGIONAL' 或 'CLOUDFRONT')")
    parser.add_argument("--ds", required=True, help="目标 Scope ('REGIONAL' 或 'CLOUDFRONT')")

    args = parser.parse_args()

    copy_all_ip_sets(args.sr, args.dr, args.ss, args.ds)
