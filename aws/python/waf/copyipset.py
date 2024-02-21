import argparse
import boto3

def copy_ip_set(sr, dr, sn, dn,ss,ds):
    # 创建 AWS WAF 客户端
    source_waf_client = boto3.client('wafv2', region_name=sr)
    destination_waf_client = boto3.client('wafv2', region_name=dr)

    # 获取源 IP Set 的 ID 和详细信息
    response = source_waf_client.list_ip_sets(Scope=ss)
    source_ip_set_id = None
    for ip_set_summary in response['IPSets']:
        if ip_set_summary['Name'] == sn:
            source_ip_set_id = ip_set_summary['Id']
            break

    if not source_ip_set_id:
        print(f"源 IP 集合 '{sn}' 在 {sr} 中未找到。")
        return

    response = source_waf_client.get_ip_set(Name=sn,Id=source_ip_set_id,Scope=ss)
    source_ip_set_details = response['IPSet']
    ip_addresses = source_ip_set_details['Addresses']
    ip_address_version = source_ip_set_details['IPAddressVersion']

    # 检查目标区域是否已存在目标 IP Set
    response = destination_waf_client.list_ip_sets(Scope=ds)
    destination_ip_set_id = None
    for ip_set_summary in response['IPSets']:
        if ip_set_summary['Name'] == dn:
            destination_ip_set_id = ip_set_summary['Id']
            break

    # 如果目标 IP Set 不存在，则创建它
    if not destination_ip_set_id:
        response = destination_waf_client.create_ip_set(
            Name=dn,
            Scope=ds,  # 或者 'CLOUDFRONT'，根据目标区域的需求
            IPAddressVersion=ip_address_version,
            Addresses=[],  # 最初为空，稍后更新
            Description='copy from  ' + sn  # 可选描述
        )
        destination_ip_set_id = response['Summary']['Id']

    # 获取 LockToken
    response = destination_waf_client.get_ip_set(Name=dn,Id=destination_ip_set_id,Scope='REGIONAL')
    lock_token = response['LockToken']

    # 将 IP 列表添加到目标 IP Set
    destination_waf_client.update_ip_set(
        Name=dn,
        Id=destination_ip_set_id,
        Scope=ds,  # 或者 'CLOUDFRONT'，根据目标区域的需求
        Addresses=ip_addresses,
        LockToken=lock_token
    )
    print(f"IP 集合 '{sn}' 已从 {sr} 复制到 {dr}，新集合名为 '{dn}'。")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="跨区域复制 AWS WAFv2 IP 集合")
    parser.add_argument("--sr", required=True, help="源区域")
    parser.add_argument("--dr", required=True, help="目标区域")
    parser.add_argument("--sn", required=True, help="源 IP 集合名称")
    parser.add_argument("--dn", required=True, help="目标 IP 集合名称")
    parser.add_argument("--ss", required=True, help="源 Scope ('REGIONAL' 或 'CLOUDFRONT')")
    parser.add_argument("--ds", required=True, help="目标 Scope ('REGIONAL' 或 'CLOUDFRONT')")

    args = parser.parse_args()

    copy_ip_set(args.sr, args.dr, args.sn, args.dn, args.ss, args.ds)
