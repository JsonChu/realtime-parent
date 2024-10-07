"""
n3
2.使用pip安装依赖包
pip install pyVmomi
pip install pyVim
"""

import sys
from datetime import datetime, timedelta
from pyVmomi import vim
from pyVim.connect import SmartConnect, Disconnect
import re
# import ssl

# 与vCenter Server建立连接
# si = SmartConnect(host="10.173.28.241", user="administrator@vsphere.local", pwd="Iop[]=-09*", port=443,
#                   disableSslCertValidation=True)

# 获取虚拟机详情
def get_vm_info(host, user, pwd):
    """
    :param host:
    :param user:
    :param pwd:
    :return:
    """
    # vm_info_list = []
    # 与vCenter Server建立连接
    try:
        si = SmartConnect(host=host, user=user, pwd=pwd, port=443,
                          disableSslCertValidation=True)

        content = si.RetrieveContent()
        container = content.viewManager.CreateContainerView(content.rootFolder, [vim.VirtualMachine], True)
        for vm in container.view:
 
            create_time = vm.config.createDate if vm.config is not None and vm.config.createDate is not None else 0
            uuid = vm.config.uuid if vm.config is not None and vm.config.uuid is not None else ""
            guest_info = vm.guest
            guest_id = guest_info.guestId if guest_info is not None and guest_info.guestId is not None else ""
            guest_family = guest_info.guestFamily if guest_info is not None and guest_info.guestFamily is not None else ""
            guest_full_name = guest_info.guestFullName if guest_info is not None and guest_info.guestFullName is not None else ""
            guest_host_name = guest_info.hostName if guest_info is not None and guest_info.hostName is not None else ""
            guest_ip_address = guest_info.ipAddress if guest_info is not None and guest_info.ipAddress is not None else ""

            dns_ip_addresses = []
            if guest_info is not None and guest_info.net is not None:
                for nic_info in guest_info.net:
                    if nic_info.ipAddress is not None:
                        dns_ip_addresses.extend(nic_info.ipAddress)
            dns_ip_address_str = ",".join(dns_ip_addresses) if dns_ip_addresses is not None else ""

            # Extracting numCpu and memorySizeMB from vm.summary.config
            num_cpu = vm.summary.config.numCpu if vm.summary.config is not None and vm.summary.config.numCpu is not None else 0
            memory_size_mb = vm.summary.config.memorySizeMB if vm.summary.config is not None and vm.summary.config.memorySizeMB is not None else 0

            # Extracting storage information from vm.summary.storage
            committed_b = vm.summary.storage.committed if vm.summary.storage is not None else 0
            uncommitted_b = vm.summary.storage.uncommitted if vm.summary.storage is not None else 0
            unshared_b = vm.summary.storage.unshared if vm.summary.storage is not None else 0
            # Extracting datastore information
            datastore_ids = get_str_store(vm.datastore)


            # Extracting disk information from vm.guest.disk
            total_capacity_b = sum(
                (disk_info.capacity if disk_info.capacity is not None else 0)
                for disk_info in guest_info.disk
            )
            total_free_space_b = sum(
                (disk_info.freeSpace  if disk_info.freeSpace is not None else 0)
                for disk_info in guest_info.disk
            )
            #capacity_b_detail = [disk_info.capacity if disk_info.capacity is not None else 0 for disk_info in guest_info.disk]
            #free_space_b_detail = [disk_info.freeSpace  if disk_info.freeSpace is not None else 0 for disk_info in guest_info.disk]
            #disk_paths = [disk_info.diskPath for disk_info in guest_info.disk]
            # disk_paths_str = "-".join(disk_paths) if disk_paths else "N/A"

            # Extracting screen information from vm.guest.screen
            screen_width = guest_info.screen.width if guest_info.screen is not None and guest_info.screen.width is not None else 0
            screen_height = guest_info.screen.height if guest_info.screen is not None and guest_info.screen.height is not None else 0

            # Extracting summary information
            power_state = vm.summary.runtime.powerState if vm.summary.runtime is not None else ""
            connection_state = vm.summary.runtime.connectionState if vm.summary.runtime is not None else ""
            max_cpu_usage = vm.summary.runtime.maxCpuUsage if vm.summary.runtime is not None and vm.summary.runtime.maxCpuUsage is not None else 0
            max_memory_usage = vm.summary.runtime.maxMemoryUsage if vm.summary.runtime is not None and vm.summary.runtime.maxMemoryUsage is not None else 0
            boot_time = vm.summary.runtime.bootTime if vm.summary.runtime is not None else 0
            num_mks_connections = vm.summary.runtime.numMksConnections if vm.summary.runtime is not None else 0

            # Convert power state to desired format
            power_state = "POWERED_ON" if power_state == "poweredOn" else "POWERED_OFF"

            # Extracting host information
            host_value = ""
            if vm.runtime is not None:
                split_result = str(vm.runtime.host).split('vim.HostSystem:')
                if len(split_result) > 1:
                    host_value = split_result[1].strip("'")
           
            # Extracting config information
            change_version = vm.config.changeVersion if vm.config is not None else 0
            # Add double quotes around string values

            uuid = f'"{uuid}"'           
            vm_name = f'"{vm.name}"'
            guest_id = f'"{guest_id}"'
            guest_family = f'"{guest_family}"'
            guest_full_name = f'"{guest_full_name}"'
            guest_host_name = f'"{guest_host_name}"'
            guest_ip_address = f'"{guest_ip_address}"'
            dns_ip_address_str = f'"{dns_ip_address_str}"'
            power_state = f'"{power_state}"'
            connection_state = f'"{connection_state}"'
            #host_value = f'"{host_value}"'
            datastore_ids = f'"{datastore_ids}"'

            print(f"python_sdk_vcenter,vcenter={host},moid={vm._moId} "
                  f"uuid={uuid},vm_name={vm_name},create_time={convert_timestamp(create_time)},guest_id={guest_id},"
                  f"guest_family={guest_family},guest_full_name={guest_full_name},"
                  f"guest_host_name={guest_host_name},guest_ip_address={guest_ip_address},"
                  f"guest_dns_ip_addresses={dns_ip_address_str},num_cpu={num_cpu},memory_size_mb={memory_size_mb},"
                  f"storage_committed_b={committed_b},storage_uncommitted_b={uncommitted_b},storage_unshared_b={unshared_b},"
                  f"total_capacity_b={total_capacity_b},total_free_space_b={total_free_space_b},"
                  f"screen_width={screen_width},screen_height={screen_height},"
                  f"power_state={power_state},connection_state={connection_state},"
                  f"max_cpu_usage={max_cpu_usage},max_memory_usage={max_memory_usage},"
                  f"boot_time={convert_timestamp(boot_time)},num_mks_connections={num_mks_connections},"
                  f"change_version_date={convert_timestamp(change_version)},datastore_ids={datastore_ids}")

        # 断开与vCenter Server的连接
        Disconnect(si)
    except Exception as e:
        # return vm_info_list
        raise  # 将异常重新引发，以便上层捕获和输出

    return 0

def get_str_store(datastore_info):

    temp_str_list = []
    for datas in datastore_info:
        temp_str = re.findall('vim.Datastore:(\w+-\d+)',str(datas))
        temp_str_list.append(temp_str[0])

    retuen_str = ",".join(temp_str_list)
    return retuen_str


def convert_timestamp(input_value):
    if input_value is None:
        return 0
    elif isinstance(input_value, str):
        try:
            # 直接将字符串转换为 datetime 对象
            date_time_obj = datetime.strptime(input_value, "%Y-%m-%dT%H:%M:%S.%fZ")
        except ValueError:
            return 0
    elif isinstance(input_value, datetime):
        date_time_obj = input_value
    else:
        return 0

    # 添加8小时
    # new_date_time_obj = date_time_obj + timedelta(hours=8)

    # 将 datetime 对象转换为毫秒时间戳
    timestamp_ms = int(date_time_obj.timestamp() * 1000)

    return timestamp_ms

def usage():
    print(f"please use {sys.argv[0]} <host> <username> <password> for example")
    print(f"{sys.argv[0]} 10.173.28.241 administrator@vsphere.local Iop[]=-09*")
    sys.exit(1)


if __name__ == '__main__':
    if len(sys.argv) < 4:
        usage()
    else:
        get_vm_info(sys.argv[1], sys.argv[2], sys.argv[3])
        sys.exit(0)
