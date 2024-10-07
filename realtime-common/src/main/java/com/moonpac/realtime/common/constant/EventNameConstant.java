package com.moonpac.realtime.common.constant;

import java.util.Arrays;
import java.util.List;

public class EventNameConstant {

    public static final String PYTHON_VM = "python_sdk_vcenter";

    // vm events
    public static final String VSPHERE_VM_CPU = "vsphere_vm_cpu";
    public static final String VSPHERE_VM_DATASTORE = "vsphere_vm_datastore";
    public static final String VSPHERE_VM_DISK = "vsphere_vm_disk";
    public static final String VSPHERE_VM_MEM = "vsphere_vm_mem";
    public static final String VSPHERE_VM_NET = "vsphere_vm_net";
    public static final String VSPHERE_VM_SYS = "vsphere_vm_sys";

    public static final List<String> VM_TABLES = Arrays.asList(
            VSPHERE_VM_CPU, VSPHERE_VM_DATASTORE, VSPHERE_VM_DISK, VSPHERE_VM_MEM,VSPHERE_VM_NET,VSPHERE_VM_SYS
    );

    // host events
    public static final String VSPHERE_HOST_CPU = "vsphere_host_cpu";
    public static final String VSPHERE_HOST_DATASTORE = "vsphere_host_datastore";
    public static final String VSPHERE_HOST_DISK = "vsphere_host_disk";
    public static final String VSPHERE_HOST_MEM = "vsphere_host_mem";
    public static final String VSPHERE_HOST_NET = "vsphere_host_net";

    public static final List<String> HOST_TABLES = Arrays.asList(
            VSPHERE_HOST_CPU, VSPHERE_HOST_DATASTORE, VSPHERE_HOST_DISK,VSPHERE_HOST_MEM,VSPHERE_HOST_NET
    );

    // datastore
    public static final String VSPHERE_DATASTORE_DATASTORE = "vsphere_datastore_datastore";
    public static final String VSPHERE_DATASTORE_DISK = "vsphere_datastore_disk";


}
