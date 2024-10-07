package com.moonpac.realtime.dwd.source;

import com.moonpac.realtime.common.bean.dim.HostInfo;
import com.moonpac.realtime.common.bean.dim.VcenterInfo;
import com.moonpac.realtime.common.bean.dim.DataStoreInfo;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.constant.ObjectTypeConstant;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.*;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.sql.Connection;


/**
 * @author zhanglingxing
 * @date 2024/4/15 11:26
 * 1.读取doris中对应的vcenter的配置 并发为1
 */

/**
 * @author zhanglingxing
 * @date 2024/02/28 13:50
 * 1.读取doris中对应的vcenter的配置 并发为1
 */
@Slf4j
public class DorisDimSource extends RichSourceFunction<VcenterInfo> {

    private final String[] tables;
    public DorisDimSource(String... tables){
        this.tables = tables;
    }
    volatile boolean flag = true;
    private Connection dorisConn;
    private Integer intervalSecond;

    static  final DecimalFormat format = new DecimalFormat("#.0000");
    @Override
    public void open(Configuration parameters) throws Exception {

        // 获取doris配置
        Map<String, String> dorisConf  = getRuntimeContext().getExecutionConfig().getGlobalJobParameters().toMap();

        String username = dorisConf.get("doris.username");
        String password = dorisConf.get("doris.password");
        String url = dorisConf.get("doris.url");
        String driver = dorisConf.get("doris.driver");
        intervalSecond = Integer.parseInt(dorisConf.getOrDefault("doris.query.interval","3600"));
        // 初始化 doris 连接
        Class.forName(driver);
        dorisConn = DriverManager.getConnection(url, username, password);
        log.debug("doris connection successfully created");

    }

    @Override
    public void run(SourceContext<VcenterInfo> out) throws Exception {
        List<String> tableList = Arrays.asList(tables);
        while (flag) {
            String sql;
            Statement stmt = null;
            ResultSet rs = null;
            if (tableList.contains(ObjectTypeConstant.DIM_VM)){
                sql = "select * from vcenter_db.dim_vm_info where is_delete=0";
                //执行sql
                stmt = dorisConn.createStatement();
                rs = stmt.executeQuery(sql);
                // 处理查询结果
                while (rs.next()) {
                    VcenterInfo vcenterInfo = extractedVMInfo(rs);
                    out.collect(vcenterInfo);
                }
            }
            if (tableList.contains(ObjectTypeConstant.DIM_HOST)){
                sql = "select * from vcenter_db.dim_host_info where is_delete=0";
                //执行sql
                stmt = dorisConn.createStatement();
                rs = stmt.executeQuery(sql);
                // 处理查询结果
                while (rs.next()) {
                    VcenterInfo vcenterInfo = extractedHostInfo(rs);
                    out.collect(vcenterInfo);
                }
            }

            if (tableList.contains(ObjectTypeConstant.DIM_DATASTORE)){
                sql = "select * from vcenter_db.dim_vm_datastore_info where is_delete=0";
                //执行sql
                stmt = dorisConn.createStatement();
                rs = stmt.executeQuery(sql);
                // 处理查询结果
                while (rs.next()) {
                    VcenterInfo vcenterInfo = extractedVMDaTaStoreInfo(rs);
                    out.collect(vcenterInfo);
                }
            }

            rs.close();
            stmt.close();
            TimeUnit.SECONDS.sleep(intervalSecond);
        }
    }

    private VcenterInfo extractedVMDaTaStoreInfo(ResultSet rs) throws SQLException {
        VcenterInfo vcenterInfo = new VcenterInfo();
        DataStoreInfo dataStoreInfo = new DataStoreInfo();
        dataStoreInfo.setHttpRegion(rs.getString("region"));
        dataStoreInfo.setHttpVcenter(rs.getString("vcenter"));
        dataStoreInfo.setHttpVmMoid(rs.getString("vm_moid"));
        dataStoreInfo.setPythonDatastoreId(rs.getString("datastore_id"));
        dataStoreInfo.setHttpDatacenterId(rs.getString("datacenter_id"));
        dataStoreInfo.setHttpDatacenterName(rs.getString("datacenter_name"));
        dataStoreInfo.setHttpClusterId(rs.getString("cluster_id"));
        dataStoreInfo.setHttpClusterName(rs.getString("cluster_name"));
        dataStoreInfo.setHttpHostId(rs.getString("host_id"));
        dataStoreInfo.setHttpHostName(rs.getString("host_name"));
        dataStoreInfo.setHttpVmName(rs.getString("vm_name"));
        dataStoreInfo.setHttpEventDate(getDate(rs,"event_date"));
        vcenterInfo.setDataStoreInfo(dataStoreInfo);
        vcenterInfo.setObjectType(ObjectTypeConstant.DIM_DATASTORE);
        vcenterInfo.setRegion(dataStoreInfo.getHttpRegion());
        vcenterInfo.setEventPartitionKey(dataStoreInfo.getHttpVcenter()+"_"+dataStoreInfo.getPythonDatastoreId());
        return vcenterInfo;
    }

    private VcenterInfo extractedHostInfo(ResultSet rs) throws SQLException {
        VcenterInfo vcenterInfo = new VcenterInfo();
        HostInfo hostInfo = new HostInfo();
        hostInfo.setVcenter(rs.getString("vcenter"));
        hostInfo.setHostId(rs.getString("host_id"));
        hostInfo.setRegion(rs.getString("region"));
        hostInfo.setDatacenterId(rs.getString("datacenter_id"));
        hostInfo.setDatacenterName(rs.getString("datacenter_name"));
        hostInfo.setClusterId(rs.getString("cluster_id"));
        hostInfo.setClusterName(rs.getString("cluster_name"));
        hostInfo.setClusterDrsEnabled(rs.getString("cluster_drs_enabled"));
        hostInfo.setClusterHaEnabled(rs.getString("cluster_ha_enabled"));
        hostInfo.setHostName(rs.getString("host_name"));
        hostInfo.setHostConnectionState(rs.getString("connection_state"));
        hostInfo.setHostPowerState(rs.getString("power_state"));
        hostInfo.setCsCpuCoreCount(rs.getInt("cpu_core_total"));
        hostInfo.setCsCpuMhz(rs.getInt("cpu_mhz"));
        hostInfo.setCsMemorySizemb(rs.getInt("memory_size_mb"));
        hostInfo.setHostVMSize(rs.getInt("vm_size"));
        hostInfo.setEventDate(getDate(rs,"event_date"));

        vcenterInfo.setHostInfo(hostInfo);
        vcenterInfo.setObjectType(ObjectTypeConstant.DIM_HOST);
        vcenterInfo.setRegion(hostInfo.getRegion());
        vcenterInfo.setEventPartitionKey(hostInfo.getVcenter()+"_"+hostInfo.getHostId());
        return vcenterInfo;
    }

    private static VcenterInfo extractedVMInfo(ResultSet rs) throws SQLException {
        VcenterInfo vcenterInfo = new VcenterInfo();
        VmInfo vmInfo = new VmInfo();
        vmInfo.setHttpRegion(rs.getString("region"));
        vmInfo.setHttpVcenter(rs.getString("vcenter"));
        vmInfo.setHttpVmMoid(rs.getString("vm_moid"));
        vmInfo.setHttpDatacenterId(rs.getString("datacenter_id"));
        vmInfo.setHttpDatacenterName(rs.getString("datacenter_name"));
        vmInfo.setHttpClusterId(rs.getString("cluster_id"));
        vmInfo.setHttpClusterName(rs.getString("cluster_name"));
        vmInfo.setHttpClusterDrsEnabled(rs.getString("cluster_drs_enabled"));
        vmInfo.setHttpClusterHaEnabled(rs.getString("cluster_ha_enabled"));
        vmInfo.setHttpHostId(rs.getString("host_id"));
        vmInfo.setHttpHostName(rs.getString("host_name"));
        vmInfo.setHttpVmName(rs.getString("vm_name"));
        vmInfo.setHttpVmUserCode(rs.getString("vm_user_code"));
        vmInfo.setHttpIsVdi(rs.getInt("is_vdi"));
        vmInfo.setPythonGuestId(rs.getString("guest_id"));
        vmInfo.setPythonGuestFamily(rs.getString("guest_family"));
        vmInfo.setPythonGuestFullName(rs.getString("guest_full_name"));
        vmInfo.setPythonGuestHostName(rs.getString("guest_host_name"));
        vmInfo.setPythonGuestIpAddress(rs.getString("guest_ip_address"));
        vmInfo.setPythonGuestDnsIpAddresses(rs.getString("guest_dns_ip_addresses"));
        vmInfo.setHttpNumCpu(rs.getLong("num_cpu"));
        vmInfo.setHttpMemorySizeMb(rs.getLong("memory_size_mb"));
        vmInfo.setPythonStorageCommittedB(rs.getLong("storage_committed_b"));
        vmInfo.setPythonStorageUncommittedB(rs.getLong("storage_uncommitted_b"));
        vmInfo.setPythonStorageUnsharedB(rs.getLong("storage_unshared_b"));
        vmInfo.setPythonTotalCapacityB(rs.getLong("total_capacity_b"));
        vmInfo.setPythonTotalFreeSpaceB(rs.getLong("total_free_space_b"));
        vmInfo.setPythonScreenWidth(rs.getLong("screen_width"));
        vmInfo.setPythonScreenHeight(rs.getLong("screen_height"));
        vmInfo.setPythonPowerState(rs.getString("power_state"));
        vmInfo.setPythonConnectionState(rs.getString("connection_state"));
        vmInfo.setPythonMaxCpuUsage(rs.getLong("max_cpu_usage"));
        vmInfo.setPythonMaxMemoryUsage(rs.getLong("max_memory_usage"));
        vmInfo.setPythonNumMksConnections(rs.getLong("num_mks_connections"));
        vmInfo.setPythonUuid(rs.getString("uuid"));
        vmInfo.setPythonChangeVersionDate(getDate(rs,"change_version_date"));
        vmInfo.setPythonBootTime(getDate(rs,"boot_time"));
        vmInfo.setHttpEventDate(getDate(rs,"event_date")); // Assuming event_date is in vmInfo
        vmInfo.setPythonCreateTime(getDate(rs,"create_time"));
        vmInfo.setPythonDatastoreIds(rs.getString("datastore_ids"));

        double totalusedSpaceB = (double) (vmInfo.getPythonTotalCapacityB() - vmInfo.getPythonTotalFreeSpaceB()) * 100 ; // C盘
        double totalCapacityB = Double.valueOf(vmInfo.getPythonTotalCapacityB()) == 0?1:Double.valueOf(vmInfo.getPythonTotalCapacityB()); // c盘
        double cdriveutilization = Double.parseDouble(format.format(totalusedSpaceB / totalCapacityB));
        // 维度和事实表同时新增C盘使用率
        vmInfo.setPythonVmcdriveutilization(cdriveutilization);

        vcenterInfo.setVmInfo(vmInfo);
        vcenterInfo.setObjectType(ObjectTypeConstant.DIM_VM);
        vcenterInfo.setRegion(vmInfo.getHttpRegion());
        vcenterInfo.setEventPartitionKey(vmInfo.getHttpVcenter()+"_"+vmInfo.getHttpVmMoid());
        return vcenterInfo;
    }

    @Override
    public void cancel() {
        flag = false;
    }

    private static Long getDate(ResultSet rs, String field) throws SQLException {
        // Convert event_date to a long
        Timestamp eventDateTimestamp = rs.getTimestamp(field);
        if (null == eventDateTimestamp){
            return null;
        }
        return eventDateTimestamp.getTime()/1000;
    }

}
