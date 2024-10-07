package com.moonpac.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.bean.dim.VcenterResourcePathInfo;
import com.moonpac.realtime.common.bean.dwd.SqlServerAfterEventData;
import com.moonpac.realtime.common.bean.dwd.SqlServerConnectionConfig;
import com.moonpac.realtime.common.bean.dwd.VmLifeCycleBean;
import com.moonpac.realtime.common.bean.metric.MetricRuleInfo;
import lombok.extern.slf4j.Slf4j;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcUtils {

    public static String pattern = "yyyy-MM-dd HH:mm:ss";

    /**
     * @author wenss
     * @date 2024/4/10 16:20
     * 获取doris中vm电源生命周期的初始数据
     */

    public static Map<String, SqlServerAfterEventData> getVdiLatestStatus(String driver, String url,
                                                                   String username, String password) throws Exception {

        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stmt = conn.createStatement();
        String sql="select vcenter,cs_ip ,vdi_name ,event_type ,event_date  from dwd_vdi_latest_event";
        ResultSet rs = stmt.executeQuery(sql);
        HashMap<String, SqlServerAfterEventData> vdiLatestStatusMap = new HashMap<>();
        while (rs.next()) {
            String vcenter = rs.getString("vcenter");
            String cs_ip = rs.getString("cs_ip");
            String vdi_name = rs.getString("vdi_name");
            String event_type = rs.getString("event_type");
            Long eventDate = getDate(rs, "event_date");
            //新增vcenter
            SqlServerAfterEventData sqlServerAfterEventData = new SqlServerAfterEventData();
            sqlServerAfterEventData.setVcenter(vcenter);
            sqlServerAfterEventData.setCsip(cs_ip);
            sqlServerAfterEventData.setNode(vdi_name);
            sqlServerAfterEventData.setEventType(event_type);
            sqlServerAfterEventData.setTime(eventDate);
            String key = vcenter+"_"+cs_ip+"_"+vdi_name;
            vdiLatestStatusMap.put(key,sqlServerAfterEventData);
        }
        rs.close();
        stmt.close();
        conn.close();
        return vdiLatestStatusMap;
    }

    public static void writeToDoris(List<VcenterResourcePathInfo> vcenterResourcePathInfoList, String dorisDriverClassName,
                                    String dorisUrl, String dorisUsername, String dorisPassword) throws Exception {
        Class.forName(dorisDriverClassName);
        Connection conn = DriverManager.getConnection(dorisUrl, dorisUsername, dorisPassword);
        PreparedStatement preparedStatement = null;
        try {
            String insertSQL = "INSERT INTO vcenter_db.dim_vcenter_resource_path " +
                    "(vcenter, moid, type, name, path, collect_host, config_name, insert_date) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            preparedStatement = conn.prepareStatement(insertSQL);

            for (VcenterResourcePathInfo dimVcenterResourcePathInfo : vcenterResourcePathInfoList) {
                preparedStatement.setString(1, dimVcenterResourcePathInfo.getVcenter());
                preparedStatement.setString(2, dimVcenterResourcePathInfo.getMoid());
                preparedStatement.setString(3, dimVcenterResourcePathInfo.getType());
                preparedStatement.setString(4, dimVcenterResourcePathInfo.getName());
                preparedStatement.setString(5, dimVcenterResourcePathInfo.getPath());
                preparedStatement.setString(6, dimVcenterResourcePathInfo.getCollectHost());
                preparedStatement.setString(7, dimVcenterResourcePathInfo.getConfigName());
                java.util.Date insertDate = dimVcenterResourcePathInfo.getInsertDate();
                preparedStatement.setTimestamp(8, (insertDate == null) ? null : new Timestamp(insertDate.getTime()));
                preparedStatement.addBatch();
            }
            preparedStatement.executeBatch();
            log.info("Data inserted successfully into Doris table.");
        } finally {
            if (preparedStatement != null) {
                preparedStatement.close();
            }
            if (conn != null) {
                conn.close();
            }
        }
    }

    public static List<MetricRuleInfo> getOracleMetricRule(String driver, String url, String username, String password)
            throws Exception {

        //查询oracle中所有自定义指标
        String sql = " SELECT * FROM " + username + ".METRIC_INFO WHERE METRIC_TYPE = 2 AND METRIC_STATUS = 1 ";
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, username, password);
        //执行sql
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        // 处理查询结果
        List<MetricRuleInfo> MetricInfoList = new ArrayList<>();
        while (rs.next()) {

            String metricId = rs.getString("METRIC_ID");
            String metricName = rs.getString("METRIC_NAME");
            String objectTypeId = rs.getString("OBJECT_TYPE_ID");
            int metricType = rs.getInt("METRIC_TYPE");
            int metricDataType = rs.getInt("METRIC_DATA_TYPE");
            int metricStatus = rs.getInt("METRIC_STATUS");
            String metricDesc = rs.getString("METRIC_DESC");
            String fieldName = rs.getString("FIELD_NAME");
            int delFlag = rs.getInt("DEL_FLAG");
            String createBy = rs.getString("CREATE_BY");
            Date createTime = rs.getDate("CREATE_TIME");
            String updateBy = rs.getString("UPDATE_BY");

            String updateTimeStr = rs.getString("UPDATE_TIME");
            SimpleDateFormat sdf = new SimpleDateFormat(pattern);
            java.util.Date date = sdf.parse(updateTimeStr);

            String metricCountFormula = rs.getString("METRIC_COUNT_FORMULA");
            MetricRuleInfo metricInfo = new MetricRuleInfo();
            metricInfo.setOp("+I");
            metricInfo.setMetricId(metricId);
            metricInfo.setMetricName(metricName);
            metricInfo.setMetricStatus(metricStatus);
            metricInfo.setFieldName(fieldName);
            metricInfo.setMetricCountFormula(metricCountFormula);
            metricInfo.setObjectTypeId(objectTypeId);
            metricInfo.setUpdateTime(date.getTime());


            MetricInfoList.add(metricInfo);
        }

        rs.close();
        stmt.close();
        conn.close();
        return MetricInfoList;

    }

    /**
     * @author zhanglingxing
     * @date 2024/2/27 16:20
     * 获取算法需要的数据
     */

    public static List<JSONObject> getDorisData(String driver, String url,
                                                String username, String password,
                                                String sql, String fields) throws Exception {

        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stmt = conn.createStatement();
        ResultSet rs = stmt.executeQuery(sql);
        // 处理查询结果
        List<JSONObject> resultList = new ArrayList<>();
        String[] fieldNames = fields.split(",");
        while (rs.next()) {
            JSONObject jsonOutPut = new JSONObject();
            // 拼装keyByFields中指定的每一个字段的值
            for (String fieldName : fieldNames) {
                jsonOutPut.put(fieldName, rs.getString(fieldName)==null?"null":rs.getString(fieldName));
            }
            resultList.add(jsonOutPut);
        }
        rs.close();
        stmt.close();
        conn.close();
        return resultList;
    }

    /**
     * @author wenss
     * @date 2024/4/10 16:20
     * 获取doris中vm电源生命周期的初始数据
     */

    public static Map<String, VmLifeCycleBean> getDorisVmLifeCycle(String driver, String url,
                                                                   String username, String password) throws Exception {

        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stmt = conn.createStatement();
        String sql="select * from vcenter_db.dwd_vm_lifecycle_info where end_time = '9999-01-01 00:00:00'";
        ResultSet rs = stmt.executeQuery(sql);
        // 处理查询结果
//        List<VMLifeCycleBean> resultList = new ArrayList<>();
        HashMap<String, VmLifeCycleBean> vmLifeCycleBeanMap = new HashMap<>();
        while (rs.next()) {
            String vcenter = rs.getString("vcenter");
            String vmMoid = rs.getString("vm_moid");
            String vmName = rs.getString("vm_name");
            String powerState = rs.getString("power_state");
            Long startTime = getDate(rs, "start_time");
            Long endTime = getDate(rs, "end_time");
            Long eventDate = getDate(rs, "event_date");
            String region = rs.getString("region");
            String datacenterId = rs.getString("datacenter_id");
            String clusterId = rs.getString("cluster_id");
            String hostId = rs.getString("host_id");
            String datacenterName = rs.getString("datacenter_name");
            String clusterName = rs.getString("cluster_name");
            String hostName = rs.getString("host_name");
            int isVdi = rs.getInt("is_vdi");
            String vmUserCode = rs.getString("vm_user_code");
            //新增vcenter
            VmLifeCycleBean vmLifeCycleBean = new VmLifeCycleBean();
            vmLifeCycleBean.setHttpVcenter(vcenter);
            vmLifeCycleBean.setHttpVmMoid(vmMoid);
            vmLifeCycleBean.setHttpVmName(vmName);
            vmLifeCycleBean.setPythonPowerState(powerState);
            vmLifeCycleBean.setStartTime(startTime);
            vmLifeCycleBean.setEndTime(endTime);
            vmLifeCycleBean.setHttpEventDate(eventDate);
            vmLifeCycleBean.setHttpRegion(region);
            vmLifeCycleBean.setHttpDatacenterId(datacenterId);
            vmLifeCycleBean.setHttpClusterId(clusterId);
            vmLifeCycleBean.setHttpHostId(hostId);
            vmLifeCycleBean.setHttpDatacenterName(datacenterName);
            vmLifeCycleBean.setHttpClusterName(clusterName);
            vmLifeCycleBean.setHttpHostName(hostName);
            vmLifeCycleBean.setHttpIsVdi(isVdi);
            vmLifeCycleBean.setHttpVmUserCode(vmUserCode);
            vmLifeCycleBeanMap.put(vcenter+"_"+vmMoid,vmLifeCycleBean);
        }
        rs.close();
        stmt.close();
        conn.close();
        return vmLifeCycleBeanMap;
    }


    public static List<SqlServerConnectionConfig> getDorisDatabaseCSEventData(String driver, String url,
                                                                              String username, String password
                                                ) throws Exception {
        Class.forName(driver);
        Connection conn = DriverManager.getConnection(url, username, password);
        Statement stmt = conn.createStatement();
        String sql="select * from vcenter_db.dim_database_info where db_type='sqlserver'";
        ResultSet rs = stmt.executeQuery(sql);
        // 处理查询结果
        List<SqlServerConnectionConfig> resultList = new ArrayList<>();
        while (rs.next()) {
            String ip = rs.getString("ip");
            int port = rs.getInt("port");
            String csUserName = rs.getString("username");
            String csPassword = rs.getString("pwd");
            String csEventDatabase = rs.getString("cs_event_database");
            String csEventTable = rs.getString("cs_event_table");
            //新增vcenter
            String vcenter = rs.getString("vcenter");
            SqlServerConnectionConfig sqlServerConnectionConfig = new SqlServerConnectionConfig();
            sqlServerConnectionConfig.setIp(ip);
            sqlServerConnectionConfig.setPort(port);
            sqlServerConnectionConfig.setUsername(csUserName);
            sqlServerConnectionConfig.setPassword(csPassword);
            sqlServerConnectionConfig.setDatabase(csEventDatabase);
            sqlServerConnectionConfig.setTable(csEventTable);
            sqlServerConnectionConfig.setVcenter(vcenter);

            resultList.add(sqlServerConnectionConfig);
        }
        rs.close();
        stmt.close();
        conn.close();
        return resultList;
    }


    private static Long getDate(ResultSet rs, String field) throws SQLException {
        // Convert event_date to a long
        Timestamp eventDateTimestamp = rs.getTimestamp(field);
        if (null == eventDateTimestamp){
            return null;
        }
        return eventDateTimestamp.getTime();
    }

}