package com.moompac.feature.telegrafconfig.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moonpac.realtime.common.bean.dim.VcenterResourcePathInfo;
import com.moonpac.realtime.common.bean.vcenter.AutoTelegrafConfig;
import com.moonpac.realtime.common.bean.vcenter.HorizonParam;
import com.moonpac.realtime.common.util.*;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import java.io.FileInputStream;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * @author zhanglingxing
 * @date 2024/3/25 16:42
 * 根据VM和host的path 可以分布式的生成对应的路径
 * 1.读取doris表 dim_vcenter_resource_path vm和host对应的path
 * 2.每条数据写入写入对应的文件
 *
 * nohup java -cp data-process-1.0-SNAPSHOT.jar com.moonpac.task.AutoTelegrafConfigGen /opt/python_model_data/jar/config.properties &
 */
@Slf4j
public class AutoTelegrafConfigGen {

    public static void main(String[] args) throws Exception{
        // 首次启动执行
        executeTask(args[0]);
        // 获取当前时间
        long currentTimeMillis = System.currentTimeMillis();
        // 计算距离下一个凌晨1点的时间差
        long initialDelay = getNextMidnightTime() - currentTimeMillis;
        // 创建 ScheduledExecutorService 实例
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        // 每天凌晨1点执行
        scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        executeTask(args[0]);
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }, // 任务
                initialDelay,               // 首次执行延迟时间
                24 * 60 * 60 * 1000,        // 执行周期（一天）
                TimeUnit.MILLISECONDS
        );
    }

    // 执行任务的方法
    private static <File> void executeTask(String configFilePath) throws Exception{
        Properties properties = new Properties();
        // 加载配置文件
        FileInputStream fileInputStream = new FileInputStream(configFilePath);
        properties.load(fileInputStream);
        fileInputStream.close();

        // 获取参数值
        String dorisDriverClassName = properties.getProperty("doris.driver");
        String dorisUrl = properties.getProperty("doris.url");
        String dorisUsername = properties.getProperty("doris.username");
        String dorisPassword = properties.getProperty("doris.password");
        String telegrafWorkPath = properties.getProperty("telegraf.config.path");
        String telegrafConfigPath = telegrafWorkPath+System.getProperty("file.separator")+"vcenter";
        String telegrafServers = properties.getProperty("telegraf.collector.servers");
        String telegrafCurrentServer = properties.getProperty("telegraf.current.server");
        Integer telegrafMaxLimit = Integer.parseInt(properties.getProperty("telegraf.max.vm.limit"));
        String kafkaBrokers = properties.getProperty("kafka.brokers");
        String kafkaTopic = properties.getProperty("kafka.topic");

        // 获取开始时间
        long startTime = System.currentTimeMillis();
        List<HorizonParam> csParams = getHorizonDim(dorisDriverClassName, dorisUrl, dorisUsername, dorisPassword);
        log.info("从doris获取Horizon配置信息 耗时：{} 毫秒", System.currentTimeMillis()- startTime); // 打印耗时
        long startTime1 = System.currentTimeMillis();
        List<VcenterResourcePathInfo> vmPathDatas = getVMPath(csParams);
        log.info("从Horizon获取vm的路径 耗时：{} 毫秒", System.currentTimeMillis()- startTime1); // 打印耗时
        if (vmPathDatas.isEmpty()){
            log.info("通过Horizon接口没有获取到vm的path，程序异常退出");
            throw  new RuntimeException("通过Horizon接口没有获取到vm的path，程序异常退出");
        }
        String freemarkerTemplatePath = properties.getProperty("ftl.path");
        // vcenter的维度
        HashMap<String, AutoTelegrafConfig> vcenterDim = getVcenterDim(vmPathDatas,telegrafMaxLimit,dorisDriverClassName,
                                                                                   dorisUrl, dorisUsername, dorisPassword,kafkaBrokers,kafkaTopic);

        //*************************生成 vm的配置 文件
        List<VcenterResourcePathInfo> outDorisList = new ArrayList<>();
        Map<String,AutoTelegrafConfig> telegrafVMConfigMap = new HashMap<>();
        for (VcenterResourcePathInfo vmResourcePath : vmPathDatas) {
            AutoTelegrafConfig autoTelegrafConfig = AutoTelegrafUtils.calculateVM(vcenterDim,vmResourcePath,telegrafServers,telegrafCurrentServer);
            if (autoTelegrafConfig != null){ // 这个里面的数据一定是当前主机的数据
                outDorisList.add(vmResourcePath); // 分布式写入 每个节点只写入自己的数据
                String fileName = autoTelegrafConfig.getFileName();
                AutoTelegrafConfig cachedAutoTelegrafConfig = telegrafVMConfigMap.get(fileName);
                if( null == cachedAutoTelegrafConfig){
                    telegrafVMConfigMap.put(fileName,autoTelegrafConfig);
                }else{
                    String newPaths = cachedAutoTelegrafConfig.getPaths()+"\","+System.getProperty("line.separator")+"                 \""+vmResourcePath.getPath();
                    cachedAutoTelegrafConfig.setPaths(newPaths);
                    telegrafVMConfigMap.put(fileName,cachedAutoTelegrafConfig);
                }
            }
        }
        // 删除之前的配置文件
        FreeMarkerUtils.deleteFilesWithPrefixAndSuffix(telegrafConfigPath,"telegraf-",".conf","vm");

        for (Map.Entry<String, AutoTelegrafConfig> entry : telegrafVMConfigMap.entrySet()) {
            String fileName = entry.getKey();
            AutoTelegrafConfig telegrafVMConfig = entry.getValue();
            telegrafVMConfig.setPaths(telegrafVMConfig.getPaths());
            // 调用工具类方法填充模板并输出到文件
            FreeMarkerUtils.fillTemplate(freemarkerTemplatePath, "telegraf-vcenter-template-vm.ftl",
                    telegrafConfigPath,fileName,telegrafVMConfig);
        }
        log.info("写入doris的数据的数据条数：{}",outDorisList.size());
        // 打印列表中每个对象的信息
        JdbcUtils.writeToDoris(outDorisList,dorisDriverClassName, dorisUrl,dorisUsername,dorisPassword);

        //*************************生成 Host和DataStore的telegraf文件 和python采集sdk的文件 每一个vcenter对应一个文件
        Map<String,AutoTelegrafConfig> telegrafHostConfigMap = AutoTelegrafUtils.calculateHost(vcenterDim,telegrafServers,telegrafCurrentServer);
        if (!telegrafHostConfigMap.isEmpty()){
            FreeMarkerUtils.deleteFilesWithPrefixAndSuffix(telegrafConfigPath,"telegraf-",".conf","host");
            FreeMarkerUtils.deleteFilesWithPrefixAndSuffix(telegrafConfigPath,"telegraf-",".conf","python");
            for (Map.Entry<String, AutoTelegrafConfig> entry : telegrafHostConfigMap.entrySet()) {
                AutoTelegrafConfig telegrafConfig = entry.getValue();
                // host和datastore的telegraf 的配置文件
                FreeMarkerUtils.fillTemplate(freemarkerTemplatePath, "telegraf-vcenter-template-host.ftl",
                        telegrafConfigPath,telegrafConfig.getFileName(),telegrafConfig);
                telegrafConfig.setType("python");
                telegrafConfig.setTelegrafPath(telegrafWorkPath);
                // python采集vcenter sdk的telegraf
                FreeMarkerUtils.fillTemplate(freemarkerTemplatePath, "telegraf-vcenter-template-python.ftl",
                        telegrafConfigPath,telegrafConfig.getFileName(),telegrafConfig);
            }
        }
        //************************* 脚本复制
        FileUtils.copyFileToDirectory(freemarkerTemplatePath+System.getProperty("file.separator")+"vcenter-info.py", telegrafWorkPath);
        FileUtils.copyFileToDirectory(freemarkerTemplatePath+System.getProperty("file.separator")+"control-vcenter-telegraf.sh", telegrafWorkPath);
        FileUtils.copyFileToDirectory(freemarkerTemplatePath+System.getProperty("file.separator")+"monitor-vcenter-telegraf.sh", telegrafWorkPath);

        AutoTelegrafConfig monitorTelegrafConfig = new AutoTelegrafConfig();
        monitorTelegrafConfig.setBrokers(convertToStrings(kafkaBrokers));
        monitorTelegrafConfig.setTopic(kafkaTopic);
        monitorTelegrafConfig.setTelegrafPath(telegrafWorkPath);
        // 监控telegraf进程的telegraf配置文件
        FreeMarkerUtils.fillTemplate(freemarkerTemplatePath, "monitor-vcenter-telegraf.ftl",
                telegrafWorkPath,"monitor-vcenter-telegraf.config",monitorTelegrafConfig);
    }

    // 获取下一个凌晨1点的时间戳
    private static long getNextMidnightTime() {
        // 获取当前时间
        long currentTimeMillis = System.currentTimeMillis();
        // 获取今天凌晨的时间戳
        long todayMidnight = currentTimeMillis - currentTimeMillis % (24 * 60 * 60 * 1000);
        // 获取明天凌晨1点的时间戳
        return todayMidnight + 24 * 60 * 60 * 1000;
    }


    private static  HashMap<String,AutoTelegrafConfig>  getVcenterDim(List<VcenterResourcePathInfo> vmPathDatas,Integer telegrafMaxLimit,
                                                              String driverClassName, String url,
                                                              String username, String password,String kafkaBrokers,String kafkaTopic) throws Exception {

        Map<String,Integer> vcenterVMSizeMap = new HashMap<>(); // hz 接口返回的 vcneter
        // 第一次遍历 获取
        for (VcenterResourcePathInfo vmResourcePath : vmPathDatas) {
            String vcenterIp = vmResourcePath.getVcenter();
            vcenterVMSizeMap.put(vcenterIp,vcenterVMSizeMap.getOrDefault(vcenterIp,0)+1);
        }
        for (Map.Entry<String, Integer> entry : vcenterVMSizeMap.entrySet()) {
            int vmSize = entry.getValue();
            int maxFileNum = (int)Math.ceil((double)vmSize/ telegrafMaxLimit);
            log.info("vcenter={},VM总个数={}，单个telegraf采集的vm个数={},生成的配置文件个数={}",entry.getKey(),vmSize,telegrafMaxLimit,maxFileNum);
        }
        HashMap<String,AutoTelegrafConfig> dimVcenterMap = new HashMap<>();
        String dimVcenterSql = "SELECT vcenter_ip ,username ,`password`,region from vcenter_db.dim_vcenter_info ;";
        String dimVcenterField = "vcenter_ip,username,password,region";
        List<JSONObject> dimVcenterDatas = JdbcUtils.getDorisData(driverClassName, url, username, password,
                                                                                dimVcenterSql,dimVcenterField);
        for (JSONObject jsonObject  : dimVcenterDatas) {// 这是数据库的
            String vcenter = jsonObject.getString("vcenter_ip");
            if ( null != vcenterVMSizeMap.get(vcenter)){
                AutoTelegrafConfig autoTelegrafConfig = new AutoTelegrafConfig();
                autoTelegrafConfig.setVcenter(vcenter);
                autoTelegrafConfig.setRegion(jsonObject.getString("region"));
                autoTelegrafConfig.setBrokers(convertToStrings(kafkaBrokers));
                autoTelegrafConfig.setTopic(kafkaTopic);
                autoTelegrafConfig.setUsername(jsonObject.getString("username"));
                autoTelegrafConfig.setPassword(jsonObject.getString("password"));
                autoTelegrafConfig.setVmSize(vcenterVMSizeMap.get(vcenter));
                int maxFileNum = (int)Math.ceil((double)vcenterVMSizeMap.get(vcenter)/ telegrafMaxLimit);
                autoTelegrafConfig.setMaxFileNum(maxFileNum);
                dimVcenterMap.put(vcenter,autoTelegrafConfig);
            }
        }
        return dimVcenterMap;
    }

    private static List<VcenterResourcePathInfo> getVMPath(List<HorizonParam> csParams) throws Exception {
        List<VcenterResourcePathInfo> dimVcenterResourcePathInfoList = new ArrayList<>();
        OkHttpClient okHttpClient = OKHttpUtil.getOKHttpClient();
        Date date = new Date();
        //循环调用所有horizon
        for (HorizonParam csParam : csParams) {
            //获取token
            String csToken = OKHttpUtil.getCSToken(okHttpClient, csParam);
            //获取vcenter id
            String horizon = csParam.getHorizonIp();
            String url = "https://" + horizon + "/rest/config/v1/virtual-centers";
            String vcenterInfoStr = OKHttpUtil.csRequest(okHttpClient, url, csToken, null);
            //封装结果集
            JsonNode vcenterInfoJsonNode = new ObjectMapper().readTree(vcenterInfoStr);
            if (vcenterInfoJsonNode.isArray() && vcenterInfoJsonNode.size() > 0) {
                //循环每一个vcenter
                for (JsonNode vcenterInfoNode : vcenterInfoJsonNode) {

                    String vcenterId = getJsonStringKey(vcenterInfoNode, "id");
                    String vcenterServer = getJsonStringKey(vcenterInfoNode, "server_name");
                    //获取vcenter下的vm路径
                    String vmUrl = "https://" + horizon + "/rest/external/v1/base-vms?vcenter_id=" + vcenterId;
                    String vmStr = OKHttpUtil.csRequest(okHttpClient, vmUrl, csToken, null);
                    //封装结果集
                    JsonNode vmJsonNode = new ObjectMapper().readTree(vmStr);
                    if (vmJsonNode.isArray() && vmJsonNode.size() > 0) {
                        for (JsonNode vmNode : vmJsonNode) {
                            String moid = getJsonStringKey(vmNode, "id");
                            String name = getJsonStringKey(vmNode, "name");
                            String path = getJsonStringKey(vmNode, "path");
                            VcenterResourcePathInfo dimVcenterResourcePathInfo = new VcenterResourcePathInfo();
                            dimVcenterResourcePathInfo.setVcenter(vcenterServer);
                            dimVcenterResourcePathInfo.setMoid(moid);
                            dimVcenterResourcePathInfo.setName(name);
                            dimVcenterResourcePathInfo.setPath(path);
                            dimVcenterResourcePathInfo.setType("vm");
                            dimVcenterResourcePathInfo.setInsertDate(date);
                            dimVcenterResourcePathInfoList.add(dimVcenterResourcePathInfo);
                        }
                    } else {
                        log.info("vm路径列表为空，采集结束。horizon: {}，vcenter: {}", horizon, vcenterServer);
                    }
                }
            } else {
                log.info("vcenter列表为空，采集结束。horizon: {}", horizon);
            }
        }
        return dimVcenterResourcePathInfoList;
    }


    private static List<HorizonParam> getHorizonDim(String driverClassName, String url,
                                               String username, String password) throws Exception {
        List<HorizonParam> csParams = new ArrayList<>();
        String dimHorizonSql = "SELECT horizon_ip,domain,username,password from vcenter_db.dim_horizon_info ;";
        String dimHorizonField = "horizon_ip,domain,username,password";
        List<JSONObject> dimCSDatas = JdbcUtils.getDorisData(driverClassName, url, username, password,
                dimHorizonSql, dimHorizonField);
        for (JSONObject jsonObject : dimCSDatas) {
            HorizonParam csParam = JSON.toJavaObject(jsonObject, HorizonParam.class);
            csParams.add(csParam);
        }
        return csParams;
    }

    /**
     * 获取jsonNode中字符串值
     */
    public static String getJsonStringKey(JsonNode jsonNode, String key) {
        if (Objects.nonNull(jsonNode) && !jsonNode.isArray()) {
            return (jsonNode.get(key) == null) ? null : jsonNode.get(key).asText();
        } else {
            return null;
        }
    }
    private static String convertToStrings(String rest) {
        StringBuilder sb = new StringBuilder();
        sb.append("\"");
        String[] parts = rest.split(",");
        for (int i = 0; i < parts.length; i++) {
            if (i == parts.length - 1){
                sb.append(parts[i]);
            }else {
                sb.append(parts[i]).append("\",\"");
            }
        }
        sb.append("\"");
        return sb.toString();
    }

}
