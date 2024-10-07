package com.moonpac.realtime.common.util;

import com.google.common.hash.Hashing;
import com.moonpac.realtime.common.bean.dim.VcenterResourcePathInfo;
import com.moonpac.realtime.common.bean.vcenter.AutoTelegrafConfig;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;

import java.util.*;

@Slf4j
public class AutoTelegrafUtils {

    public static AutoTelegrafConfig calculateVM(HashMap<String, AutoTelegrafConfig> vcenterDim,
                                                 VcenterResourcePathInfo vmResourcePath,
                                                 String hostStr, String currentHost) throws Exception {

        String vcenter = vmResourcePath.getVcenter();
        String moid = vmResourcePath.getMoid();
        String type = vmResourcePath.getType();
        String path = vmResourcePath.getPath();
        AutoTelegrafConfig dimVcenter = vcenterDim.get(vcenter);
        if ( null == dimVcenter){
            return null;
        }
        Integer maxFileNum = dimVcenter.getMaxFileNum();
        // host主机列表 防止了 用户输入的hostStr不一样
        List<String> hostList = new ArrayList<>(new TreeSet<>(Arrays.asList(hostStr.replaceAll("\\s+", "").split(","))));
        // 使用 Guava 的一致性哈希函数
        int fileNum = Hashing.consistentHash(path.hashCode(),maxFileNum);
        int index = fileNum % hostList.size();
        String fileHost = hostList.get(index);
        if (currentHost.equals(fileHost) ){
            AutoTelegrafConfig autoTelegrafConfig = new AutoTelegrafConfig(); // 创建第一个对象
            // 将dimVcenter的属性复制到autoTelegrafConfig中
            BeanUtils.copyProperties(autoTelegrafConfig, dimVcenter);
            autoTelegrafConfig.setVcenter(vcenter);
            autoTelegrafConfig.setHost(currentHost);
            autoTelegrafConfig.setMoid(moid);
            autoTelegrafConfig.setType(type);
            autoTelegrafConfig.setFileNum(fileNum);
            autoTelegrafConfig.setMaxFileNum(maxFileNum);
            autoTelegrafConfig.setPaths(path);
            // 对vmResourcePath复制采集信息
            vmResourcePath.setConfigName(autoTelegrafConfig.getFileName());
            vmResourcePath.setCollectHost(currentHost);
            return autoTelegrafConfig;
        }
        return null;
    }

    public static Map<String, AutoTelegrafConfig> calculateHost(HashMap<String, AutoTelegrafConfig> vcenterDim,
                                                                String telegrafServers, String currentHost) {
        Map<String,AutoTelegrafConfig> telegrafHostConfigMap = new HashMap<>();
        for (Map.Entry<String, AutoTelegrafConfig> entry : vcenterDim.entrySet()) {
            String vcenter = entry.getKey();
            // host主机列表 防止了 用户输入的hostStr不一样
            List<String> hostList = new ArrayList<>(new TreeSet<>(Arrays.asList(telegrafServers.replaceAll("\\s+", "").split(","))));
            // 使用 Guava 的一致性哈希函数
            int fileNum = Hashing.consistentHash(vcenter.hashCode(),hostList.size());
            int index = fileNum % hostList.size();
            String fileHost = hostList.get(index);
            if (currentHost.equals(fileHost) ){
                AutoTelegrafConfig autoTelegrafConfig = entry.getValue();
                autoTelegrafConfig.setType("host");
                autoTelegrafConfig.setMaxFileNum(1);
                autoTelegrafConfig.setFileNum(0);
                telegrafHostConfigMap.put(vcenter,autoTelegrafConfig);
            }
        }
        return telegrafHostConfigMap;
    }
}
