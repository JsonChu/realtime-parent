package com.moonpac.realtime.common.bean.vcenter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AutoTelegrafConfig {
    private String vcenter;
    private String username;
    private String password;
    private String paths;
    private String host;
    private String moid;
    private String type;
    private Integer MaxFileNum;
    private Integer fileNum;
    private Integer vmSize; // vcenter对应的vm的个数
    private String brokers;
    private String topic;
    private String region;
    private String telegrafPath;

    @Override
    public String toString() {
        return vcenter + "-" + type+ "-" + MaxFileNum+ "-" + fileNum;
    }
    public String getFileName(){
        return "telegraf-"+vcenter + "-" + type+ "-" + MaxFileNum+ "-" + fileNum+".conf";
    }

}
