package com.moonpac.realtime.common.bean.dws;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.text.DecimalFormat;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MergeEventBean {

    private Long eventdate;

    private int datacount;

    private String  key;

    private String objecttype;

    private String vcenter;

    private String region; // vcenter 对应的区域

    /**
     * vm维度数据
     */
    private String vmmoid;
    private String vmname;
    //  private Double vmdatasize;
    // cpu
    private Double vmcpucoretotal=0.0;
    private Double vmcpuusage=0.0;
    private Double vmcpuusagemhz=0.0;
    // mem
    private Double vmmemusage=0.0;
    private Double vmmemactive=0.0;
    // 增加mem总容量及vm存储总容量 2024-03-19
    private Double vmmemorysizemb=0.0;
    private Double vmtotalcapacityb=0.0;
    private Double isvdi = 0.0;
    // disk
    // 新增C盘使用率 2024-1-9
    private Double vmcdriveutilization;
    private Double vmdiskusage=0.0;
    private Double vmdiskread=0.0;
    private Double vmdiskwrite=0.0;
    // net
    private Double vmnetbytesrx=0.0;
    private Double vmnetbytestx=0.0;
    // datastore
    private Double vmdsnumberread=0.0;
    private Double vmdsnumberwrite=0.0;
    private Double vmdsnumbertotal=0.0;

    // 电源状态 2024-02-28
    private String vmpowerstate;
    private Double uptimeseconds=0.0;

    /**
     * host维度数据
     */
    private String hostmoid;
    private String hostname;
    //  private Double hostdatasize;
    // cpu

    private Double hostcpuusage=0.0;
    private Double hostcpuusagemhz=0.0;
    // mem
    private Double hostmemusage=0.0;
    private Double hostmemactive=0.0;
    // disk
    private Double hostdiskwrite=0.0;
    private Double hostdiskread=0.0;
    // net
    private Double hostnetbytesrx=0.0;
    private Double hostnetbytestx=0.0;
    // datastore
    private Double hostdsnumberread=0.0;
    private Double hostdsnumberwrite=0.0;

    private Double hostcpucoretotal=0.0; //逻辑核数 创建vdi的可以选择的核数
    private Double memorysizegb; // 内存大小

    /**
     * datastore维度数据
     */
    private String dsmoid;
    private String dsname;
    private Double dstotalcapacity=0.0;
    private Double dsusedcapacity=0.0;
    private Double dsfreecapacity=0.0;
    private Double dsusagerate=0.0;

    // 增加分配容量
    private Double dsprovisionedcapacity=0.0;

    private Double dsnumberreadaveraged=0.0;
    private Double dsnumberwriteaeraged=0.0;


    /**
     * vcenter 维度数据
     */
    private String vcmoid; // vcenter 的唯一ID
    private Double totalvmnum;// 所有的VDI个数
    private Double bootedvmnum; // 开机的VDI个数
    private Double shutdownvmnum; // 关机的VDI数据个数
    private Double bootedrate; // 开机率
    private Double shutdownrate; // 关机率

    public void calculateRates() {
        if (totalvmnum != null && totalvmnum != 0) {
            DecimalFormat df = new DecimalFormat("#.##");
            this.bootedrate = Double.valueOf(df.format((bootedvmnum / totalvmnum) * 100));
            this.shutdownrate = Double.valueOf(df.format(((totalvmnum - bootedvmnum) / totalvmnum) * 100));
            this.shutdownvmnum = Double.valueOf(df.format(totalvmnum - bootedvmnum));
        } else {
            // Handle division by zero or null
            bootedrate = 0.0;
            shutdownrate = 0.0;
            shutdownvmnum = 0.0;
        }
    }

    /**
     * 数据库对象
     */
    private String dbmoid; // vcenter 的唯一ID
    private String dbtype; // 数据库类型
    private Double vcdbusercurconnectnum=0.0; // vcenter数据库用户当前连接数
    private Double vcdbusermaxconnectnum=0.0; // vcenter数据库用户最大连接数
    private Double vcdbuserrate=0.0; // vcenter数据库使用率

    private Double csdbusercurconnectnum=0.0; // cs数据库用户当前连接数
    private Double csdbusermaxconnectnum=0.0; // cs数据库用户最大连接数
    private Double csdbuserrate=0.0; // vcenter数据库使用率

    // 新增ip地址 为vm，host，数据库等ip
    private String ip;

}
