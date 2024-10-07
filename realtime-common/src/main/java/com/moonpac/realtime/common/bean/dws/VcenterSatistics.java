package com.moonpac.realtime.common.bean.dws;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class VcenterSatistics {
    @JSONField(name = "objectType")
    private String objectType;
    private String vcenter;
    private String region; // vcenter 对应的区域
    private Double totalVmNum = 0.0D;// 所有的VDI
    private Double bootedVmNum = 0.0D; // 开机的VDI
    private Long eventDate;

    public void increaseBootedVmNum() {
        this.bootedVmNum++;
    }

    public void increaseTotalVmNum() {
        this.totalVmNum++;
    }

}
