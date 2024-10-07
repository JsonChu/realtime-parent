package com.moonpac.realtime.common.bean.dws.meger;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.List;
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class MergeAutoCompute {
    private String ruleCode; // 规则的唯一编码
    private String ruleStatus; // 1 上线 0下载
    private List<ObjectAGGCompute> objectAGGCompute;
}
