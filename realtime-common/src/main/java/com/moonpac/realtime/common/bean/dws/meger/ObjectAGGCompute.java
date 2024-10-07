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
public class ObjectAGGCompute {
    private String objectType;  // 合并的objectType
    private String mergeOgnlExpression; // 合并的表达式
    private List<KeyAutoCompute> groupKey; // 分组的key
    private List<CombinationFieldCompute> combinationFields; // 普通字段
    private List<AGGFieldCompute> aggFieldCompute; // 统计字段
}
