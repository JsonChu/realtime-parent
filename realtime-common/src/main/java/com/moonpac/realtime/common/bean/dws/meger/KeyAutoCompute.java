package com.moonpac.realtime.common.bean.dws.meger;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class KeyAutoCompute {
    private String  eventNameExpression; // 事件类型名称
    private String  groupExpression; // group by key 动态计算字段
    private String  uniqueKeyExpression; // 数据去重的表达式
}
