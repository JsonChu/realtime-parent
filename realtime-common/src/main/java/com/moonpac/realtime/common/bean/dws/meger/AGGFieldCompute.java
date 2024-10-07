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
public class AGGFieldCompute {
    private String filterExpression;
    private String aggType; // 聚合的方式 sum
    private List<AtomsField> atomsFields; // 原子字段
}
