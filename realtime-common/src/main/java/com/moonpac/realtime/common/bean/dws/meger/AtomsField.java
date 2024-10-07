package com.moonpac.realtime.common.bean.dws.meger;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AtomsField {
    private String  getFieldExpression;
    private String  fieldName; // 生成的字段名称
    private Boolean isCoreField; // 是否核心字段
}
