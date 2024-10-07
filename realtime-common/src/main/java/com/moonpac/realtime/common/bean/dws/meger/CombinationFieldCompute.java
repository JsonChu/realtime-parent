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
public class CombinationFieldCompute {
    private String  filterExpression;
    private List<AtomsField> atomsFields;
}
