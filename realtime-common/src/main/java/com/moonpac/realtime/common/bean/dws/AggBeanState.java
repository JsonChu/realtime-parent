package com.moonpac.realtime.common.bean.dws;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.ArrayList;
import java.util.List;

/**
 * @author zhanglingxing
 * @date 2024/3/14 13:37
 *  AggBean 对应的中间
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class AggBeanState {

    //从merge主题聚合
    private List<Double> cpuusedmhzList = new ArrayList<>();
    private List<Double> memusedgbList = new ArrayList<>();
    private List<Double> netusedList = new ArrayList<>();
    private List<Double> diskusedList = new ArrayList<>();
    private List<Double> datastoreusedList = new ArrayList<>();
    private List<Double> dsnumberreadaveragedList = new ArrayList<>();
    private List<Double> dsnumberwriteaeragedList = new ArrayList<>();

}
