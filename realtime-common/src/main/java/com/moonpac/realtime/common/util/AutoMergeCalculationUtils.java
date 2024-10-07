package com.moonpac.realtime.common.util;

import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.bean.dws.meger.AGGFieldCompute;
import com.moonpac.realtime.common.bean.dws.meger.AtomsField;
import com.moonpac.realtime.common.bean.dws.meger.CombinationFieldCompute;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.constant.AutoCalFunctionConstant;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class AutoMergeCalculationUtils {

    /*
       普通字段计算
     */
    public static void  combinationFieldCalculation(JSONObject jsonResult, List<CombinationFieldCompute> combinationFields,
                                                    Set<KafkaVcenterInputBean> inputBeans) {
        for (KafkaVcenterInputBean inputBean : inputBeans) {
            for (CombinationFieldCompute combinationField : combinationFields) {
                String filterExpression = combinationField.getFilterExpression();
                boolean filterResult = AutoMergeOgnlUtils.getBooleanValue(filterExpression,inputBean);
                if (filterResult){
                    List<AtomsField> atomsFields = combinationField.getAtomsFields();
                    for (AtomsField atomsField : atomsFields) {
                        String fieldName = atomsField.getFieldName();
                        String getFieldExpression = atomsField.getGetFieldExpression();
                        Object fieldValue = AutoMergeOgnlUtils.getObjectValue(getFieldExpression, inputBean);
                        jsonResult.put(fieldName,fieldValue);
                    }
                }
            }
        }
    }

    /*
     聚合字段计算
    */
    public static void aggFieldCalculation(JSONObject jsonResult, List<AGGFieldCompute> aggFieldComputeList,
                                     Set<KafkaVcenterInputBean> inputBeans) {

        Map<String,Double> aggResult = new HashMap<>();
        for (KafkaVcenterInputBean inputBean : inputBeans) {
            for (AGGFieldCompute aggFieldCompute : aggFieldComputeList) {
                String filterExpression = aggFieldCompute.getFilterExpression();
                boolean filterResult = AutoMergeOgnlUtils.getBooleanValue(filterExpression,inputBean);
                if (filterResult){
                    String aggType = aggFieldCompute.getAggType();
                    if (AutoCalFunctionConstant.aggSum.equals(aggType)){ // 求 和 的计算
                        List<AtomsField> atomsFields = aggFieldCompute.getAtomsFields();
                        aggSum(aggResult,inputBean,atomsFields); // 获取inputBean对应的属性 然后累计到aggResult
                    }
                    if (AutoCalFunctionConstant.aggCount.equals(aggType)){ // 求 次数 的计算
                        List<AtomsField> atomsFields = aggFieldCompute.getAtomsFields();
                        aggCount(aggResult,inputBean,atomsFields); // 获取inputBean对应的属性 然后累计到aggResult
                    }
                }
            }
        }
        // 计算完成之后 统一把 aggResult里面的值赋值给jsonResult
        jsonResult.putAll(aggResult);
    }

    /*
        计算次数
     */
    private static void aggCount(Map<String, Double> aggResult, KafkaVcenterInputBean inputBean,
                                 List<AtomsField> atomsFields) {
        for (AtomsField atomsField : atomsFields) {
            String fieldName = atomsField.getFieldName();
            Double cachedResult = aggResult.getOrDefault(fieldName,0D);
            aggResult.put(fieldName,cachedResult+1);
        }
    }

    /*
        求和
     */
    private static void aggSum(Map<String, Double> aggResult, KafkaVcenterInputBean inputBean,
                               List<AtomsField> atomsFields) {
        for (AtomsField atomsField : atomsFields) {
            String fieldName = atomsField.getFieldName();
            String getFieldExpression = atomsField.getGetFieldExpression();
            double resultValue = AutoMergeOgnlUtils.getDoubleValue(getFieldExpression,inputBean);
            double cachedResult = aggResult.getOrDefault(fieldName,0D);
            aggResult.put(fieldName,cachedResult+resultValue);
        }
    }


}
