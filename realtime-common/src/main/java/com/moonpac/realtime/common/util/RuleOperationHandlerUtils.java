package com.moonpac.realtime.common.util;

import com.moonpac.realtime.common.bean.metric.MetricRuleInfo;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.flink.api.common.state.BroadcastState;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RuleOperationHandlerUtils {

    /**
     * 指标规则操作处理入口方法
     */
    public static void handleMetricRuleOper(int indexOfThisSubtask, MetricRuleInfo metricRuleInfo,
                                            BroadcastState<String, List<MetricRuleInfo>> broadcastState) {

        try {

            String op = metricRuleInfo.getOp();   // +U -D
            String ruleId = metricRuleInfo.getMetricId();
            String metricName = metricRuleInfo.getMetricName();
            int ruleStatus = metricRuleInfo.getMetricStatus();
            String objectTypeId = metricRuleInfo.getObjectTypeId();
            String fieldName = metricRuleInfo.getFieldName();
            String metricCountFormula = metricRuleInfo.getMetricCountFormula();

            List<MetricRuleInfo> metricRuleInfos = broadcastState.get(objectTypeId);
            if (null == metricRuleInfos){
                metricRuleInfos  = new ArrayList<>();
            }
            // 新增的规则 或者 初始化加载上线的规则
            if ( ( "+I".equals(op) || "+U".equals(op) ) && ruleStatus == 1 ) {

                boolean f = false;
                for (MetricRuleInfo metricRule : metricRuleInfos) {
                    if (metricRule.getMetricId().equals(ruleId)){
                        if (metricRuleInfo.getUpdateTime() > metricRule.getUpdateTime()){
                            // 替换之前的规则
                            BeanUtils.copyProperties(metricRule,metricRuleInfo);
                            f = true;
                        }else{
                            return;
                        }
                    }
                }
                if (!f){ // 正常添加
                    metricRuleInfos.add(metricRuleInfo);
                }
                broadcastState.put(objectTypeId,metricRuleInfos);
                log.info("subtask={},启用规则={},对象类型={},指标id={},指标name={},指标字段={},指标表达式={}",
                        indexOfThisSubtask,op,objectTypeId,ruleId,metricName,fieldName,metricCountFormula);
            }else if ( ( "+U".equals(op) && ruleStatus == 0 ) || "-D".equals(op) ) {
                // 删除下线的规则
                metricRuleInfos.removeIf(next -> ruleId.equals(next.getMetricId()));
                broadcastState.put(objectTypeId,metricRuleInfos);
                log.info("subtask={},停用规则={},对象类型={},指标id={},指标name={},指标字段={},指标表达式={}",
                        indexOfThisSubtask,op,objectTypeId,ruleId,metricName,fieldName,metricCountFormula);
            }

        } catch (Exception e) {
            log.error("规则处理出现异常,异常信息: \n {}",e.getMessage());
        }
    }


}
