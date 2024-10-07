package com.moompac.feature.metric.engine.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.moonpac.realtime.common.bean.dws.MergeEventBean;
import com.moonpac.realtime.common.bean.metric.MetricRuleInfo;
import com.moonpac.realtime.common.util.FlinkStateDescUtils;
import com.moonpac.realtime.common.util.RuleOperationHandlerUtils;
import lombok.extern.slf4j.Slf4j;
import ognl.Ognl;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import java.text.DecimalFormat;
import java.util.List;

@Slf4j
public class MetricRuleBroadcastProcessFunction extends BroadcastProcessFunction<MergeEventBean, MetricRuleInfo, String> {
    private String  runMode;
    public MetricRuleBroadcastProcessFunction(String  runMode){
        this.runMode = runMode;
    }

    @Override
    public void processElement(MergeEventBean mergeEventBean, BroadcastProcessFunction<MergeEventBean,
            MetricRuleInfo, String>.ReadOnlyContext ctx, Collector<String> out ) throws Exception {

        ReadOnlyBroadcastState<String, List<MetricRuleInfo>> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.metricRuleStateDesc);

        String objecttype = mergeEventBean.getObjecttype();
        // 获取数据对应的指标规则组
        List<MetricRuleInfo> metricRuleInfos = broadcastState.get(objecttype);

        if (metricRuleInfos == null || metricRuleInfos.size() == 0) return;

        JSONObject jsonObject = new JSONObject();
        DecimalFormat format = new DecimalFormat("#.0000");
        StringBuilder sb = new StringBuilder();
        for (MetricRuleInfo metricRuleInfo : metricRuleInfos) { // 根据规则 动态生成表达式
            String metricCountFormula = metricRuleInfo.getMetricCountFormula();
            String fieldName = metricRuleInfo.getFieldName();
            String metricId = metricRuleInfo.getMetricId();
            sb.append(metricId);
            sb.append(",");
            try {
                // 执行条件的返回
                Double result = Double.parseDouble(Ognl.getValue(Ognl.parseExpression(metricCountFormula), mergeEventBean).toString());
                jsonObject.put(fieldName,Double.parseDouble(format.format(result)));
            }catch (Exception e){
                log.error("指标计算异常 对象类型={}，指标id={}，指标name={},指标表达式：{}，日志数据={},异常日志={}"
                        ,objecttype,metricId,fieldName,metricCountFormula,mergeEventBean,e.getMessage());
            }
        }

        if ( jsonObject.keySet().size() > 0 ){
            jsonObject.put("objecttype",objecttype);
            jsonObject.put("vcenter",mergeEventBean.getVcenter());
            switch (objecttype) {
                case "vm":
                    jsonObject.put("moid",mergeEventBean.getVmmoid());
                    break;
                case "host":
                    jsonObject.put("moid",mergeEventBean.getHostmoid());
                    break;
                case "datastore":
                    jsonObject.put("moid",mergeEventBean.getDsmoid());
                    break;
                case "vcenter":
                    jsonObject.put("moid",mergeEventBean.getVcmoid());
                    break;
                case "database":
                    jsonObject.put("moid",mergeEventBean.getDbmoid());
                    break;
            }
            jsonObject.put("processtime",System.currentTimeMillis());
            jsonObject.put("eventdate",mergeEventBean.getEventdate());
            jsonObject.put("metricids", sb.substring(0, sb.length() - 1));
            if ("test".equals(runMode)){
                jsonObject.put("logdata",mergeEventBean);
            }
            out.collect(JSON.toJSONString(jsonObject));
        }
    }

    @Override
    public void processBroadcastElement(MetricRuleInfo metricRuleInfo, BroadcastProcessFunction<MergeEventBean,
            MetricRuleInfo, String>.Context ctx, Collector<String> collector) throws Exception {

        int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

        BroadcastState<String, List<MetricRuleInfo>> broadcastState =
                ctx.getBroadcastState(FlinkStateDescUtils.metricRuleStateDesc);

        // 将规则封装到 广播变量中
        RuleOperationHandlerUtils.handleMetricRuleOper(indexOfThisSubtask,metricRuleInfo,broadcastState);

    }
}
