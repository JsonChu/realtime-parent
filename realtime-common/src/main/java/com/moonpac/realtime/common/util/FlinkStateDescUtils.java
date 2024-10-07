package com.moonpac.realtime.common.util;

import com.moonpac.realtime.common.bean.dim.HostInfo;
import com.moonpac.realtime.common.bean.dim.VcenterInfo;
import com.moonpac.realtime.common.bean.dim.VmInfo;
import com.moonpac.realtime.common.bean.dwd.SqlServerAfterEventData;
import com.moonpac.realtime.common.bean.dwd.VmLifeCycleBean;
import com.moonpac.realtime.common.bean.dws.AggBean;
import com.moonpac.realtime.common.bean.dws.AggBeanState;
import com.moonpac.realtime.common.bean.dws.meger.MergeAutoCompute;
import com.moonpac.realtime.common.bean.metric.MetricRuleInfo;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.util.List;
import java.util.Set;
import java.util.TreeSet;

public class FlinkStateDescUtils {

    public static MapStateDescriptor<String, VmInfo> getHttpVMInfoBroadcastDesc
            = new MapStateDescriptor<>("http_vm_info_broadcast_state", TypeInformation.of(String.class),
            TypeInformation.of(VmInfo.class));

    public static MapStateDescriptor<String, HostInfo> getHttpCSHostInfoBroadcastDesc
            = new MapStateDescriptor<>("http_host_info_broadcast_state", TypeInformation.of(String.class),
            TypeInformation.of(HostInfo.class));

    public static MapStateDescriptor<String, VcenterInfo> getDIMVcenterVMInfoBroadcastDesc
            = new MapStateDescriptor<>("http_vcenter_info_broadcast_state", TypeInformation.of(String.class),
            TypeInformation.of(VcenterInfo.class));

    // 自动合并
    public static MapStateDescriptor<String, MergeAutoCompute> mergeAutoComputeBroadcastDesc
            = new MapStateDescriptor<>("merge_auto_compute_broadcast_state", TypeInformation.of(String.class),
            TypeInformation.of(MergeAutoCompute.class));
    public static MapStateDescriptor<Long, Set<KafkaVcenterInputBean>>  getWidenEventBeansStateSetDesc(){
        MapStateDescriptor<Long, Set<KafkaVcenterInputBean>>  mapStateDescriptor = new MapStateDescriptor<>("MergeAutoKafkaVcenterInputBean",
                TypeInformation.of(Long.class), TypeInformation.of(new TypeHint<Set<KafkaVcenterInputBean>>() {}));
        return mapStateDescriptor;
    }
    public static ValueStateDescriptor<TreeSet<Long>> mergeAutoSendEventDates
            = new ValueStateDescriptor<>("mergeAutoSendEventDates"
            ,TypeInformation.of(new TypeHint<TreeSet<Long>>() {}));
    public static ValueStateDescriptor<Boolean> firstRegisterOnTimerValueStateDes
            = new ValueStateDescriptor<>("firstRegisterOnTimer"
            ,TypeInformation.of(Boolean.class));
    public static MapStateDescriptor<Long,Integer> onTimeCheckResultMapStateDesc
            = new MapStateDescriptor<>("onTimeCheckResult"
            ,TypeInformation.of(Long.class),TypeInformation.of(Integer.class));

    // 容量预测
    public static MapStateDescriptor<Long, Set<AggBean>>  getKeyedStateSetDesc(){
        MapStateDescriptor<Long, Set<AggBean>>  mapStateDescriptor = new MapStateDescriptor<>("getKeyedStateSetDesc",
                TypeInformation.of(Long.class), TypeInformation.of(new TypeHint<Set<AggBean>>() {}));
        return mapStateDescriptor;
    }
    public static MapStateDescriptor<Long, AggBeanState> getAGGHourTJStateDesc
            = new MapStateDescriptor<>("cluster_agg_hour_tj_datastore_state"
            ,TypeInformation.of(Long.class),TypeInformation.of(AggBeanState.class));


    // 指标引擎  <触发事件ID，list<多条规则分组对象>>
    public static MapStateDescriptor<String, List<MetricRuleInfo>> metricRuleStateDesc
            = new MapStateDescriptor<>("metric_rule_broadcast_state"
            ,TypeInformation.of(String.class)
            ,TypeInformation.of(new TypeHint<List<MetricRuleInfo>>() {}));

    // vm 生命周期
    public static ValueStateDescriptor<VmLifeCycleBean>  getVmLifeCycleValueStateDes
            = new ValueStateDescriptor<>("getVmLifeCycleValueStateDes"
            ,TypeInformation.of(VmLifeCycleBean.class));

    // 维度合并 等待http的数据
    public static ValueStateDescriptor<VmInfo> pythonVmInfoStateDes
            = new ValueStateDescriptor<>("pythonVminfo"
            ,TypeInformation.of(VmInfo.class));
    public static ValueStateDescriptor<HostInfo> vcenterHostInfoStateDes
            = new ValueStateDescriptor<>("pythonVminfo"
            ,TypeInformation.of(HostInfo.class));

    public static ValueStateDescriptor<Integer> attemptsVmNumberStateDes
            = new ValueStateDescriptor<>("attemptsVmNumberStateDes"
            ,TypeInformation.of(Integer.class));
    public static ValueStateDescriptor<Integer> attemptsHostNumberStateDes
            = new ValueStateDescriptor<>("attemptsHostNumberStateDes"
            ,TypeInformation.of(Integer.class));

    // vdi最新状态程序
    public static ValueStateDescriptor<SqlServerAfterEventData>  getVdiLatestStatusValueStateDes
            = new ValueStateDescriptor<>("getVmLifeCycleValueStateDes"
            ,TypeInformation.of(SqlServerAfterEventData.class));

}
