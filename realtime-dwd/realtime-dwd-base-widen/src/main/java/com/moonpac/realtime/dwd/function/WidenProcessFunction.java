package com.moonpac.realtime.dwd.function;

import com.alibaba.fastjson.JSON;
import com.moonpac.realtime.common.bean.ods.KafkaVcenterInputBean;
import com.moonpac.realtime.common.constant.EventNameConstant;
import com.moonpac.realtime.common.util.DateUtils;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import java.util.Map;
public class WidenProcessFunction extends ProcessFunction<String, KafkaVcenterInputBean> {

    @Override
    public void processElement(String line, ProcessFunction<String, KafkaVcenterInputBean>.Context context,
                               Collector<KafkaVcenterInputBean> out) throws Exception {

        KafkaVcenterInputBean kafkaVcenterInputBean = JSON.parseObject(line, KafkaVcenterInputBean.class);
        String eventName = kafkaVcenterInputBean.getEventName();
        long eventTimestamp = kafkaVcenterInputBean.getEventTimestamp();

        Map<String, String> eventTags = kafkaVcenterInputBean.getEventTags();
        String uniqueKey = eventTags.get("vcenter") + "_" + eventTags.get("moid") + "_" + eventName + "_" + eventTimestamp;

        switch (eventName) {
            case EventNameConstant.VSPHERE_VM_CPU:
            case EventNameConstant.VSPHERE_HOST_CPU:
                uniqueKey = uniqueKey + "_" + eventTags.get("cpu");
                break;
            case EventNameConstant.VSPHERE_VM_DATASTORE:
            case EventNameConstant.VSPHERE_HOST_DATASTORE:
            case EventNameConstant.VSPHERE_DATASTORE_DISK:
                uniqueKey = uniqueKey + "_" + eventTags.get("dsname");
                break;
            case EventNameConstant.VSPHERE_VM_DISK:
            case EventNameConstant.VSPHERE_HOST_DISK:
                uniqueKey = uniqueKey + "_" + eventTags.get("disk");
                break;
            case EventNameConstant.VSPHERE_VM_MEM:
            case EventNameConstant.VSPHERE_VM_SYS:
                uniqueKey = uniqueKey + "_" + eventName;
                break;
            case EventNameConstant.VSPHERE_VM_NET:
            case EventNameConstant.VSPHERE_HOST_NET:
                uniqueKey = uniqueKey + "_" + eventTags.get("interface");
                break;
            case EventNameConstant.VSPHERE_HOST_MEM:
                uniqueKey = uniqueKey + "_" + eventTags.get("instance");
                break;
            case EventNameConstant.VSPHERE_DATASTORE_DATASTORE:
                uniqueKey = uniqueKey + "_" + eventTags.get("lun");
                break;
            default:
                // Handle default case if needed
        }

        kafkaVcenterInputBean.setUniqueKey(uniqueKey);
        kafkaVcenterInputBean.setEventPartitionKey(eventTags.get("vcenter") + "_" + eventTags.get("moid"));

        if (EventNameConstant.VM_TABLES.contains(eventName)){
            kafkaVcenterInputBean.setObjectType("vm");
            out.collect(kafkaVcenterInputBean);
        }else if (EventNameConstant.HOST_TABLES.contains(eventName)){
            kafkaVcenterInputBean.setObjectType("host");
            out.collect(kafkaVcenterInputBean);
        }else if ("vsphere_datastore_disk".equals(eventName)){ // dataStore的容量
            /*
                这个时间很大的bug。例如 可能一会是 2024-02-01的数据 一会是2024-04-10的数据 很奇怪 对后面的程序造成很大的bug。
                    这两天数据都属于 vcneter的历史统计数据，

                    vcenter的更新频率是5分钟更新一次，
                    而且 dataStore_disk的数据时间不一样，
                    举例：datastore-10 datastore-15 同一次调用，返回的数据有可能不一样

                    为什么需要单独说明个事情
                    以为我们采集vcenter对应的vm和host的性能指标数据的时候，同一次telegraf请求，返回
                    的vm和host对应的eventDate是一样的

                    因为现在这种差异，我们需要单独对dataStore的数据出额外的处理，才能不影响，
                    后续的程序逻辑，因为他们在使用层面来说是一样的
             */
            kafkaVcenterInputBean.setEventTimestamp(DateUtils.getCurrentTimeRoundedToFiveMinutesInSeconds());
            kafkaVcenterInputBean.setObjectType("datastore");
            out.collect(kafkaVcenterInputBean);
        }else if ("vsphere_datastore_datastore".equals(eventName)){ // dataStore的读写性能
             /*
                这个时间很大的bug。例如 可能一会是 2024-02-01的数据 一会是2024-04-10的数据 很奇怪 对后面的程序造成很大的bug。
                    这两天数据都属于 vcneter的历史统计数据，

                    vcenter的更新频率是5分钟更新一次，
                    而且 dataStore_disk的数据时间不一样，
                    举例：datastore-10 datastore-15 同一次调用，返回的数据有可能不一样

                    为什么需要单独说明个事情
                    以为我们采集vcenter对应的vm和host的性能指标数据的时候，同一次telegraf请求，返回
                    的vm和host对应的eventDate是一样的

                    因为现在这种差异，我们需要单独对dataStore的数据出额外的处理，才能不影响，
                    后续的程序逻辑，因为他们在使用层面来说是一样的
             */
            kafkaVcenterInputBean.setEventTimestamp(DateUtils.getCurrentTimeRoundedToFiveMinutesInSeconds());
            kafkaVcenterInputBean.setObjectType("datastore");
            out.collect(kafkaVcenterInputBean);
        }

    }
}
