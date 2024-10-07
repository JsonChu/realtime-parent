package com.moonpac.realtime.common.bean.dws;

import com.alibaba.fastjson.annotation.JSONField;
import com.moonpac.realtime.common.bean.dim.VcenterInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Map;

/**
 * @author zhanglingxing
 * @date 2023/12/21 10:44
 * 维度拉宽。开窗合并，去重的最终对象
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class WidenResultBean {
    private String objectType;
    private Long eventDate;
    @JSONField(alternateNames = {"vcenterInfo", "vcenterDIMInfo"}, name = "vcenterInfo")
    private VcenterInfo vcenterInfo;
    // 这个字段的不能用驼峰命名
    private MergeEventBean mergeEventBean;
    private Map<String,String> property;
    private Integer dataCount;
    private String key;

}
