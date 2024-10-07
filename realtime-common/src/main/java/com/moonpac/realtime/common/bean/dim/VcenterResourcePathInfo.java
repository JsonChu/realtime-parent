package com.moonpac.realtime.common.bean.dim;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.util.Date;

/**
 * 智能运维vm/host路径信息表;
 *
 * @author : wenss
 * @date : 2024-03-25
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class VcenterResourcePathInfo {
    private String vcenter;
    private String moid;
    private String type;
    private String name;
    private String path;
    private String collectHost;
    private String configName;
    private Date insertDate;
}
