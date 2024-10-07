package com.moonpac.realtime.common.bean.vcenter;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class HorizonParam {
    private String horizonIp;
    private String url;
    private String vcenter;
    private String domain;
    private String username;
    private String password;
    private Integer intervalSecond;
}
