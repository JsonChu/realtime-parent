package com.moonpac.realtime.common.bean.dwd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class DatabaseParam {
    private String vcenter;
    private String region;
    private String dbMoid;
    private String dbType;
    private String dbName;
    private String url;
    private String username;
    private String password;
    private Integer intervalSecond;

}
