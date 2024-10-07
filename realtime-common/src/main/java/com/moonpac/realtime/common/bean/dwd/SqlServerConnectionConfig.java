package com.moonpac.realtime.common.bean.dwd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

import java.io.Serializable;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SqlServerConnectionConfig implements Serializable {
    private String name;
    private String ip;
    private int port;
    private String username;
    private String password;
    private String database;
    private String table;

    private String vcenter;
}
