package com.moonpac.realtime.common.bean.dwd;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class SqlServerAfterEventData {

    private int EventID;
    private String Module;
    private String EventType;
    private String ModuleAndEventText;
    private Long Time;
    private String Source;
    private String Severity;
    private String Node;
    private int Acknowledged;
    private String UserSID;
    private String DesktopId;
    private String MachineId;
    private String FolderPath;
    private String LUNId;
    private String ThinAppId;
    private String EndpointId;
    private String UserDiskPathId;
    private String GroupId;
    private String ApplicationId;
    private String SessionId;

    // 2024-03-19
    private String csip;
    private String vcenter;

}
