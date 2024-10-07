package com.moonpac.realtime.dim.source;

import com.moonpac.realtime.common.bean.vcenter.VcenterParam;
import com.moonpac.realtime.common.util.DorisJdbcUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author zhanglingxing
 * @date 2024/02/28 13:50
 * 1.读取doris中对应的vcenter的配置 并发为1
 */

@Slf4j
public class DorisVcenterConfigParamSource extends RichSourceFunction<VcenterParam> {

    volatile boolean flag = true;

    @Override
    public void run(SourceContext<VcenterParam> ctx) throws Exception {
        // 获取doris配置
        Map<String, String> dorisConf  = getRuntimeContext()
                .getExecutionConfig()
                .getGlobalJobParameters()
                .toMap();
        int intervalSecond = Integer.parseInt(dorisConf.getOrDefault("doris.query.interval", "3600"));
        String  sql = " select CONCAT('https://',vcenter_ip) as url,username,`password`,region from vcenter_db.dim_vcenter_info";
        while (flag) {
            // 预加载初始的维度表信息
            Connection dorisConnection = DorisJdbcUtils.getDorisConnection(dorisConf);
            List<VcenterParam> tableProcessDims = DorisJdbcUtils.queryList(dorisConnection,sql, VcenterParam.class, true);
            for (VcenterParam param : tableProcessDims) {
                param.setIntervalSecond(intervalSecond);
                ctx.collect(param);
            }
            DorisJdbcUtils.closeConnection(dorisConnection);
            TimeUnit.SECONDS.sleep(intervalSecond);
        }

    }

    @Override
    public void cancel() {
        flag = false;
    }

}
