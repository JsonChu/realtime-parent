package com.moonpac.realtime.dim.source;

import com.moonpac.realtime.common.bean.dim.HostInfo;
import com.moonpac.realtime.common.util.DorisJdbcUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * @author zhanglingxing
 * @date 2024/4/15 11:26
 * 1.读取doris中对应的vcenter的配置 并发为1
 */
@Slf4j
public class DorisHostPythonSource extends RichSourceFunction<HostInfo> {
    volatile boolean flag = true;
    @Override
    public void run(SourceContext<HostInfo> ctx) throws Exception {

        // 获取doris配置
        Map<String, String> dorisConf  = getRuntimeContext()
                                            .getExecutionConfig()
                                            .getGlobalJobParameters()
                                            .toMap();
        String sql = "select vcenter,region,moid as host_id,memory_size as cs_memory_sizemb, \n" +
                     "cpu_cpucoresnum as cs_cpu_core_count, cpu_totalcapacity as cs_cpu_mhz \n" +
                     ",CONCAT(vcenter,'_',moid)  as event_partition_key\n" +
                     "from vcenter_db.dwd_host_python_latest";
        while (flag) {
            // 预加载初始的维度表信息
            Connection dorisConnection = DorisJdbcUtils.getDorisConnection(dorisConf);
            List<HostInfo> tableProcessDims = DorisJdbcUtils.queryList(dorisConnection,sql, HostInfo.class, true);
            for (HostInfo csParam : tableProcessDims) {
                ctx.collect(csParam);
            }
            DorisJdbcUtils.closeConnection(dorisConnection);
            TimeUnit.SECONDS.sleep(Integer.parseInt(dorisConf.getOrDefault("doris.query.interval","300")));
        }
    }
    @Override
    public void cancel() {
        flag = false;
    }

}
