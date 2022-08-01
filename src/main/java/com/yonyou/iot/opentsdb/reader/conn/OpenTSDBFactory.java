package com.yonyou.iot.opentsdb.reader.conn;

import com.alibaba.fastjson.JSON;
import com.yonyou.iotdb.metric.domain.MetricData;
import net.opentsdb.core.TSDB;
import net.opentsdb.utils.Config;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

public final class OpenTSDBFactory {

    private static final TSDB TSDB_INSTANCE;

    static {
        try{
            Config config = new Config(false);
            File file = Paths.get("opentsdb.json").toFile();
            String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
            Map configurations = JSON.parseObject(content, Map.class);
            for (Object key : configurations.keySet()) {
                config.overrideConfig(key.toString(), configurations.get(key.toString()).toString());
            }
            TSDB_INSTANCE = new TSDB(config);
        } catch (Exception e) {
            throw new RuntimeException("Cannot init OpenTSDB connection!");
        }
    }

    private OpenTSDBFactory() {
    }

    public static List<MetricData> dump(OpenTSDBConnection conn, String metric, Long start, Long end) throws Exception {
        return DumpSeries.doDump(getTSDB(conn), new String[]{start + "", end + "", "none", metric});
    }

    private static TSDB getTSDB(OpenTSDBConnection conn) {
        return TSDB_INSTANCE;
    }
}
