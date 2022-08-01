package com.yonyou.iot.opentsdb.reader.conn;

import cn.hutool.core.text.CharSequenceUtil;
import com.google.common.collect.Lists;
import com.yonyou.iotdb.metric.domain.MetricData;
import net.opentsdb.core.*;
import net.opentsdb.core.Internal.Cell;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public final class DumpSeries {

    private DumpSeries(){}
    private static final List<String> TID_LIST = Lists.newArrayList("iOwdNRpAxy2751868226,DFNDSFyFSr0835468931,xkgaNePgDy3006426242,PeEGPbGdnV8112743975,KfaBQkekjh8308138184,sfvyoZdIXz6986238859,befbAhAjaE6919882905,BUqylGKCyJ2501368296,BpcCtvPnLo1497966353,AxlJmjhWEL3995801226,AHSxesoqMe3716207755,BDwYgRnQEi1044762872,AIGmSkBAkv2458968842,AvkWhFUIhe7141912163".split(","));

    private static final Logger LOG = LoggerFactory.getLogger(DumpSeries.class);

    static List<MetricData> doDump(TSDB tsdb, String[] args) throws Exception {
        final ArrayList<Query> queries = new ArrayList<>();
        CliQuery.parseCommandLineQuery(args, tsdb, queries);

        List<MetricData> dps = new ArrayList<>();
        for (final Query query : queries) {
            final List<Scanner> scanners = Internal.getScanners(query);
            for (Scanner scanner : scanners) {
                ArrayList<ArrayList<KeyValue>> rows;
                while ((rows = scanner.nextRows().join()) != null) {
                    for (final ArrayList<KeyValue> row : rows) {
                        final byte[] key = row.get(0).key();
                        final long baseTime = Internal.baseTime(tsdb, key);
                        final String metric = Internal.metricName(tsdb, key);
                        for (final KeyValue kv : row) {
                            formatKeyValue(dps, tsdb, kv, baseTime, metric);
                        }
                    }
                }
            }
        }
        return dps;
    }

    /**
     * Parse KeyValue into data points.
     */
    private static void formatKeyValue(final List<MetricData> dps, final TSDB tsdb,
                                       final KeyValue kv, final long baseTime, final String metric) {
        Map<String, String> tagKVs = Internal.getTags(tsdb, kv.key());
//        String tid = tagKVs.get("tid");
//        if(CharSequenceUtil.isNotEmpty(tid) && TID_LIST.contains(tid)){
//            return;
//        }
        final byte[] qualifier = kv.qualifier();
        final int qLen = qualifier.length;

        if (!AppendDataPoints.isAppendDataPoints(qualifier) && qLen % 2 != 0) {
            // custom data object, not a data point
            if (LOG.isDebugEnabled()) {
                LOG.debug("Not a data point");
            }
        } else if (qLen == 2 || qLen == 4 && Internal.inMilliseconds(qualifier)) {
            // regular data point
            final Cell cell = Internal.parseSingleValue(kv);
            if (cell == null) {
                throw new IllegalDataException("Unable to parse row: " + kv);
            }
            MetricData metricData = new MetricData();
            metricData.setMetric(metric);
            metricData.setValue(cell.parseValue());
            metricData.setTags(tagKVs);
            metricData.setTimestamp(cell.absoluteTimestamp(baseTime));
            dps.add(metricData);
        } else {
            final Collection<Cell> cells;
            if (qLen == 3) {
                // append data points
                cells = new AppendDataPoints().parseKeyValue(tsdb, kv);
            } else {
                // compacted column
                cells = Internal.extractDataPoints(kv);
            }
            for (Cell cell : cells) {
                MetricData metricData = new MetricData();
                metricData.setMetric(metric);
                metricData.setValue(cell.parseValue());
                metricData.setTags(tagKVs);
                metricData.setTimestamp(cell.absoluteTimestamp(baseTime));
                dps.add(metricData);
            }
        }
    }
}
