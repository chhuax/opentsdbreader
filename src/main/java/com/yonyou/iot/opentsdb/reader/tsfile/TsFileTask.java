package com.yonyou.iot.opentsdb.reader.tsfile;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.text.CharSequenceUtil;
import com.google.common.base.Stopwatch;
import com.yonyou.iot.opentsdb.reader.conn.OpenTSDBConnection;
import com.yonyou.iot.opentsdb.reader.conn.OpenTSDBFactory;
import com.yonyou.iotdb.domain.TimeseriesInfo;
import com.yonyou.iotdb.exception.IoTDBSdkRuntimeException;
import com.yonyou.iotdb.metric.domain.MetricData;
import com.yonyou.iotdb.metric.domain.MetricTagMeta;
import com.yonyou.iotdb.metric.domain.MetricTagOrder;
import com.yonyou.iotdb.utils.KeywordUtil;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.yonyou.iotdb.constant.Constant.NULL_TAG_PLACEHOLDER;

public class TsFileTask implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(TsFileTask.class);
    //文件导出路径
    private String exportPath;
    //存储组
    private String storageGroup;
    //指标
    private Map<String, List<String>> metricMap;
    //开始时间
    private DateTime startTime;
    //结束时间
    private DateTime endTime;
    //tsdb连接，获取zookeeper地址
    private OpenTSDBConnection conn;
    //数据模型的tagMap
    private Map<String, MetricTagMeta> tagMap;

    public TsFileTask(String exportPath, String storageGroup, Map<String, MetricTagMeta> tagMap, Map<String, List<String>> metricMap, DateTime startTime, DateTime endTime, OpenTSDBConnection conn) {
        this.exportPath = exportPath;
        this.storageGroup = storageGroup;
        this.metricMap = metricMap;
        this.startTime = startTime;
        this.endTime = endTime;
        this.conn = conn;
        this.tagMap = tagMap;
    }

    @Override
    public void run() {
        Stopwatch watch = Stopwatch.createUnstarted();
        metricMap.entrySet().stream().forEach(device -> {
            DateTime taskStartTime = new DateTime(startTime);
            while (taskStartTime.isBefore(endTime)){
                LOG.info("任务执行开始:device->{},dateTime->{}.",device.getKey(),taskStartTime);
                DateTime taskEndTime = taskStartTime.plusHours(1);
                executeTask(device, taskStartTime.getMillis(), taskEndTime.getMillis(), watch);
                taskStartTime = taskEndTime;
            }
        });
    }

    private void executeTask(Map.Entry<String, List<String>> device, long taskStartTime, long taskEndTime, Stopwatch watch){
        try {
            watch.start();
            List<MetricData> dataList = device.getValue().stream().parallel().flatMap(metric -> {
                try {
                    if(metric.startsWith("metric_")){
                        metric = metric.replace("metric_", "");
                    }
                    return OpenTSDBFactory.dump(conn, metric, taskStartTime, taskEndTime- 1).stream();
                } catch (Exception e) {
                    LOG.error("scan数据失败,metric:{}",metric,e);
                    throw new IoTDBSdkRuntimeException(e);
                }
            }).collect(Collectors.toList());
            if(dataList == null || dataList.isEmpty()){
                return;
            }
            watch.stop();
            watch.start();
            //创建tsFile
            String fileName = getTsFileName(exportPath, storageGroup, device.getKey(), taskStartTime);
            File f = new File(fileName);
            if (!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
            TsFileWriter writer = new TsFileWriter(f);
            //生成tablet
            buildTablet(dataList, writer);
            writer.close();
            watch.stop();
            LOG.info("device:{},start->{},end->{},数据量->{},耗时->{}",device.getKey(),taskStartTime,taskEndTime,dataList.size(), watch);
            watch.reset();
        } catch (Exception e) {
            LOG.error("任务执行失败:",e);
            throw new IoTDBSdkRuntimeException(e);
        }
    }

    private String getTsFileName(String exportPath, String storageGroup, String device, long startTime){
        String filePath = String.format(exportPath, storageGroup, 0, 0);
        String fileName = startTime + "-0-0-0.tsfile";
        return filePath.concat(device).concat("/").concat(fileName);
    }

    private void buildTablet(List<MetricData> dataList, TsFileWriter writer) throws IOException, WriteProcessException {
        //以设备-时间为纬度的插入数据结构
        Map<String, Map<Long, Map<String, MetricData>>> dataMap = new HashMap<>();
        //设备-时间序列结构详情
        Map<String, Map<String, TimeseriesInfo>> timeSeriesInfoMap = new HashMap<>();
        dataList.forEach(metricData -> {
            MetricTagMeta metricTagMeta = tagMap.get(metricData.getMetric());
            if(metricTagMeta == null){
                LOG.error("metric:{} not exit", metricData.getMetric());
                return;
            }
            TimeseriesInfo timeseriesInfo =
                    buildTimeseriesByMetricData(
                            storageGroup, metricTagMeta, metricTagMeta.getTagOrderList(), metricData.getTags());

            metricData.setMetric(timeseriesInfo.getMeasurement());
            Map<Long, Map<String, MetricData>> timeDataMap =
                    dataMap.computeIfAbsent(timeseriesInfo.getEntity(), k -> new HashMap<>());
            Map<String, MetricData> tDataMap =
                    timeDataMap.computeIfAbsent(metricData.getTimestamp(), k -> new HashMap<>());
            tDataMap.put(timeseriesInfo.getMeasurement(), metricData);

            timeSeriesInfoMap.computeIfAbsent(timeseriesInfo.getEntity(), k -> new HashMap<>());
            timeSeriesInfoMap
                    .get(timeseriesInfo.getEntity())
                    .put(timeseriesInfo.getFullPath(), timeseriesInfo);
        });

        for(Map.Entry<String, Map<Long, Map<String, MetricData>>> entry : dataMap.entrySet()){
            Map<Long, Map<String, MetricData>> childDataMap = entry.getValue();
            Map<String, TimeseriesInfo> childTimeseriesInfoMap = timeSeriesInfoMap.get(entry.getKey());
            List<MeasurementSchema> schemaList =
                    childTimeseriesInfoMap.values().stream()
                            .map(t -> new MeasurementSchema(t.getMeasurement(), t.getType(), t.getEncoding(), t.getCompressor()))
                            .collect(Collectors.toList());
            schemaList.forEach(schema -> {
                try {
                    writer.registerTimeseries(new Path(entry.getKey()), schema);
                } catch (WriteProcessException e) {
                    throw new RuntimeException(e);
                }
            });
            Tablet tablet = new Tablet(entry.getKey(), schemaList, childDataMap.size());
            tablet.initBitMaps();
            childDataMap.keySet().stream().sorted().forEach(key -> {
                int rowIndex = tablet.rowSize++;
                tablet.addTimestamp(rowIndex, key);
                int tIndex = 0;
                for (TimeseriesInfo t : childTimeseriesInfoMap.values()) {
                    MetricData metricData = childDataMap.get(key).get(t.getMeasurement());
                    if (metricData == null) {
                        tablet.bitMaps[tIndex].mark(rowIndex);
                        tablet.addValue(t.getMeasurement(), rowIndex, null);
                    } else {
                        tablet.addValue(
                                t.getMeasurement(), rowIndex, convertFun(t.getType()).apply(metricData.getValue()));
                    }
                    tIndex++;
                }
            });
            writer.write(tablet);
        }
    }

    private Function<Object, Object> convertFun(TSDataType dataType) {
        return o -> {
            switch (dataType) {
                case INT32:
                    return new BigDecimal(o.toString()).intValue();
                case INT64:
                    return new BigDecimal(o.toString()).longValue();
                case TEXT:
                    return String.valueOf(o.toString());
                case FLOAT:
                    return Float.parseFloat((String) o);
                case DOUBLE:
                    return Double.parseDouble(o.toString());
                case BOOLEAN:
                    return Boolean.parseBoolean(o.toString());
                default:
                    throw new IoTDBSdkRuntimeException("错误的数据类型");
            }
        };
    }

    private TimeseriesInfo buildTimeseriesByMetricData(
            String storageGroup,
            MetricTagMeta metaInfo,
            List<MetricTagOrder> tagOrderList,
            Map<String, String> metricTag) {
        // 根据元数据时间序列中存储的Tag顺序信息，进行业务数据时间序列的组装
        TimeseriesInfo dataTimeseries = new TimeseriesInfo();
        String measurement = KeywordUtil.metric(metaInfo.getMetric());
        if(measurement.equals("version") || measurement.equals("timestamp")){
            measurement = "metric_"+measurement;
        }
        dataTimeseries.setEntity(getDevicePath(storageGroup, tagOrderList, metricTag));
        dataTimeseries.setAlias(metaInfo.getMetric());
        dataTimeseries.setMeasurement(measurement);
        dataTimeseries.setStorageGroup(storageGroup);
        dataTimeseries.setType(metaInfo.getDataType().getType());
        dataTimeseries.setCompressor(metaInfo.getDataType().getCompressionType());
        dataTimeseries.setEncoding(metaInfo.getDataType().getEncoding());
        return dataTimeseries;
    }

    /**
     * 根据数据生成设备路径
     * @param storageGroup
     * @param tagOrderList
     * @return
     */
    private String getDevicePath(String storageGroup, List<MetricTagOrder> tagOrderList, Map<String, String> metricTag){
        StringBuilder builder = new StringBuilder(storageGroup);
        if (CollUtil.isNotEmpty(tagOrderList)) {
            tagOrderList.forEach(
                    tag -> {
                        String tagValue = metricTag.get(tag.getTagName());
                        if (CharSequenceUtil.isNotEmpty(tagValue)) {
                            builder.append(".").append(KeywordUtil.tag(tagValue));
                        } else {
                            // 新增的数据已有tag不存在时，采用默认占位符占位
                            builder.append(".").append(NULL_TAG_PLACEHOLDER);
                        }
                    });
        }
        return builder.toString();
    }

}
