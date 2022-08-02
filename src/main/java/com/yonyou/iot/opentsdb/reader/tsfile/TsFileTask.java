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
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    private DateTime dateTime;
    //tsdb连接，获取zookeeper地址
    private OpenTSDBConnection conn;
    //数据模型的tagMap
    private Map<String, MetricTagMeta> tagMap;
    //需写入tsFile的tablet
    private Tablet tablet;

    public TsFileTask(String exportPath, String storageGroup, Map<String, MetricTagMeta> tagMap, Map<String, List<String>> metricMap, DateTime dateTime, OpenTSDBConnection conn) {
        this.exportPath = exportPath;
        this.storageGroup = storageGroup;
        this.metricMap = metricMap;
        this.dateTime = dateTime;
        this.conn = conn;
        this.tagMap = tagMap;
    }

    @Override
    public void run() {
        metricMap.entrySet().stream().forEach(device -> {
            Stopwatch watch = Stopwatch.createUnstarted();
            LOG.info("任务执行开始:device->{},dateTime->{}.",device.getKey(),dateTime);
            try {
                watch.start();
                long startTime = dateTime.getMillis();
                long endTime = dateTime.plusHours(1).getMillis();
                List<MetricData> dataList = device.getValue().stream().parallel().flatMap(metric -> {
                    try {
                        List<MetricData> childList = OpenTSDBFactory.dump(conn, metric, startTime, endTime- 1);
                        LOG.info("metric ->{}, count->{}", metric, childList.size());
                        return childList.stream();
                    } catch (Exception e) {
                        LOG.error("scan数据失败,metric:{}",metric,e);
                        throw new IoTDBSdkRuntimeException(e);
                    }
                }).collect(Collectors.toList());
                watch.stop();
                watch.start();
                LOG.info("dataList size ->{}, 耗时->{}", dataList.size(), watch);
                //生成tablet
                buildTablet(device, dataList);
                //写入tsFile
                String fileName = getTsFileName(exportPath, storageGroup, device.getKey(), startTime);
                LOG.info("fileName ->{}, 耗时->{}", fileName, watch);
                File f = new File(fileName);
                if (!f.getParentFile().exists()) {
                    f.getParentFile().mkdirs();
                }
                TsFileWriter writer = new TsFileWriter(f);
                writer.write(tablet);
                writer.close();
                watch.stop();
                LOG.info("device:{},start->{},end->{},数据量->{},耗时->{}",device.getKey(),startTime,endTime,dataList.size(), watch);
            } catch (Exception e) {
                LOG.error("任务执行失败:",e);
                throw new IoTDBSdkRuntimeException(e);
            }
        });
    }

    private String getTsFileName(String exportPath, String storageGroup, String device, long startTime){
        String filePath = String.format(exportPath, storageGroup, 0, 0);
        String fileName = startTime + "-0-0-0.tsfile";
        return filePath.concat(device).concat("/").concat(fileName);
    }

    private void buildTablet(Map.Entry<String, List<String>> device, List<MetricData> dataList){
        //以时间为纬度的插入数据结构
        Map<Long, Map<String, MetricData>> dataMap = new HashMap<>();
        //时间序列结构详情
        Map<String, TimeseriesInfo> timeSeriesInfoMap = new HashMap<>();
        dataList.stream().forEach(metricData -> {
            MetricTagMeta metricTagMeta = tagMap.get(metricData.getMetric());
            TimeseriesInfo timeseriesInfo =
                    buildTimeseriesByMetricData(
                            storageGroup, metricTagMeta, metricTagMeta.getTagOrderList(), metricData.getTags());

            Map<String, MetricData> timeDataMap =
                    dataMap.computeIfAbsent(metricData.getTimestamp(), k -> new HashMap<>());
            timeDataMap.put(timeseriesInfo.getMeasurement(), metricData);
            timeSeriesInfoMap.put(timeseriesInfo.getFullPath(), timeseriesInfo);
        });

        List<MeasurementSchema> schemaList =
                timeSeriesInfoMap.values().stream()
                        .map(t -> new MeasurementSchema(t.getMeasurement(), t.getType(), t.getEncoding(), t.getCompressor()))
                        .collect(Collectors.toList());
        tablet = new Tablet(device.getKey(), schemaList, dataMap.size());
        tablet.initBitMaps();
        for (Map.Entry<Long, Map<String, MetricData>> entry : dataMap.entrySet()) {
            int rowIndex = tablet.rowSize++;
            tablet.addTimestamp(rowIndex, entry.getKey());
            int tIndex = 0;
            for (TimeseriesInfo t : timeSeriesInfoMap.values()) {
                MetricData metricData = entry.getValue().get(t.getMeasurement());
                if (metricData == null) {
                    tablet.bitMaps[tIndex].mark(rowIndex);
                    tablet.addValue(t.getMeasurement(), rowIndex, null);
                } else {
                    tablet.addValue(
                            t.getMeasurement(), rowIndex, null);
                }
                tIndex++;
            }
        }
    }

    private TimeseriesInfo buildTimeseriesByMetricData(
            String storageGroup,
            MetricTagMeta metaInfo,
            List<MetricTagOrder> tagOrderList,
            Map<String, String> metricTag) {
        // 根据元数据时间序列中存储的Tag顺序信息，进行业务数据时间序列的组装
        TimeseriesInfo dataTimeseries = new TimeseriesInfo();
        String measurement = KeywordUtil.metric(metaInfo.getMetric());
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
