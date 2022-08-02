package com.yonyou.iot.opentsdb.reader.tsfile;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.json.JSONUtil;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.yonyou.iot.opentsdb.reader.conn.OpenTSDBConnection;
import com.yonyou.iot.opentsdb.reader.opentsdbreader.MetricMetaInfo;
import com.yonyou.iotdb.domain.DataType;
import com.yonyou.iotdb.exception.IoTDBSdkRuntimeException;
import com.yonyou.iotdb.metric.domain.MetricTagMeta;
import com.yonyou.iotdb.metric.domain.MetricTagOrder;
import com.yonyou.iotdb.utils.KeywordUtil;
import org.apache.commons.io.FileUtils;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class TsFile {
    private static final Logger LOG = LoggerFactory.getLogger(TsFile.class);
    private static final ExecutorService pool = new ThreadPoolExecutor(1, 1, 6000L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(1024), new ThreadFactoryBuilder().setNameFormat("ts-file-thread-local-pool-%d").build(), new ThreadPoolExecutor.AbortPolicy());

    public static void main(String[] args) {
        Properties properties = loadConfig();
        String exportPath = properties.getProperty("exportPath");
        String storageGroup = properties.getProperty("storageGroup");
        DateTime startTime = new DateTime(Long.parseLong(properties.getProperty("task.startTime")));
        DateTime endTime = new DateTime(Long.parseLong(properties.getProperty("task.endTime")));
        String address = properties.getProperty("task.otsserver");
        OpenTSDBConnection tsdbConn = new OpenTSDBConnection(address);
        Map<String, MetricTagMeta> tagMap = buildTagMap();
        Map<String, List<String>> metricMap = buildEntityMap(storageGroup, tagMap.values());
        while (startTime.isBefore(endTime)) {
            TsFileTask task = new TsFileTask(exportPath, storageGroup, tagMap, metricMap, startTime, tsdbConn);
            pool.execute(task);
            startTime = startTime.plusMonths(1);
        }

        LOG.info("生成TsFile文件执行成功");
    }

    /**
     * 加载配置文件
     * @return
     */
    private static Properties loadConfig() {
        File file = new File("config.properties");
        try {
            InputStream inputStream = new FileInputStream(file);
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        }catch (IOException e){
            LOG.error("配置文件加载失败:",e);
            throw new IoTDBSdkRuntimeException(e);
        }
    }

    private static Map<String, MetricTagMeta> buildTagMap(){
        Map<String, MetricTagMeta> map = new HashMap<>();
        File file = new File("metric_meta.json");
        String fileContent = null;
        try {
            fileContent = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.error("数据模型文件加载失败:",e);
            throw new IoTDBSdkRuntimeException(e);
        }
        List<MetricMetaInfo> metaInfoList = JSONUtil.toList(fileContent, MetricMetaInfo.class);
        metaInfoList.stream().forEach(metricMetaInfo -> {
            metricMetaInfo.getContent().stream().forEach(content -> {
                MetricTagMeta metricTagMeta = new MetricTagMeta();
                metricTagMeta.setMetric(content.getMetric());
                metricTagMeta.setDataType(DataType.valueOf(content.getType()));
                List<MetricTagOrder> metricTagOrderList = new ArrayList<>();
                metricMetaInfo.getHeader().stream().forEach(header -> metricTagOrderList.add(new MetricTagOrder(header, metricTagOrderList.size()+1)));
                content.getTags().stream().forEach(tag -> metricTagOrderList.add(new MetricTagOrder(tag, metricTagOrderList.size()+1)));
                metricTagMeta.setTagOrderList(metricTagOrderList);
                map.put(content.getMetric(), metricTagMeta);
            });
        });
        return map;
    }

    private static Map<String, List<String>> buildEntityMap(String storageGroup, Collection<MetricTagMeta> tagSet){
        Map<String, List<String>> map = new HashMap<>();
        tagSet.stream().forEach(metricTagMeta -> {
            String devicePath = getDevicePath(storageGroup, metricTagMeta.getTagOrderList());
            map.computeIfAbsent(devicePath, k -> new ArrayList<>());
            map.get(devicePath).add(metricTagMeta.getMetric());
        });
        return map;
    }

    /**
     * 根据数据模型生成设备路径
     * @param storageGroup
     * @param tagOrderList
     * @return
     */
    private static String getDevicePath(String storageGroup, List<MetricTagOrder> tagOrderList){
        StringBuilder builder = new StringBuilder(storageGroup);
        if (CollUtil.isNotEmpty(tagOrderList)) {
            tagOrderList.forEach(tag -> builder.append(".").append(KeywordUtil.tag(tag.getTagName())));
        }
        return builder.toString();
    }

}
