package com.yonyou.iot.opentsdb.reader.opentsdbreader;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.text.CharSequenceUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.esotericsoftware.minlog.Log;
import com.google.common.collect.Lists;
import com.yonyou.iot.opentsdb.reader.conn.OpenTSDBConnection;
import com.yonyou.iotdb.core.IotConfiguration;
import com.yonyou.iotdb.core.IotHandler;
import com.yonyou.iotdb.domain.DataType;
import com.yonyou.iotdb.exception.IoTDBSdkException;
import com.yonyou.iotdb.metric.domain.MetricTagMeta;
import com.yonyou.iotdb.metric.tag.MetricTagManager;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {
    private static final Logger LOG = LoggerFactory.getLogger(DataReader.class);

    public static void main(String[] args)throws Exception{
        Properties properties = loadConfig();
        String iotAddress = properties.getProperty("task.iotserver");
        String iotUser = properties.getProperty("task.iotuser");
        String iotPw = properties.getProperty("task.iotpw");
        String storageGroup = properties.getProperty("task.storageGroup");
        int poolsize = Integer.parseInt(properties.getProperty("task.iotPoolSize"));
        IotConfiguration configuration = new IotConfiguration(iotAddress, iotUser, iotPw, poolsize);
        IotHandler.getInstance().init(configuration);
        LOG.info("IOT 连接池初始化完成");
        String metainit = properties.getProperty("task.metainit");
        if(CharSequenceUtil.isNotEmpty(metainit)){
            Log.info("初始化元数据");
            initMetricMeta(storageGroup);
        }
        String address = properties.getProperty("task.otsserver");
        OpenTSDBConnection tsdbConn = new OpenTSDBConnection(address);
        LOG.info("otsdb 连接池初始化完成");
        long startTime = Long.parseLong(properties.getProperty("task.startTime"));
        long endTime = Long.parseLong(properties.getProperty("task.endTime"));
        String[] metrics = properties.getProperty("metrics").split(";");
        List<String> columns = Lists.newArrayList(metrics);

        ThreadPoolExecutor pool = new ThreadPoolExecutor(poolsize, poolsize, 1, TimeUnit.HOURS,
                new ArrayBlockingQueue<>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
        columns.forEach(metric-> {
            LOG.info("提交执行任务:{}",metrics);
            pool.submit(new DataReader(storageGroup,metric, startTime, endTime, tsdbConn));
        });

    }

    private static Properties loadConfig() throws IOException {
        try (InputStream inputStream = Files.newInputStream(Paths.get("config.properties"))) {
            Properties properties = new Properties();
            properties.load(inputStream);
            return properties;
        }
    }

    private static void initMetricMeta(String storageGroup)throws Exception {
        MetricTagManager tagManager = MetricTagManager.getInstance();
        File file = Paths.get("metric_meta.json").toFile();
        String content = FileUtils.readFileToString(file, StandardCharsets.UTF_8);
        List<MetricMetaInfo> metaInfoList = JSONUtil.toList(content, MetricMetaInfo.class);
        metaInfoList
                .forEach(
                        metaInfo -> {
                            List<String> header = metaInfo.getHeader();
                            List<MetricMetaInfo.Content> contentList = metaInfo.getContent();
                            contentList
                                    .forEach(
                                            c -> {
                                                MetricTagMeta meta = new MetricTagMeta();
                                                meta.setMetric(c.getMetric());
                                                meta.setStorageGroup(storageGroup);
                                                meta.setDataType(DataType.valueOf(c.getType().toUpperCase()));
                                                List<String> tagList = new ArrayList<>(header);
                                                if (CollUtil.isNotEmpty(c.getTags())) {
                                                    tagList.addAll(c.getTags());
                                                }
                                                meta.setTagList(tagList);
                                                try {
                                                    tagManager.saveMetricMeta(meta);
                                                    tagManager.addMetricTag(meta,tagList);
                                                } catch (IoTDBSdkException e) {
                                                    throw new RuntimeException(e);
                                                }
                                            });
                        });
    }

}
