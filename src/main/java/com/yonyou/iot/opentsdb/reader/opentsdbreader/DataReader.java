package com.yonyou.iot.opentsdb.reader.opentsdbreader;

import cn.hutool.core.collection.CollUtil;
import com.google.common.base.Stopwatch;
import com.google.common.collect.Lists;
import com.yonyou.iot.opentsdb.reader.conn.OpenTSDBConnection;
import com.yonyou.iot.opentsdb.reader.conn.OpenTSDBFactory;
import com.yonyou.iot.opentsdb.reader.util.TimeUtils;
import com.yonyou.iotdb.exception.IoTDBSdkRuntimeException;
import com.yonyou.iotdb.metric.MetricHandler;
import com.yonyou.iotdb.metric.domain.MetricData;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class DataReader implements Runnable{
    private static final Logger LOG = LoggerFactory.getLogger(DataReader.class);
    private List<String> metricList;
    private DateTime startDateTime;
    private DateTime stopDateTime;
    private OpenTSDBConnection conn;
    private MetricHandler metricHandler;
    private String storageGroup;

    public DataReader(String storageGroup,String metrics,long startTime,long endTime,OpenTSDBConnection conn){
        this.conn = conn;
        this.storageGroup = storageGroup;
        this.metricList = Lists.newArrayList(metrics.split(","));
        this.startDateTime = new DateTime(TimeUtils.getTimeInHour(startTime));
        this.stopDateTime = new DateTime(TimeUtils.getTimeInHour(endTime));
        this.metricHandler = MetricHandler.getInstance();
    }

    @Override
    public void run() {
        Stopwatch watch = Stopwatch.createUnstarted();
        LOG.info("任务执行开始:metric->{}",this.metricList.get(0));
        while (startDateTime.isBefore(stopDateTime)) {
            DateTime endDateTime = startDateTime.plusHours(1);
            try {
                LOG.info("metric:{},start->{},end->{}",this.metricList.get(0),startDateTime,endDateTime);
                watch.start();
                List<MetricData> dataList = metricList.stream().parallel().flatMap(metric -> {
                    try {
                        return OpenTSDBFactory.dump(conn, metric, startDateTime.getMillis(), endDateTime.getMillis() - 1).stream();
                    } catch (Exception e) {
                        LOG.error("scan数据失败,metric:{}",metric,e);
                        throw new IoTDBSdkRuntimeException(e);
                    }
                }).collect(Collectors.toList());
                watch.stop();
                watch.start();
                if(CollUtil.isNotEmpty(dataList)){
                    metricHandler.save(storageGroup,dataList, ArrayList::new);
                }
                watch.stop();
                LOG.info("metric:{},start->{},end->{},数据量->{},耗时->{}",this.metricList.get(0),startDateTime,endDateTime,dataList.size(), watch);
                startDateTime = endDateTime;
                watch.reset();
            } catch (Exception e) {
                LOG.error("任务执行失败:",e);
                throw new IoTDBSdkRuntimeException(e);
            }
        }

    }

    private void read(){
        Stopwatch watch = Stopwatch.createUnstarted();
        LOG.info("任务执行开始:metric->{}",this.metricList.get(0));
        while (startDateTime.isBefore(stopDateTime)) {
            DateTime endDateTime = startDateTime.plusHours(1);
            try {
                LOG.info("metric:{},start->{},end->{}",this.metricList.get(0),startDateTime,endDateTime);
                Map<String,List<MetricData>> dataMap = new HashMap<>();
                watch.start();
                AtomicInteger count = new AtomicInteger(0);
                metricList.stream().parallel().flatMap(metric -> {
                    try {
                        return OpenTSDBFactory.dump(conn, metric, startDateTime.getMillis(), endDateTime.getMillis() - 1).stream().collect(Collectors.groupingBy(o-> o.getTags().get("tid"), Collectors.toList())).entrySet().stream();
                    } catch (Exception e) {
                        LOG.error("scan数据失败,metric:{}",metric,e);
                        throw new IoTDBSdkRuntimeException(e);
                    }
                }).forEach(e->{
                    if(CollUtil.isNotEmpty(e.getValue())){
                        List<MetricData> dataList = dataMap.get(e.getKey());
                        if(dataList == null){
                            dataList = new ArrayList<>();
                        }
                        count.addAndGet(e.getValue().size());
                        if(CollUtil.isNotEmpty(e.getValue())){
                            dataList.addAll(e.getValue());
                        }
                        dataMap.put(e.getKey(),dataList);
                    }
                });
                watch.stop();
                watch.start();
                dataMap.values().stream().parallel().forEach(list->{
                    try{
                        metricHandler.save("root.yyy",list, ArrayList::new);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                });
                watch.stop();
                LOG.info("metric:{},start->{},end->{},数据量->{},耗时->{}",this.metricList.get(0),startDateTime,endDateTime,count.get(), watch);
                startDateTime = endDateTime;
                watch.reset();
            } catch (Exception e) {
                LOG.error("任务执行失败:",e);
                throw new IoTDBSdkRuntimeException(e);
            }
        }
    }

}
