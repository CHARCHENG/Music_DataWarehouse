package dhu.Charlie.RealTimeFeatureProcessing.utils;

import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.metrics.Counter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class MyMetricsAccumlate {

    private static final Logger logger = LoggerFactory.getLogger(MyMetricsAccumlate.class);

    private RuntimeContext runtimeContext;

    // 名称
    private String IN_RECORDS_COUNT = "IN_RECORDS_COUNT";
    private String OUT_RECORDS_COUNT = "OUT_RECORDS_COUNT";

    // 指标
    private transient HashMap<String, Counter> metricsMaps;

    // 累加器初始化
    private transient HashMap<String, LongCounter> accumlatorsMaps;

    public MyMetricsAccumlate(RuntimeContext runtimeContext){
        this.runtimeContext = runtimeContext;

        this.metricsMaps = new HashMap<>();
        this.accumlatorsMaps = new HashMap<>();
        // metrics初始化
        Counter inRecords = this.runtimeContext.getMetricGroup().counter(IN_RECORDS_COUNT);
        Counter outRecords = this.runtimeContext.getMetricGroup().counter(OUT_RECORDS_COUNT);
        metricsMaps.put(IN_RECORDS_COUNT, inRecords);
        metricsMaps.put(OUT_RECORDS_COUNT, outRecords);

        //注册累加器
        LongCounter inRecordsAccumlator = new LongCounter();
        LongCounter outRecordsAccumlator = new LongCounter();
        accumlatorsMaps.put(IN_RECORDS_COUNT, inRecordsAccumlator);
        accumlatorsMaps.put(OUT_RECORDS_COUNT, outRecordsAccumlator);

        this.runtimeContext.addAccumulator(IN_RECORDS_COUNT, accumlatorsMaps.get(IN_RECORDS_COUNT));
        this.runtimeContext.addAccumulator(OUT_RECORDS_COUNT, accumlatorsMaps.get(OUT_RECORDS_COUNT));
    }


    public void addOneToRecordsIn(){
        metricsMaps.get(IN_RECORDS_COUNT).inc(1L);
        accumlatorsMaps.get(IN_RECORDS_COUNT).add(1L);
    }

    public void addOneToRecordsOut(){
        metricsMaps.get(OUT_RECORDS_COUNT).inc(1L);
        accumlatorsMaps.get(OUT_RECORDS_COUNT).add(1L);
    }

    public void createCustomAccumlatorAndMetric(String name){
        if(accumlatorsMaps.containsKey(name)){
            return;
        }
        LongCounter customAccumlator = new LongCounter();
        Counter counter = this.runtimeContext.getMetricGroup().counter(name);

        metricsMaps.put(name, counter);
        accumlatorsMaps.put(name, customAccumlator);

        this.runtimeContext.addAccumulator(name, customAccumlator);
    }

    public void addOneToCustomAccumlatorAndMetric(String name){
        metricsMaps.get(name).inc(1L);
        accumlatorsMaps.get(name).add(1L);
    }

}
