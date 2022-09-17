package dhu.Charlie.RealTimeFeatureProcessing.operator;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dhu.Charlie.RealTimeFeatureProcessing.utils.MyMetricsAccumlate;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

public class SongsMsgFlatMap extends RichFlatMapFunction<String, String> {

    MyMetricsAccumlate metric = null;

    private final String NULL_JSON_MSG = "NULL_JSON_MSG";

    @Override
    public void open(Configuration parameters) throws Exception {
        metric = new MyMetricsAccumlate(getRuntimeContext());
    }

    @Override
    public void flatMap(String s, Collector<String> collector) throws Exception {
        metric.addOneToRecordsIn();

        metric.createCustomAccumlatorAndMetric(NULL_JSON_MSG);

        JSONObject jsonObject = JSON.parseObject(s);

        if(jsonObject == null){
            metric.addOneToCustomAccumlatorAndMetric(NULL_JSON_MSG);
            return;
        }

        String after = jsonObject.getString("after");

        if(after == null){
            metric.addOneToCustomAccumlatorAndMetric(NULL_JSON_MSG);
            return;
        }
        metric.addOneToRecordsOut();
        collector.collect(after);
    }

}
