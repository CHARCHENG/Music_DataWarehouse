package dhu.Charlie.RealTimeFeatureProcessing.operator;

import dhu.Charlie.RealTimeFeatureProcessing.bean.MusicClickCountRes;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class MusicKeyedProcessFunc extends KeyedProcessFunction<Long, MusicClickCountRes, List<MusicClickCountRes>> {

    private Integer n;
    private ListState<MusicClickCountRes> musicClickCountState;

    public MusicKeyedProcessFunc(Integer n){
        this.n = n;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        musicClickCountState = getRuntimeContext().getListState(
           new ListStateDescriptor<>(
           "MusicClickCountsState",
           MusicClickCountRes.class
       )
      );
    }

    @Override
    public void processElement(MusicClickCountRes entity, Context context, Collector<List<MusicClickCountRes>> collector) throws Exception {
        musicClickCountState.add(entity);
        context.timerService().registerEventTimeTimer(context.getCurrentKey() + 1);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<List<MusicClickCountRes>> out) throws Exception {
        List<MusicClickCountRes> tList = new ArrayList();
        for(MusicClickCountRes mc: musicClickCountState.get()){
            tList.add(mc);
        }

        musicClickCountState.clear();

        // 排序
        Collections.sort(tList, (v1, v2) -> v2.getClickTime() - v1.getClickTime());

        List<MusicClickCountRes> topNLists = new ArrayList();

        for(int i = 0; i < this.n; i++){
            topNLists.add(tList.get(i));
        }

        out.collect(topNLists);

    }
}
