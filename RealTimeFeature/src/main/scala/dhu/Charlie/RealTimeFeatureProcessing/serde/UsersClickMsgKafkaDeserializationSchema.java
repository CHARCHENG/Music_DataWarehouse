package dhu.Charlie.RealTimeFeatureProcessing.serde;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class UsersClickMsgKafkaDeserializationSchema implements KafkaDeserializationSchema<UsersClickMsg> {

    @Override
    public boolean isEndOfStream(UsersClickMsg usersClickMsg) {
        return false;
    }

    @Override
    public UsersClickMsg deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        String value = null;

        if(consumerRecord.value() == null){
            return null;
        }

        value = new String(consumerRecord.value());

        JSONObject entity = JSON.parseObject(value);

        UsersClickMsg usersClickMsg = new UsersClickMsg(
                entity.getString("id"),
                entity.getString("songid"),
                entity.getInteger("mid"),
                entity.getInteger("optrate_type"),
                entity.getLong("uid"),
                entity.getInteger("consume_type"),
                entity.getInteger("play_time"),
                entity.getInteger("dur_time"),
                entity.getInteger("session_id"),
                entity.getString("songname"),
                entity.getInteger("pkg_id"),
                entity.getString("order_id"),
                entity.getString("datetimes")
        );
        return usersClickMsg;
    }

    @Override
    public TypeInformation<UsersClickMsg> getProducedType() {
        return TypeInformation.of(new TypeHint<UsersClickMsg>() {});
    }
}
