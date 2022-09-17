package dhu.Charlie.RealTimeFeatureProcessing.serde;

import com.alibaba.fastjson.JSON;
import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;
import javax.annotation.Nullable;

public class UsersClickMsgKafkaSerializationSchema implements KafkaSerializationSchema<UsersClickMsg>{

    private String key;

    public UsersClickMsgKafkaSerializationSchema( ){ }

    public UsersClickMsgKafkaSerializationSchema(String topic){
        this.key = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(UsersClickMsg usersClickMsg, @Nullable Long timestamp) {
        byte[] c = null;

        try {
            String res = JSON.toJSONString(usersClickMsg);
            c = res.getBytes("UTF-8");
        }
        catch (Exception e) {
            e.printStackTrace();
        }
        // 发送给KafkaBroker的key/value 值对
        return new ProducerRecord< >(key, c);
    }
}
