package dhu.Charlie.RealTimeFeatureProcessing.operator;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.SimpleVersionedStringSerializer;

public class CustomizedBucketAssigner<IN> implements BucketAssigner<IN, String> {

    private static final String DEFAULT_FORMAT_STRING = "yy-MM-dd--HH";
    private static final String BUCKET_ID_PREFIX = "date=";

    private DateTimeBucketAssigner<IN> dateTimeBucketAssigner;

    public CustomizedBucketAssigner() {
        this(DEFAULT_FORMAT_STRING);
    }

    public CustomizedBucketAssigner(String formatString) {
        this.dateTimeBucketAssigner = new DateTimeBucketAssigner<>(formatString);
    }

    @Override
    public String getBucketId(IN element, Context context) {
        String bucketId = this.dateTimeBucketAssigner.getBucketId(element, context);
        return BUCKET_ID_PREFIX + bucketId;
    }

    @Override
    public SimpleVersionedSerializer<String> getSerializer() {
        return SimpleVersionedStringSerializer.INSTANCE;
    }
}
