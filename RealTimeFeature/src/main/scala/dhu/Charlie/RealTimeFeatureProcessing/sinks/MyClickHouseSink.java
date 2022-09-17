package dhu.Charlie.RealTimeFeatureProcessing.sinks;

import dhu.Charlie.RealTimeFeatureProcessing.bean.UsersClickMsg;
import dhu.Charlie.RealTimeFeatureProcessing.utils.ClickHouseUtil;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

public class MyClickHouseSink extends RichSinkFunction<UsersClickMsg> {
    Connection connection = null;

    String sql;

    public MyClickHouseSink(String sql) {
        this.sql = sql;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = ClickHouseUtil.getConn("Hadoop102", 8123, "music_users_logs");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
    }

    @Override
    public void invoke(UsersClickMsg user, Context context) throws Exception {
        PreparedStatement preparedStatement = connection.prepareStatement(sql);
        preparedStatement.setString(1, user.getId());
        preparedStatement.setString(2, user.getSongid());
        preparedStatement.setInt(3, user.getMid());
        preparedStatement.setInt(4, user.getOptrate_type());
        preparedStatement.setLong(5, user.getUid());
        preparedStatement.setInt(6, user.getConsume_type());
        preparedStatement.setLong(7, user.getPlay_time());
        preparedStatement.setLong(8, user.getDur_time());
        preparedStatement.setLong(9, user.getSession_id());
        preparedStatement.setString(10, user.getSongname());
        preparedStatement.setInt(11, user.getPkg_id());
        preparedStatement.setString(12, user.getOrder_id());
        preparedStatement.setString(13, user.getDatetimes());
        preparedStatement.addBatch();

        long startTime = System.currentTimeMillis();
        int[] ints = preparedStatement.executeBatch();
        connection.commit();
        long endTime = System.currentTimeMillis();
        System.out.println("批量插入完毕用时：" + (endTime - startTime) + " -- 插入数据 = " + ints.length);
    }
}
