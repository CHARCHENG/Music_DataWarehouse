package dhu.Charlie.RealTimeFeatureProcessing.sinks;

import dhu.Charlie.RealTimeFeatureProcessing.bean.MusicClickCountRes;
import dhu.Charlie.RealTimeFeatureProcessing.utils.TimeUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;

public class Flink2MySqlSink extends RichSinkFunction<List<MusicClickCountRes>> {
    private Connection conn;
    private PreparedStatement insertStmt;
    private PreparedStatement updateStmt;

    // 配置选项
    private String url;
    private String host;
    private String port;
    private String databases;
    private String table;
    private String users;
    private String password;

    public Flink2MySqlSink(String host, String port, String databases, String table, String users, String password) {
        this.host = host;
        this.port = port;
        this.databases = databases;
        this.table = table;
        this.users = users;
        this.password = password;
        this.url = "jdbc:mysql://" + this.host + ":" + this.port + "/" + this.databases + "?useSSL=false&allowPublicKeyRetrieval=true";
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        conn = DriverManager.getConnection(this.url, this.users, this.password);
        insertStmt = conn.prepareStatement("insert into " + this.table + " values(?, ?, ?, ?)");
        updateStmt = conn.prepareStatement("update " + this.table + " set songname=?, clickTimes=?, times=? where rankId=?");
    }

    @Override
    public void invoke(List<MusicClickCountRes> value, Context context) throws Exception {
        int sizes = value.size();

        Thread.sleep(30 * 1000);

        for(int i = 0; i < sizes; i++){
            MusicClickCountRes nowEntity = value.get(i);
            updateStmt.setString(1, nowEntity.getSongname());
            updateStmt.setInt(2, nowEntity.getClickTime());
            updateStmt.setString(3, TimeUtils.formatTimestamps(nowEntity.getEndTime(), "yyyy-MM-dd HH:mm:ss"));
            updateStmt.setInt(4, i + 1);

            updateStmt.execute();

            if(updateStmt.getUpdateCount() == 0){
                insertStmt.setInt(1, i + 1);
                insertStmt.setString(2, nowEntity.getSongname());
                insertStmt.setInt(3, nowEntity.getClickTime());
                insertStmt.setString(4, TimeUtils.formatTimestamps(nowEntity.getEndTime(), "yyyy-MM-dd HH:mm:ss"));
                insertStmt.execute();
            }
        }

    }

    @Override
    public void close() throws Exception {
        insertStmt.close();
        updateStmt.close();
        conn.close();
    }
}
