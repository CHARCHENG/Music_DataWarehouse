package dhu.Charlie.RealTimeFeatureProcessing.etl.bean;

import lombok.Data;

@Data
public class UsersClickMsg {
    private String id;
    private String songid;
    private Integer mid;
    private Integer optrate_type;
    private Long uid;
    private Integer consume_type;
    private Integer play_time;
    private Integer dur_time;
    private Integer session_id;
    private String songname;
    private Integer pkg_id;
    private String order_id;
    private String datetimes;

    public UsersClickMsg(){

    }

    public UsersClickMsg(String id, String songid, Integer mid, Integer optrate_type, Long uid, Integer consume_type, Integer play_time, Integer dur_time, Integer session_id, String songname, Integer pkg_id, String order_id, String datetimes) {
        this.id = id;
        this.songid = songid;
        this.mid = mid;
        this.optrate_type = optrate_type;
        this.uid = uid;
        this.consume_type = consume_type;
        this.play_time = play_time;
        this.dur_time = dur_time;
        this.session_id = session_id;
        this.songname = songname;
        this.pkg_id = pkg_id;
        this.order_id = order_id;
        this.datetimes = datetimes;
    }
}
