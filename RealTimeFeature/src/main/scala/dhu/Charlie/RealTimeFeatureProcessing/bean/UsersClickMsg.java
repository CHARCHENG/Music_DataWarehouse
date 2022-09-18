package dhu.Charlie.RealTimeFeatureProcessing.bean;


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

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSongid() {
        return songid;
    }

    public void setSongid(String songid) {
        this.songid = songid;
    }

    public Integer getMid() {
        return mid;
    }

    public void setMid(Integer mid) {
        this.mid = mid;
    }

    public Integer getOptrate_type() {
        return optrate_type;
    }

    public void setOptrate_type(Integer optrate_type) {
        this.optrate_type = optrate_type;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public Integer getConsume_type() {
        return consume_type;
    }

    public void setConsume_type(Integer consume_type) {
        this.consume_type = consume_type;
    }

    public Integer getPlay_time() {
        return play_time;
    }

    public void setPlay_time(Integer play_time) {
        this.play_time = play_time;
    }

    public Integer getDur_time() {
        return dur_time;
    }

    public void setDur_time(Integer dur_time) {
        this.dur_time = dur_time;
    }

    public Integer getSession_id() {
        return session_id;
    }

    public void setSession_id(Integer session_id) {
        this.session_id = session_id;
    }

    public String getSongname() {
        return songname;
    }

    public void setSongname(String songname) {
        this.songname = songname;
    }

    public Integer getPkg_id() {
        return pkg_id;
    }

    public void setPkg_id(Integer pkg_id) {
        this.pkg_id = pkg_id;
    }

    public String getOrder_id() {
        return order_id;
    }

    public void setOrder_id(String order_id) {
        this.order_id = order_id;
    }

    public String getDatetimes() {
        return datetimes;
    }

    public void setDatetimes(String datetimes) {
        this.datetimes = datetimes;
    }
}
