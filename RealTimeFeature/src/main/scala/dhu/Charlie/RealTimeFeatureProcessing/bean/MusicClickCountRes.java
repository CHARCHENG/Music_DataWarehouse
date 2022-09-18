package dhu.Charlie.RealTimeFeatureProcessing.bean;

import lombok.AllArgsConstructor;
import lombok.Data;

public class MusicClickCountRes {
    private String songname;
    private Long startTime;
    private Long endTime;
    private Integer clickTime;

    public MusicClickCountRes(){ }

    public MusicClickCountRes(String songname, Long startTime, Long endTime, Integer clickTime) {
        this.songname = songname;
        this.startTime = startTime;
        this.endTime = endTime;
        this.clickTime = clickTime;
    }

    public String getSongname() {
        return songname;
    }

    public void setSongname(String songname) {
        this.songname = songname;
    }

    public Long getStartTime() {
        return startTime;
    }

    public void setStartTime(Long startTime) {
        this.startTime = startTime;
    }

    public Long getEndTime() {
        return endTime;
    }

    public void setEndTime(Long endTime) {
        this.endTime = endTime;
    }

    public Integer getClickTime() {
        return clickTime;
    }

    public void setClickTime(Integer clickTime) {
        this.clickTime = clickTime;
    }
}
