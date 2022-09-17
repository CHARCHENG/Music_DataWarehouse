package dhu.Charlie.test.bean;

import java.util.List;

public class Lines {
    private String text;
    private List<SingleSpo> spo_list;

    public Lines(String text, List<SingleSpo> spo_list) {
        this.text = text;
        this.spo_list = spo_list;
    }

    public String getText() {
        return text;
    }

    public void setText(String text) {
        this.text = text;
    }

    public List<SingleSpo> getSpo_list() {
        return spo_list;
    }

    public void setSpo_list(List<SingleSpo> spo_list) {
        this.spo_list = spo_list;
    }

    @Override
    public String toString() {
        return "{" + "\"text\":\"" + text + "\", \"spo_list\":" + spo_list + "}";
    }
}
