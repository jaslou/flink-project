package com.jaslou.adclickAnalysis.domain;

public class AdClickEvent {
    public long userId;//用户ID
    public long adId;//广告Id
    public String province;// 用户所属省份
    public String city; // 用户所属城市
    public long timestamp; // 时间戳

    // 作为POJO类型，必须要有一个共有的无参构造函数，否则会报ClassCastException错误
    public AdClickEvent(){}

    public AdClickEvent(long userId, long adId, String province, String city, long timestamp) {
        this.userId = userId;
        this.adId = adId;
        this.province = province;
        this.city = city;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AdClickEvent{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", province='" + province + '\'' +
                ", city='" + city + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
