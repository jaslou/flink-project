package com.jaslou.marketAnalysis.domain;

public class AppMarketUserBehavior {
    public String userId;
    public String behavior;
    public String channel;
    public Long timestamp;

    public AppMarketUserBehavior(String userId, String channel, String behavior, Long timestamp) {
        this.userId = userId;
        this.behavior = behavior;
        this.channel = channel;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "AppMarketUserBehavior{" +
                "userId='" + userId + '\'' +
                ", behavior='" + behavior + '\'' +
                ", channel='" + channel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
