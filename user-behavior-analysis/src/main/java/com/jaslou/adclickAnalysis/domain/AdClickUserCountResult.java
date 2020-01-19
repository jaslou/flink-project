package com.jaslou.adclickAnalysis.domain;

public class AdClickUserCountResult {
    public Long windowEnd;
    public Long userId;
    public Long adId;
    public Long count;

    public AdClickUserCountResult(Long windowEnd, Long userId, Long adId, Long count) {
        this.windowEnd = windowEnd;
        this.userId = userId;
        this.adId = adId;
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdClickUserCountResult{" +
                "windowEnd=" + windowEnd +
                ", userId=" + userId +
                ", adId=" + adId +
                ", count=" + count +
                '}';
    }
}
