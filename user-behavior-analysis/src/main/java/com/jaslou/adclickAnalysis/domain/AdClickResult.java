package com.jaslou.adclickAnalysis.domain;

public class AdClickResult {
    public String windowEnd;
    public String province;
    public Long adId;
    public Long clickCount;

    public AdClickResult(String windowEnd, String province, Long adId, Long clickCount) {
        this.windowEnd = windowEnd;
        this.province = province;
        this.adId = adId;
        this.clickCount = clickCount;
    }

    @Override
    public String toString() {
        return "AdClickResult{" +
                "windowEnd='" + windowEnd + '\'' +
                ", province='" + province + '\'' +
                ", adId=" + adId +
                ", clickCount=" + clickCount +
                '}';
    }
}
