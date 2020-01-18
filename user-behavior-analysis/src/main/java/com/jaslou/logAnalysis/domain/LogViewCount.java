package com.jaslou.logAnalysis.domain;

public class LogViewCount {
    public String accessURL;
    public long windowEnd;
    public long viewCount;

    public LogViewCount(String accessURL, long windowEnd, long viewCount) {
        this.accessURL = accessURL;
        this.windowEnd = windowEnd;
        this.viewCount = viewCount;
    }

    public String getAccessURL() {
        return accessURL;
    }

    public void setAccessURL(String accessURL) {
        this.accessURL = accessURL;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }
}
