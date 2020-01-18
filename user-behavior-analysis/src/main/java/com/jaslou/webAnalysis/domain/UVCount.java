package com.jaslou.webAnalysis.domain;

public class UVCount {
    public Long windowEnd;
    public int usCount;

    public UVCount(Long windowEnd, int usCount) {
        this.windowEnd = windowEnd;
        this.usCount = usCount;
    }

    @Override
    public String toString() {
        return "UVCount{" +
                "windowEnd=" + windowEnd +
                ", usCount=" + usCount +
                '}';
    }
}
