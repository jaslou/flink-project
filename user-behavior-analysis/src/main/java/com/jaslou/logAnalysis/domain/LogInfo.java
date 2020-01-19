package com.jaslou.logAnalysis.domain;

public class LogInfo {
    public String ipaddress; // 访问网站的用户IP地址
    public Long timestamp; //时间
    public String accessURL; // 访问的网站URL
    public String method;   // 访问方式：GET、POST

    public LogInfo(String ipaddress, Long timestamp, String accessURL, String method) {
        this.ipaddress = ipaddress;
        this.timestamp = timestamp;
        this.accessURL = accessURL;
        this.method = method;
    }

    @Override
    public String toString() {
        return "LogInfo{" +
                "ipaddress='" + ipaddress + '\'' +
                ", timestamp=" + timestamp +
                ", accessURL='" + accessURL + '\'' +
                ", method='" + method + '\'' +
                '}';
    }
}
