package com.jaslou.loginAnalysis.domain;

public class LoginEvent {
    public Long userId;
    public String ipAddress;
    public String status; // success and fail
    public Long timestamp;

    public LoginEvent() {
    }

    public LoginEvent(Long userId, String ipAddress, String status, Long timestamp) {
        this.userId = userId;
        this.ipAddress = ipAddress;
        this.status = status;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "LoginEvent{" +
                "userId=" + userId +
                ", ipAddress='" + ipAddress + '\'' +
                ", status='" + status + '\'' +
                ", timeStamp=" + timestamp +
                '}';
    }
}
