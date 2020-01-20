package com.jaslou.loginAnalysis.domain;

public class LoginWarningMessage {
    public Long userId;
    public Long firstFailTime;
    public Long lastFailTime;
    public String message;

    public LoginWarningMessage(Long userId, Long firstFailTime, Long lastFailtime, String message) {
        this.userId = userId;
        this.firstFailTime = firstFailTime;
        this.lastFailTime = lastFailtime;
        this.message = message;
    }

    @Override
    public String toString() {
        return "LoginWarningMessage{" +
                "userId=" + userId +
                ", firstFailTime=" + firstFailTime +
                ", lastFailtime=" + lastFailTime +
                ", message='" + message + '\'' +
                '}';
    }
}
