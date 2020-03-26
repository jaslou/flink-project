package com.jaslou.orderAnalysis.domain;

import java.util.Objects;

public class ReceiptEvent {
    public String txId; // 交易ID，支付回调后ID
    public String payChannel; // 支付通道
    public Long timestamp; // 时间戳

    public ReceiptEvent(String txId, String payChannel, Long timestamp) {
        this.txId = txId;
        this.payChannel = payChannel;
        this.timestamp = timestamp;
    }

    public ReceiptEvent() {
    }

    @Override
    public String toString() {
        return "ReceiptEvent{" +
                "txId='" + txId + '\'' +
                ", payChannel='" + payChannel + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ReceiptEvent that = (ReceiptEvent) o;
        return Objects.equals(txId, that.txId) &&
                Objects.equals(payChannel, that.payChannel) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(txId, payChannel, timestamp);
    }
}
