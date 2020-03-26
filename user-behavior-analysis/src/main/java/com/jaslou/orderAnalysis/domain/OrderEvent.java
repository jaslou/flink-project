package com.jaslou.orderAnalysis.domain;

import java.util.Objects;

public class OrderEvent {


    public Long orderId; // 订单ID
    public String behavior; // 订单行为
    public String txId; // 交易ID，支付回调后ID
    public Long timestamp;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String behavior, String txId, Long timestamp) {
        this.orderId = orderId;
        this.behavior = behavior;
        this.txId = txId;
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "OrderEvent{" +
                "orderId=" + orderId +
                ", behavior='" + behavior + '\'' +
                ", txId='" + txId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderEvent that = (OrderEvent) o;
        return Objects.equals(orderId, that.orderId) &&
                Objects.equals(behavior, that.behavior) &&
                Objects.equals(txId, that.txId) &&
                Objects.equals(timestamp, that.timestamp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(orderId, behavior, txId, timestamp);
    }
}
