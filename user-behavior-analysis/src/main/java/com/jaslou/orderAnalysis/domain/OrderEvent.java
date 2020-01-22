package com.jaslou.orderAnalysis.domain;

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
}
