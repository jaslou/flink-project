package com.jaslou.orderAnalysis.domain;

public class OrderTimeResult {
    public Long orderId;
    public String result;

    public OrderTimeResult(Long orderId, String result) {
        this.orderId = orderId;
        this.result = result;
    }

    public OrderTimeResult() { }

    @Override
    public String toString() {
        return "OrderTimeResult{" +
                "orderId=" + orderId +
                ", result='" + result + '\'' +
                '}';
    }
}
