package com.jaslou.webAnalysis;

import scala.Serializable;

/**
 * 布隆过滤器
 */
public class BloomFilter implements Serializable {
    // 默认值,位图大小16M=(1*2^4)M=(1*2^4)^10KB=((1*2^4)^10KB)^10K=(((1*2^4)^10KB)^10K)*8位
    private long cap = 1 << 27;
    private int seed; //
    public BloomFilter(Long cap, int seed) {
        this.cap = cap;
        this.seed = seed;
    }

    /**
     * 计算hash值
     * @param  value
     * @return return hash of string value
     */
    public Long hash(String value) {
        long result = 0L;
        for (int i = 0; i < value.length(); i++) {
            result = result * seed + value.charAt(i);
        }
        return result & (cap - 1);
    }
}
