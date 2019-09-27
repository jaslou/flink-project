package com.jark;
/** 商品点击量(窗口操作的输出类型) */
public class ItemViewCount {
    private long itemId;     // 商品ID
    private long windowEnd;  // 窗口结束时间戳
    private long viewCount;  // 商品的点击量

    public static ItemViewCount of(long itemId, long windowEnd, long viewCount) {
        ItemViewCount result = new ItemViewCount();
        result.itemId = itemId;
        result.windowEnd = windowEnd;
        result.viewCount = viewCount;
        return result;
    }

    public long getItemId() {
        return itemId;
    }

    public long getWindowEnd() {
        return windowEnd;
    }

    public long getViewCount() {
        return viewCount;
    }

    public void setItemId(long itemId) {
        this.itemId = itemId;
    }

    public void setWindowEnd(long windowEnd) {
        this.windowEnd = windowEnd;
    }

    public void setViewCount(long viewCount) {
        this.viewCount = viewCount;
    }
}
