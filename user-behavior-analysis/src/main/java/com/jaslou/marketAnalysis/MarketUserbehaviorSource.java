package com.jaslou.marketAnalysis;

import com.jaslou.marketAnalysis.domain.AppMarketUserBehavior;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.Random;
import java.util.UUID;

public class MarketUserbehaviorSource extends RichSourceFunction<AppMarketUserBehavior> {

    Random random = new Random();
    String[] channels = {"weibo", "weixin", "appstore", "baidu"};
    String[] behaviors = {"click", "download", "install", "uninstall"};
    boolean running = true;

    @Override
    public void run(SourceContext<AppMarketUserBehavior> ctx) throws Exception {
        while (running) {
            String userId = UUID.randomUUID().toString();
            String channel = channels[random.nextInt(channels.length)];
            String behavior = behaviors[random.nextInt(behaviors.length)];
            Long timestamp = System.currentTimeMillis();
            ctx.collect(new AppMarketUserBehavior(userId, channel, behavior, timestamp));
//            System.out.println(new AppMarketUserBehavior(userId, channel, behavior, timestamp).toString());
            Thread.sleep(10L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
