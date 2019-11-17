package com.jaslou.source;

import com.jaslou.domin.UserBehavior;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Calendar;
import java.util.Random;

/***
 * Flink SourceFunction to generate user behavior
 * Each parallel instance of the source simulates 10 producter which emit one user reading every 100 ms.
 */
public class UserBehaviorSource extends RichParallelSourceFunction<UserBehavior> {

    // flag indicating whether source is still running
    private boolean running = true;

    @Override
    public void run(SourceContext<UserBehavior> sourceContext) throws Exception {

        Random rand = new Random();
        // look up index of this parallel task
        int taskIdx = getRuntimeContext().getIndexOfThisSubtask();

        String[] behaviorType = {"pv", "buy", "cart", "fav"};
        while(running) {
            long userId = rand.nextInt(1000);// 用户ID
            long itemId = rand.nextInt(1000);// 产品ID
            int categoryId = rand.nextInt(4);// 商品类目ID
            long timeStamp = Calendar.getInstance().getTimeInMillis(); // 行为发生的时间戳，单位秒
            String behavior = behaviorType[rand.nextInt(3)]; // 用户行为, 包括("pv", "buy", "cart", "fav")
            UserBehavior user =  new UserBehavior(userId, itemId, categoryId, behavior, timeStamp);
            sourceContext.collect(user);
            System.out.println(user.toString());
            // wait for 100 ms
            Thread.sleep(100);
        }
    }

    @Override
    public void cancel() {
        this.running = false;
    }
}
