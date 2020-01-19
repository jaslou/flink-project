package com.jaslou.assigner;

import com.jaslou.domin.UserBehavior;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Assigns timestamps to SensorReadings based on their internal timestamp and
 * emits watermarks with five seconds slack.
 */
public class UserBehaviorTimeAssigner extends BoundedOutOfOrdernessTimestampExtractor<UserBehavior> {


    public UserBehaviorTimeAssigner(Time maxOutOfOrderness) {
        super(maxOutOfOrderness);
    }

    @Override
    public long extractTimestamp(UserBehavior userBehavior) {
        return userBehavior.timestamp;
    }



}
