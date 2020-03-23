package com.jaslou.source;

import java.util.Objects;

/**
 * POJO to hold sensor reading data.
 */
public class SensorEvent {

    // id of the sensor
    public String id;
    // timestamp of the reading
    public long timestamp;
    // temperature value of the reading
    public double temperature;

    /**
     * Empty default constructor to satify Flink's POJO requirements.
     */
    public SensorEvent() { }

    public SensorEvent(String id, long timestamp, double temperature) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
    }

    public String toString() {
        return "(" + this.id + ", " + this.timestamp + ", " + this.temperature + ")";
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof SensorEvent) {
            SensorEvent that = (SensorEvent) o;
            return timestamp == that.timestamp &&
                    Double.compare(that.temperature, temperature) == 0 &&
                    Objects.equals(id, that.id);
        }else{
            return false;
        }

    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, temperature);
    }
}