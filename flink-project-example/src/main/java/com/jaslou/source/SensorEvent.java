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
    public String type;

    /**
     * Empty default constructor to satify Flink's POJO requirements.
     */
    public SensorEvent() { }

    public SensorEvent(String id, long timestamp, double temperature, String type) {
        this.id = id;
        this.timestamp = timestamp;
        this.temperature = temperature;
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SensorEvent that = (SensorEvent) o;
        return timestamp == that.timestamp &&
                Double.compare(that.temperature, temperature) == 0 &&
                Objects.equals(id, that.id) &&
                Objects.equals(type, that.type);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, timestamp, temperature, type);
    }

    @Override
    public String toString() {
        return "SensorEvent{" +
                "id='" + id + '\'' +
                ", timestamp=" + timestamp +
                ", temperature=" + temperature +
                ", type='" + type + '\'' +
                '}';
    }
}