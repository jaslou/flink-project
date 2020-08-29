package com.jaslou.cep;

import java.util.Objects;

public class SensorWarning {
    public String id;
    public double averageTemperature;

    public SensorWarning(String id, double averageTemperature) {
        this.id = id;
        this.averageTemperature = averageTemperature;
    }

    public SensorWarning() {
        this("",-1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SensorWarning that = (SensorWarning) o;
        return Double.compare(that.averageTemperature, averageTemperature) == 0 &&
                Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, averageTemperature);
    }
}
