package com.jaslou.cep;

import java.util.Objects;

public class SensorAlert {
    public String id;

    public SensorAlert(String id) {
        this.id = id;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SensorAlert that = (SensorAlert) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }

    @Override
    public String toString() {
        return "SensorAlert{" +
                "id='" + id + '\'' +
                '}';
    }
}
