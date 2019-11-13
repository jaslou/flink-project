package com.jaslou.domin;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class UserDeserializationSchema implements DeserializationSchema<UserBehavior>, SerializationSchema<UserBehavior> {

    @Override
    public UserBehavior deserialize(byte[] message) throws IOException {
        return null;
    }

    @Override
    public boolean isEndOfStream(UserBehavior nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(UserBehavior element) {
        return new byte[0];
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return null;
    }
}
