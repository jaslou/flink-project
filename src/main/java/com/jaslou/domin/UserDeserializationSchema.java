package com.jaslou.domin;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class UserDeserializationSchema implements DeserializationSchema<UserBehavior>, SerializationSchema<UserBehavior> {

    @Override
    public UserBehavior deserialize(byte[] message) throws IOException {
        return JSONObject.parseObject(message, UserBehavior.class);
    }

    @Override
    public boolean isEndOfStream(UserBehavior nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(UserBehavior element) {
        return JSONObject.toJSONString(element).getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
