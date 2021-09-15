package serializer;

import org.apache.kafka.common.serialization.Serializer;
import org.codehaus.jackson.map.ObjectMapper;
import prod.rahul.User;

import java.util.Map;

public class UserSerializer implements Serializer<User> {

    @Override public void configure(Map<String, ?> map, boolean b) {

    }

    @Override public byte[] serialize(String arg0, User arg1) {
        byte[] val = null;
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        try {
            val = objectMapper.writeValueAsString(arg1).getBytes();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return val;
    }

    @Override public void close() {

    }

}
