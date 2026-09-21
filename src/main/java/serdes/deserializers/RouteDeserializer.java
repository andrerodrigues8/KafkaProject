package serdes.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import Objects.Route;

public class RouteDeserializer implements Deserializer<Route> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Route deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, Route.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Route from topic " + topic, e);
        }
    }
}