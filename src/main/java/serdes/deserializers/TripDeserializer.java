package serdes.deserializers;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;


import Objects.Trip;

public class TripDeserializer implements Deserializer<Trip> {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public Trip deserialize(String topic, byte[] data) {
        if (data == null) {
            return null;
        }
        try {
            return objectMapper.readValue(data, Trip.class);
        } catch (Exception e) {
            throw new SerializationException("Error deserializing Route from topic " + topic, e);
        }
    }
}