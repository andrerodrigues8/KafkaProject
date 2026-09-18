package serdes.wrapper;

import org.apache.kafka.common.serialization.Serdes;
import serdes.serializers.TripSerializer;
import serdes.deserializers.TripDeserializer;
import Objects.Trip;

public class TripSerdes extends Serdes.WrapperSerde<Trip> {
    public TripSerdes() {
        super(new TripSerializer(), new TripDeserializer());
    }
}