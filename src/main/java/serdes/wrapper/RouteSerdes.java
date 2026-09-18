package serdes.wrapper;

import org.apache.kafka.common.serialization.Serdes;
import serdes.serializers.RouteSerializer;
import serdes.deserializers.RouteDeserializer;
import Objects.Route;

public class RouteSerdes extends Serdes.WrapperSerde<Route> {
    public RouteSerdes() {
        super(new RouteSerializer(), new RouteDeserializer());
    }
}