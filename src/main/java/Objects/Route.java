package Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Route {
    private String routeId;
    private String origin;
    private String destination;
    private String transportType;
    private String operator;
    private int capacity;

    @JsonCreator
    public Route(
            @JsonProperty("routeId") String routeId,
            @JsonProperty("origin") String origin,
            @JsonProperty("destination") String destination,
            @JsonProperty("transportType") String transportType,
            @JsonProperty("operator") String operator,
            @JsonProperty("capacity") int capacity) {
        this.routeId = routeId;
        this.origin = origin;
        this.destination = destination;
        this.transportType = transportType;
        this.operator = operator;
        this.capacity = capacity;
    }

    public String getRouteId() { return routeId; }
    public String getOrigin() { return origin; }
    public String getDestination() { return destination; }
    public String getTransportType() { return transportType; }
    public String getOperator() { return operator; }
    public int getCapacity() { return capacity; }
}