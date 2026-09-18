package Objects;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Trip {
    private String tripId;
    private String routeId;
    private String origin;
    private String destination;
    private String transportType;
    private String passengerId;

    @JsonCreator
    public Trip(
            @JsonProperty("tripId") String tripId,
            @JsonProperty("routeId") String routeId,
            @JsonProperty("origin") String origin,
            @JsonProperty("destination") String destination,
            @JsonProperty("transportType") String transportType,
            @JsonProperty("passengerId") String passengerId) {
        this.tripId = tripId;
        this.routeId = routeId;
        this.origin = origin;
        this.destination = destination;
        this.transportType = transportType;
        this.passengerId = passengerId;
    }

    public String getRouteId() {
        return routeId;
    }

    public String getOrigin() {
        return origin;
    }

    public String getDestination() {
        return destination;
    }

    public String getTransportType() {
        return transportType;
    }

    public String getPassengerId() {
        return passengerId;
    }

    public String getTripId() {
        return tripId;
    }
}