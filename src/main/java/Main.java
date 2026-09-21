import generators.*;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import KafkaStreams.*;

public class Main {
    public static void main(String[] args) throws Exception {

        RouteGenerator.generateRoutes();
        TripGenerator.generateTrips();
        // Req 4
        PerRoute.getPassengerPerRoute();
        // Req 5
        PerRoute.getAvailableSeats();
        // Req 6
        PerRoute.getOccupancy();
        // Req 7
        AllRoutes.getTotalPassengers();
        // Req 8
        AllRoutes.getTotalAvailableSeats();
        // Req 9
        AllRoutes.getTotalOccupancy();
        // Req 10
        PerTransport.getAveragePerTransport();
        // Req 11
        PerTransport.getTransportWithMostPassengers();
        // Req 12
        PerTransport.getRoutesWithLeastOccupancyPerTransport();
        // Req 13
        TumblingWindows.getTransportWithMostPassengersInTheLastHour();
        // Req 14
        TumblingWindows.getLeastOccupiedTransportTypeInTheLastHour();
        // Req 15
        NameStats.getOperaratorWithMostOccupancy();
        // Req 16
        NameStats.getPassengerWithMostTrips();

    }

}
