package KafkaStreams;

import java.time.Duration;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.kstream.WindowedSerdes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Objects.Route;
import Objects.Trip;
import serdes.wrapper.RouteSerdes;
import serdes.wrapper.TripSerdes;

public class TumblingWindows {
    private static final Logger log = LoggerFactory.getLogger(TumblingWindows.class);

    // Req 13
    public static void getTransportWithMostPassengersInTheLastHour() {
        log.info("Streams for Requirement 13 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "transport-with-most-passengers-in-the-last-hour";
        String tripsTopic = "trips";
        String outputTopic = "results-req13";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trip> trips = builder.stream(tripsTopic);
        // Get the number of passengers per transport type (Transport,Passengers)
        KTable<Windowed<String>, Long> passengersPerTransport = trips
                .groupBy((key, value) -> value.getTransportType(), Grouped.with(Serdes.String(), new TripSerdes()))
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(
                                Duration.ofSeconds(5)))
                .count();
        passengersPerTransport
                .toStream()
                .map(
                        (windowedTransport, passengers) -> KeyValue.pair(
                                new Windowed<>(
                                        "highest",
                                        windowedTransport.window()),
                                windowedTransport.key() + "|" + passengers))
                .groupByKey(
                        Grouped.with(
                                WindowedSerdes.timeWindowedSerdeFrom(
                                        String.class,
                                        Duration.ofSeconds(5).toMillis()),
                                Serdes.String()))
                .reduce(
                        (current, candidate) -> passengersOf(candidate) > passengersOf(current)
                                ? candidate
                                : current)
                .toStream()
                .map(
                        (windowedTransport, transportAndPassengers) -> KeyValue.pair(
                                windowedTransport,
                                transportOf(transportAndPassengers)))
                .to(
                        outputTopic,
                        Produced.with(
                                WindowedSerdes.timeWindowedSerdeFrom(
                                        String.class,
                                        Duration.ofSeconds(5).toMillis()),
                                Serdes.String()));
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }

    private static long passengersOf(String transportAndCount) {
        return Long.parseLong(transportAndCount.substring(transportAndCount.lastIndexOf('|') + 1));
    }

    private static String transportOf(String transportAndPassengers) {
        return transportAndPassengers.substring(
                0,
                transportAndPassengers.indexOf('|'));
    }

    // Req 14
    public static void getRoutesWithLeastOccupancyPerTransportInTheLastHour() {
        log.info("Streams for Requirement 14 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "routes-with-least-occupancy-per-transport";
        String tripsTopic = "trips";
        String routesTopic = "routes";
        String outputTopic = "results-req14";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        // 1. Get routes, keyed by route ID
        KTable<String, Route> routes = builder
                .stream(
                        routesTopic,
                        Consumed.with(
                                Serdes.String(),
                                new RouteSerdes()))
                .toTable(
                        Materialized.with(
                                Serdes.String(),
                                new RouteSerdes()));

        // 2. Count passengers per route, per 5-second window
        KTable<Windowed<String>, Long> passengersPerRoute = builder
                .stream(
                        tripsTopic,
                        Consumed.with(
                                Serdes.String(),
                                new TripSerdes()))
                .groupBy(
                        (key, trip) -> trip.getRouteId(),
                        Grouped.with(
                                Serdes.String(),
                                new TripSerdes()))
                .windowedBy(
                        TimeWindows.ofSizeWithNoGrace(
                                Duration.ofSeconds(1)))
                .count();

        // 3. Join passenger counts with route information
        passengersPerRoute
                .toStream()
                .map(
                        (windowedRoute, passengers) -> KeyValue.pair(
                                windowedRoute.key(),
                                windowedRoute.window().start()
                                        + "|"
                                        + windowedRoute.window().end()
                                        + "|"
                                        + passengers))
                .join(
                        routes,
                        (windowInfo, route) -> {

                            long passengers = passengersOf(windowInfo);

                            double occupancy = Math.round(
                                    (double) passengers
                                            / route.getCapacity()
                                            * 100
                                            * 100.0)
                                    / 100.0;

                            return startOf(windowInfo)
                                    + "|"
                                    + endOf(windowInfo)
                                    + "|"
                                    + route.getTransportType()
                                    + "|"
                                    + route.getRouteId()
                                    + "|"
                                    + occupancy;
                        })

                // 4. Change key to window + transport type
                .map(
                        (routeId, transportRouteOccupancy) -> KeyValue.pair(
                                startOf(transportRouteOccupancy)
                                        + "|"
                                        + endOf(transportRouteOccupancy)
                                        + "|"
                                        + transportTypeOf(transportRouteOccupancy),
                                transportRouteOccupancy))

                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                Serdes.String()))

                // 5. Find the least occupied route within each
                // window + transport type
                .reduce(
                        (current, candidate) -> occupancyOf(candidate) < occupancyOf(current)
                                ? candidate
                                : current)

                // 6. Output transport/window -> route
                .toStream()
                .map(
                        (windowTransport, transportRouteOccupancy) -> KeyValue.pair(
                                windowTransport,
                                routeIdOf(transportRouteOccupancy)))

                .to(
                        outputTopic,
                        Produced.with(
                                Serdes.String(),
                                Serdes.String()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }

    private static double occupancyOf(String transportRouteOccupancy) {
        return Double.parseDouble(
                transportRouteOccupancy.substring(
                        transportRouteOccupancy.lastIndexOf('|') + 1));
    }

    private static String routeIdOf(String transportRouteOccupancy) {
        String[] parts = transportRouteOccupancy.split("\\|");
        return parts[3];
    }

    private static String startOf(String s) {
        return s.substring(0, s.indexOf('|'));
    }

    private static String endOf(String s) {
        int first = s.indexOf('|');
        int second = s.indexOf('|', first + 1);
        return s.substring(first + 1, second);
    }

    private static String transportTypeOf(String s) {
        int first = s.indexOf('|');
        int second = s.indexOf('|', first + 1);
        int third = s.indexOf('|', second + 1);

        return s.substring(second + 1, third);
    }
}
