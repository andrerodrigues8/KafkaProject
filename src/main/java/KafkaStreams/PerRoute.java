package KafkaStreams;

import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Objects.Route;
import Objects.Trip;
import serdes.wrapper.RouteSerdes;
import serdes.wrapper.TripSerdes;

public class PerRoute {

    private static final Logger log = LoggerFactory.getLogger(PerRoute.class);

    // Req 4
    public static void getPassengerPerRoute() {
        log.info("Streams for Requirement 4 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "passengers-per-route-" + UUID.randomUUID();
        String inputTopic = "trips";
        String outputTopic = "results-req4";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trip> trips = builder.stream(inputTopic);
        trips.groupBy((key, value) -> value.getRouteId(),
                Grouped.with(Serdes.String(), new TripSerdes()))
                .count()
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }

    // Req 5
    public static void getAvailableSeats() {
        log.info("Streams for Requirement 5 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "available-seats-" + UUID.randomUUID();
        String tripsTopic = "trips";
        String routesTopic = "routes";
        String outputTopic = "results-req5";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Route> routes = builder
                .stream(
                        routesTopic,
                        Consumed.with(Serdes.String(), new RouteSerdes()))
                .selectKey((key, route) -> route.getRouteId())
                .toTable(
                        Materialized.with(
                                Serdes.String(),
                                new RouteSerdes()));
        builder
                .stream(
                        tripsTopic,
                        Consumed.with(Serdes.String(), new TripSerdes()))
                .groupBy(
                        (key, trip) -> trip.getRouteId(),
                        Grouped.with(
                                Serdes.String(),
                                new TripSerdes()))
                .count()
                .join(
                        routes,
                        (passengers, route) -> (long) route.getCapacity() - passengers)
                .toStream()
                .to(
                        outputTopic,
                        Produced.with(Serdes.String(), Serdes.Long()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }
    // Req 6
    public static void getOccupancy() {
        log.info("Streams for Requirement 6 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "occupancy-" + UUID.randomUUID();
        String tripsTopic = "trips";
        String routesTopic = "routes";
        String outputTopic = "results-req6";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Route> routes = builder
                .stream(
                        routesTopic,
                        Consumed.with(Serdes.String(), new RouteSerdes()))
                .selectKey((key, route) -> route.getRouteId())
                .toTable(
                        Materialized.with(
                                Serdes.String(),
                                new RouteSerdes()));
        builder
                .stream(
                        tripsTopic,
                        Consumed.with(Serdes.String(), new TripSerdes()))
                .groupBy(
                        (key, trip) -> trip.getRouteId(),
                        Grouped.with(
                                Serdes.String(),
                                new TripSerdes()))
                .count()
                .join(
                        routes,
                        (passengers, route) -> (double) Math.round((double) passengers / route.getCapacity() * 100 * 100.0) / 100.0)
                .toStream()
                .to(
                        outputTopic,
                        Produced.with(Serdes.String(), Serdes.Double()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }
}
