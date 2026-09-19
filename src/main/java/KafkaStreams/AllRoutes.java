package KafkaStreams;

import java.util.Properties;

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

import Objects.Trip;
import serdes.wrapper.RouteSerdes;
import serdes.wrapper.TripSerdes;

public class AllRoutes {
    private static final Logger log = LoggerFactory.getLogger(AllRoutes.class);

    // Req 7
    public static void getTotalPassengers() {
        log.info("Streams for Requirement 7 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "total-passengers";
        String inputTopic = "trips";
        String outputTopic = "results-req7";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trip> trips = builder.stream(inputTopic);
        trips.groupBy((key, value) -> "Total",
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

    // Req 8
    public static void getTotalAvailableSeats() {
        log.info("Streams for Requirement 8 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "total-available-seats";
        String tripsTopic = "trips";
        String routesTopic = "routes";
        String outputTopic = "results-req8";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Long> totalCapacity = builder
                .stream(
                        routesTopic,
                        Consumed.with(Serdes.String(), new RouteSerdes()))
                .groupBy(
                        (key, route) -> "Total",
                        Grouped.with(
                                Serdes.String(),
                                new RouteSerdes()))
                .aggregate(
                        () -> 0L,
                        (key, route, total) -> total + route.getCapacity(),
                        Materialized.with(
                                Serdes.String(),
                                Serdes.Long()));

        KTable<String, Long> totalPassengers = builder
                .stream(
                        tripsTopic,
                        Consumed.with(Serdes.String(), new TripSerdes()))
                .groupBy(
                        (key, trip) -> "Total",
                        Grouped.with(
                                Serdes.String(),
                                new TripSerdes()))
                .count();

        totalPassengers
                .join(
                        totalCapacity,
                        (passengers, capacity) -> capacity - passengers)
                .toStream()
                .to(
                        outputTopic,
                        Produced.with(
                                Serdes.String(),
                                Serdes.Long()));
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }
    // Req 9
    public static void getTotalOccupancy() {
        log.info("Streams for Requirement 9 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "total-occupancy";
        String tripsTopic = "trips";
        String routesTopic = "routes";
        String outputTopic = "results-req9";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Long> totalCapacity = builder
                .stream(
                        routesTopic,
                        Consumed.with(Serdes.String(), new RouteSerdes()))
                .groupBy(
                        (key, route) -> "Total",
                        Grouped.with(
                                Serdes.String(),
                                new RouteSerdes()))
                .aggregate(
                        () -> 0L,
                        (key, route, total) -> total + route.getCapacity(),
                        Materialized.with(
                                Serdes.String(),
                                Serdes.Long()));

        KTable<String, Long> totalPassengers = builder
                .stream(
                        tripsTopic,
                        Consumed.with(Serdes.String(), new TripSerdes()))
                .groupBy(
                        (key, trip) -> "Total",
                        Grouped.with(
                                Serdes.String(),
                                new TripSerdes()))
                .count();

        totalPassengers
                .join(
                        totalCapacity,
                        (passengers, capacity) -> (double) Math.round((double) passengers / capacity * 100 * 100.0) / 100.0)
                .toStream()
                .to(
                        outputTopic,
                        Produced.with(
                                Serdes.String(),
                                Serdes.Double()));
        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }

}
