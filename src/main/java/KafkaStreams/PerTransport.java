package KafkaStreams;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Objects.Route;
import Objects.Trip;
import serdes.wrapper.RouteSerdes;
import serdes.wrapper.TripSerdes;

public class PerTransport {
        private static final Logger log = LoggerFactory.getLogger(PerTransport.class);

        // Req 10
        public static void getAveragePerTransport() {
                log.info("Streams for Requirement 10 started.");
                String bootstrapServers = "broker1:9092";
                String applicationId = "average-per-transport";
                String tripsTopic = "trips";
                String routesTopic = "routes";
                String outputTopic = "results-req10";
                Properties props = new Properties();

                props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                StreamsBuilder builder = new StreamsBuilder();

                KStream<String, Trip> trips = builder.stream(tripsTopic);
                KTable<String, Long> passengersPerTransport = trips
                                .groupBy((key, value) -> value.getTransportType(),
                                                Grouped.with(Serdes.String(), new TripSerdes()))
                                .count();

                KTable<String, Long> transportsPerType = builder
                                .stream(
                                                routesTopic,
                                                Consumed.with(Serdes.String(), new RouteSerdes()))
                                .groupBy((key, value) -> value.getTransportType(),
                                                Grouped.with(Serdes.String(), new RouteSerdes()))
                                .count();

                transportsPerType
                                .join(passengersPerTransport,
                                                (transports, passengers) -> (double) Math
                                                                .round((double) passengers / transports * 100.0)
                                                                / 100.0)
                                .toStream()
                                .mapValues(String::valueOf)
                                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

                Topology topology = builder.build();

                KafkaStreams streams = new KafkaStreams(topology, props);

                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
                
                streams.start();
        }

        // Req 11
        public static void getTransportWithMostPassengers() {
                log.info("Streams for Requirement 11 started.");
                String bootstrapServers = "broker1:9092";
                String applicationId = "transport-with-most-passengers";
                String tripsTopic = "trips";
                String outputTopic = "results-req11";
                Properties props = new Properties();

                props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

                StreamsBuilder builder = new StreamsBuilder();

                KStream<String, Trip> trips = builder.stream(tripsTopic);
                // Get the number of passengers per transport type (Transport,Passengers)
                KTable<String, Long> passengersPerTransport = trips
                                .groupBy((key, value) -> value.getTransportType(),
                                                Grouped.with(Serdes.String(), new TripSerdes()))
                                .count();
                passengersPerTransport
                                .toStream()
                                .map((transport, passengers) -> KeyValue.pair("highest", transport + "|" + passengers))
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                                .reduce((current, candidate) -> passengersOf(candidate) > passengersOf(current)
                                                ? candidate
                                                : current)
                                .toStream()
                                .map(
                                                (key, transportAndPassengers) -> KeyValue.pair(
                                                                key,
                                                                transportOf(transportAndPassengers)))
                                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

                Topology topology = builder.build();

                KafkaStreams streams = new KafkaStreams(topology, props);

                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
                
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

        // Req 12
        public static void getRoutesWithLeastOccupancyPerTransport() {
                log.info("Streams for Requirement 12 started.");
                String bootstrapServers = "broker1:9092";
                String applicationId = "routes-with-least-occupancy-per-transport";
                String tripsTopic = "trips";
                String routesTopic = "routes";
                String outputTopic = "results-req12";
                Properties props = new Properties();

                props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
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

                // 2. Count passengers per route
                KTable<String, Long> passengersPerRoute = builder
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
                                .count();

                // 3. Calculate occupancy for each route
                passengersPerRoute
                                .join(
                                                routes,
                                                (passengers, route) -> {

                                                        double occupancy = Math.round(
                                                                        (double) passengers
                                                                                        / route.getCapacity()
                                                                                        * 100
                                                                                        * 100.0)
                                                                        / 100.0;

                                                        return route.getTransportType()
                                                                        + "|"
                                                                        + route.getRouteId()
                                                                        + "|"
                                                                        + occupancy;
                                                })

                                // 4. Group routes by transport type
                                .toStream()
                                .map(
                                                (routeId, transportRouteOccupancy) -> KeyValue.pair(
                                                                transportRouteOccupancy.substring(
                                                                                0,
                                                                                transportRouteOccupancy.indexOf('|')),
                                                                transportRouteOccupancy))
                                .groupByKey(
                                                Grouped.with(
                                                                Serdes.String(),
                                                                Serdes.String()))

                                // 5. Keep the route with the lowest occupancy
                                .reduce(
                                                (current, candidate) -> occupancyOf(candidate) < occupancyOf(current)
                                                                ? candidate
                                                                : current)
                                // 6. Write the result
                                .toStream()
                                .map(
                                                (transport, transportRouteOccupancy) -> KeyValue.pair(
                                                                transport,
                                                                routeIdOf(transportRouteOccupancy)))
                                .to(
                                                outputTopic,
                                                Produced.with(
                                                                Serdes.String(),
                                                                Serdes.String()));

                Topology topology = builder.build();

                KafkaStreams streams = new KafkaStreams(topology, props);

                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
                
                streams.start();
        }

        private static double occupancyOf(String transportRouteOccupancy) {
                return Double.parseDouble(
                                transportRouteOccupancy.substring(
                                                transportRouteOccupancy.lastIndexOf('|') + 1));
        }

        private static String routeIdOf(String transportRouteOccupancy) {
                String[] parts = transportRouteOccupancy.split("\\|");
                return parts[1];
        }

}