package KafkaStreams;

import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.stream.Collectors;

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

// Paste the method + helpers into your existing class (which already has `log`,
// Trip, Route, TripSerdes and RouteSerdes).
public class NameStats {
    private static final Logger log = LoggerFactory.getLogger(NameStats.class);

    // Req 15
    public static void getOperaratorWithMostOccupancy() {
        log.info("Streams for Requirement 15 started.");
        String bootstrapServers = "broker1:9092";
        // NEW application id: the topology changed, so old changelog/repartition
        // topics from the previous version are incompatible.
        String applicationId = "operator-with-most-occupancy-v2";
        String tripsTopic = "trips";
        String routesTopic = "routes";
        String outputTopic = "results-req15";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trip> trips = builder.stream(
                tripsTopic,
                Consumed.with(Serdes.String(), new TripSerdes()));

        // 1. Number of passengers (trips) per route -> KTable<routeId, count>
        KTable<String, Long> passengersPerRoute = trips
                .groupBy(
                        (key, value) -> value.getRouteId(),
                        Grouped.with(Serdes.String(), new TripSerdes()))
                .count();

        // 2. Routes as a table (assumes the routes topic is keyed by routeId)
        KTable<String, Route> routes = builder
                .stream(
                        routesTopic,
                        Consumed.with(Serdes.String(), new RouteSerdes()))
                .toTable(
                        Materialized.with(Serdes.String(), new RouteSerdes()));

        // 3. Join: value = "operator|passengers|capacity"
        KTable<String, String> perRoute = passengersPerRoute.join(
                routes,
                (passengers, route) -> route.getOperator()
                        + "|" + passengers
                        + "|" + route.getCapacity());

        // 4 + 5. Re-key by operator and aggregate passengers/capacity.
        // Using a KTable groupBy + adder/subtractor means an updated route count
        // REPLACES its previous contribution instead of being added on top of it.
        KTable<String, String> totalsPerOperator = perRoute
                .groupBy(
                        (routeId, value) -> {
                            String[] p = value.split("\\|"); // [operator, passengers, capacity]
                            return KeyValue.pair(p[0], p[1] + "|" + p[2]);
                        },
                        Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "0|0",
                        (operator, value, agg) -> addPassengerAndCapacity(agg, value),
                        (operator, value, agg) -> subtractPassengerAndCapacity(agg, value),
                        Materialized.with(Serdes.String(), Serdes.String()));

        // 6. Occupancy (%) per operator
        KTable<String, String> occupancyPerOperator = totalsPerOperator
                .mapValues(passengersAndCapacity -> calculateOccupancy(passengersAndCapacity));

        // 7 + 8. Put all operators in one group and keep a map operator -> occupancy.
        // A "max" cannot be subtracted, so we keep every operator's current occupancy
        // and compute the max from the map. Stale values are removed correctly.
        KTable<String, String> highest = occupancyPerOperator
                .groupBy(
                        (operator, occupancy) -> KeyValue.pair("highest", operator + "|" + occupancy),
                        Grouped.with(Serdes.String(), Serdes.String()))
                .aggregate(
                        () -> "",
                        (key, value, agg) -> putOccupancy(agg, value), // adder
                        (key, value, agg) -> removeOccupancy(agg, value), // subtractor
                        Materialized.with(Serdes.String(), Serdes.String()))
                .mapValues(agg -> highestOf(agg));

        // 9. Output: key "highest", value "operator|occupancy"
        highest
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.String()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }

    // ---------- helpers for "operator|occupancy" strings ----------

    private static String operatorOf(String s) {
        return s.substring(0, s.indexOf('|'));
    }

    private static double occupancyOf(String s) {
        return Double.parseDouble(s.substring(s.lastIndexOf('|') + 1));
    }

    // ---------- helpers for "passengers|capacity" strings ----------

    private static long passengersOf(String s) {
        return Long.parseLong(s.substring(0, s.indexOf('|')));
    }

    private static long capacityOf(String s) {
        return Long.parseLong(s.substring(s.indexOf('|') + 1));
    }

    private static String addPassengerAndCapacity(String current, String candidate) {
        long passengers = passengersOf(current) + passengersOf(candidate);
        long capacity = capacityOf(current) + capacityOf(candidate);
        return passengers + "|" + capacity;
    }

    private static String subtractPassengerAndCapacity(String current, String old) {
        long passengers = passengersOf(current) - passengersOf(old);
        long capacity = capacityOf(current) - capacityOf(old);
        return passengers + "|" + capacity;
    }

    private static String calculateOccupancy(String passengersAndCapacity) {
        long passengers = passengersOf(passengersAndCapacity);
        long capacity = capacityOf(passengersAndCapacity);

        if (capacity == 0) {
            return "0.0";
        }

        double occupancy = Math.round((double) passengers / capacity * 100 * 100.0) / 100.0;
        return String.valueOf(occupancy);
    }

    // ---------- helpers for the "current occupancy per operator" state ----------
    // State format: "opA=12.5;opB=40.0" (assumes operator names contain no ';' or
    // '=')

    private static Map<String, Double> parseOccupancies(String state) {
        Map<String, Double> map = new TreeMap<>();
        if (state == null || state.isEmpty()) {
            return map;
        }
        for (String entry : state.split(";")) {
            int i = entry.lastIndexOf('=');
            map.put(entry.substring(0, i), Double.parseDouble(entry.substring(i + 1)));
        }
        return map;
    }

    private static String serializeOccupancies(Map<String, Double> map) {
        return map.entrySet().stream()
                .map(e -> e.getKey() + "=" + e.getValue())
                .collect(Collectors.joining(";"));
    }

    // value = "operator|occupancy"
    private static String putOccupancy(String state, String value) {
        Map<String, Double> map = parseOccupancies(state);
        map.put(operatorOf(value), occupancyOf(value));
        return serializeOccupancies(map);
    }

    // Only removes the entry if it still holds the old value, so it is safe
    // regardless of whether the adder or subtractor runs first.
    private static String removeOccupancy(String state, String oldValue) {
        Map<String, Double> map = parseOccupancies(state);
        map.remove(operatorOf(oldValue), occupancyOf(oldValue));
        return serializeOccupancies(map);
    }

    // Returns "operator|occupancy" for the highest occupancy, or null if empty
    private static String highestOf(String state) {
        return parseOccupancies(state).entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(e -> e.getKey() + "|" + e.getValue())
                .orElse(null);
    }

    // Req 16
    public static void getPassengerWithMostTrips() {
        log.info("Streams for Requirement 16 started.");
        String bootstrapServers = "broker1:9092";
        String applicationId = "passengers-most-trips";
        String inputTopic = "trips";
        String outputTopic = "results-req16";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trip> trips = builder.stream(
                inputTopic,
                Consumed.with(
                        Serdes.String(),
                        new TripSerdes()));

        trips
                // 1. Group trips by passenger
                .groupBy(
                        (key, value) -> value.getPassengerId(),
                        Grouped.with(
                                Serdes.String(),
                                new TripSerdes()))

                // 2. Count trips for each passenger
                .count()

                // 3. Put every passenger into the same group
                .toStream()
                .map(
                        (passenger, tripCount) -> KeyValue.pair(
                                "highest",
                                passenger + "|" + tripCount))

                // 4. Group all passengers together
                .groupByKey(
                        Grouped.with(
                                Serdes.String(),
                                Serdes.String()))

                // 5. Keep the passenger with the highest trip count
                .reduce(
                        (current, candidate) -> tripsOf(candidate) > tripsOf(current)
                                ? candidate
                                : current)

                // 6. Output: passenger|number of trips
                .toStream()
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

    private static long tripsOf(String passengerAndTrips) {

        return Long.parseLong(
                passengerAndTrips.substring(
                        passengerAndTrips.indexOf('|') + 1));
    }
}