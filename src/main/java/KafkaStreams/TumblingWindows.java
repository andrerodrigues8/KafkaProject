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
import org.apache.kafka.streams.kstream.Joined;
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
                                .groupBy((key, value) -> value.getTransportType(),
                                                Grouped.with(Serdes.String(), new TripSerdes()))
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
                                .map((windowedTransport, transport) -> KeyValue.pair(
                                                windowedTransport.key()
                                                                + "|"
                                                                + windowedTransport.window().start()
                                                                + "|"
                                                                + windowedTransport.window().end(),
                                                transport))
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

        private static long passengersOf(String transportAndCount) {
                return Long.parseLong(transportAndCount.substring(transportAndCount.lastIndexOf('|') + 1));
        }

        // private static String transportOf(String transportAndPassengers) {
        // return transportAndPassengers.substring(
        // 0,
        // transportAndPassengers.indexOf('|'));
        // }

        // Req 14
        public static void getLeastOccupiedTransportTypeInTheLastHour() {
                log.info("Streams for Requirement 14 started.");
                Properties props = new Properties();
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "least-occupied-transport-type");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "broker1:9092");
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                // Demo only: emit every update instead of batching in the cache
                props.put(StreamsConfig.STATESTORE_CACHE_MAX_BYTES_CONFIG, 0);

                StreamsBuilder builder = new StreamsBuilder();

                // 1. Routes table, keyed by routeId
                KTable<String, Route> routes = builder
                                .stream("routes", Consumed.with(Serdes.String(), new RouteSerdes()))
                                .toTable(Materialized.with(Serdes.String(), new RouteSerdes()));

                // 2. Total capacity per transport type
                KTable<String, Long> capacityByType = routes
                                .groupBy(
                                                (routeId, route) -> KeyValue.pair(route.getTransportType(),
                                                                (long) route.getCapacity()),
                                                Grouped.with(Serdes.String(), Serdes.Long()))
                                .aggregate(
                                                () -> 0L,
                                                (type, cap, agg) -> agg + cap, // adder
                                                (type, cap, agg) -> agg - cap, // subtractor
                                                Materialized.with(Serdes.String(), Serdes.Long()));

                // 3. Trips -> transport type -> count per 1-hour tumbling window
                KTable<Windowed<String>, Long> tripsPerType = builder
                                .stream("trips", Consumed.with(Serdes.String(), new TripSerdes()))
                                .selectKey((k, trip) -> trip.getRouteId())
                                .join(routes, (trip, route) -> route.getTransportType(),
                                                Joined.with(Serdes.String(), new TripSerdes(), new RouteSerdes()))
                                .selectKey((routeId, type) -> type)
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                                .count();

                tripsPerType
                                .toStream()
                                // key = type, value = start|end|count
                                .map((w, count) -> KeyValue.pair(
                                                w.key(), w.window().start() + "|" + w.window().end() + "|" + count))
                                // 4. occupancy = passengers / total capacity of that type
                                .join(capacityByType,
                                                (windowAndCount, capacity) -> {
                                                        String[] p = windowAndCount.split("\\|");
                                                        double occ = Math.round(
                                                                        Double.parseDouble(p[2]) / capacity * 10000.0)
                                                                        / 100.0;
                                                        return p[0] + "|" + p[1] + "|" + occ;
                                                },
                                                Joined.with(Serdes.String(), Serdes.String(), Serdes.Long()))
                                // 5. re-key by window only, value = type|occupancy
                                .map((type, v) -> {
                                        String[] p = v.split("\\|");
                                        return KeyValue.pair(p[0] + "|" + p[1], type + "=" + p[2]);
                                })
                                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()))
                                // 6. keep the LATEST occupancy per type (replaces stale values)
                                .aggregate(
                                                () -> "",
                                                (window, typeOcc, agg) -> upsert(agg, typeOcc),
                                                Materialized.with(Serdes.String(), Serdes.String()))
                                // 7. pick the minimum
                                .toStream()
                                .mapValues(TumblingWindows::minEntry)
                                .to("results-req14", Produced.with(Serdes.String(), Serdes.String()));

                KafkaStreams streams = new KafkaStreams(builder.build(), props);
                Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
                
                streams.start();
        }

        // "Bus=12.5;Train=3.1" -> replace or add the entry for the type
        private static String upsert(String agg, String typeOcc) {
                String type = typeOcc.substring(0, typeOcc.indexOf('='));
                StringBuilder sb = new StringBuilder();
                for (String e : agg.split(";")) {
                        if (!e.isEmpty() && !e.startsWith(type + "="))
                                sb.append(e).append(';');
                }
                return sb.append(typeOcc).toString();
        }

        // returns "Type|occupancy" for the smallest occupancy
        private static String minEntry(String agg) {
                String best = null;
                double bestOcc = Double.MAX_VALUE;
                for (String e : agg.split(";")) {
                        if (e.isEmpty())
                                continue;
                        String[] p = e.split("=");
                        double occ = Double.parseDouble(p[1]);
                        if (occ < bestOcc) {
                                bestOcc = occ;
                                best = p[0];
                        }
                }
                return best + "|" + bestOcc;
        }

        // private static double occupancyOf(String transportRouteOccupancy) {
        //         return Double.parseDouble(
        //                         transportRouteOccupancy.substring(
        //                                         transportRouteOccupancy.lastIndexOf('|') + 1));
        // }

        // private static String routeIdOf(String transportRouteOccupancy) {
        //         String[] parts = transportRouteOccupancy.split("\\|");
        //         return parts[3];
        // }

        // private static String startOf(String s) {
        //         return s.substring(0, s.indexOf('|'));
        // }

        // private static String endOf(String s) {
        //         int first = s.indexOf('|');
        //         int second = s.indexOf('|', first + 1);
        //         return s.substring(first + 1, second);
        // }

        // private static String transportTypeOf(String s) {
        //         int first = s.indexOf('|');
        //         int second = s.indexOf('|', first + 1);
        //         int third = s.indexOf('|', second + 1);

        //         return s.substring(second + 1, third);
        // }
}
