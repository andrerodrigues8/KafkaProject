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
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        String routesTopic="routes";
        String outputTopic = "results-req10";
        Properties props = new Properties();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, TripSerdes.class);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, Trip> trips = builder.stream(tripsTopic);
        KTable<String,Long> passengersPerTransport=trips
                .groupBy((key, value) -> value.getTransportType(),Grouped.with(Serdes.String(), new TripSerdes()))
                .count();

         KTable<String,Long> transportsPerType=builder
                .stream(
                        routesTopic,
                        Consumed.with(Serdes.String(), new RouteSerdes()))
                .groupBy((key, value) -> value.getTransportType(),Grouped.with(Serdes.String(), new RouteSerdes()))
                .count();

        transportsPerType
                .join(passengersPerTransport,
                    (transports,passengers)-> (double) Math.round((double) passengers / transports * 100.0) / 100.0)       
                .toStream()
                .to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));

        Topology topology = builder.build();

        KafkaStreams streams = new KafkaStreams(topology, props);

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        streams.cleanUp();
        streams.start();
    }
}