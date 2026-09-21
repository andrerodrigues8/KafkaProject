package generators;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import Objects.Route;
import Objects.Trip;
import serdes.deserializers.RouteDeserializer;
import serdes.serializers.TripSerializer;

public class TripGenerator {
    private static final Logger log = LoggerFactory.getLogger(TripGenerator.class);
    
    public static List<Route> getRoutes() throws Exception {
        log.warn("Consuming routes from Routes topic");

        String bootstrapServers = "broker1:9092";
        String groupId = "routes";
        String topic = "routes";

        List<Route> routes = new ArrayList<>();

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                RouteDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, Route> consumer = new KafkaConsumer<>(properties);
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        // poll for new data
        Long startTime = System.currentTimeMillis();
        long timeout = 5000; // 5 seconds

        while (System.currentTimeMillis() - startTime < timeout) {
            ConsumerRecords<String, Route> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, Route> record : records) {
                // log.warn("Key: " + record.key() + ", Value: " + record.value());

                
                routes.add(record.value());

            }
        }
        consumer.close();
        return routes;

    }
    public static void generateTrips() throws Exception {
        log.info("Generating Trips");

        String bootstrapServers = "broker1:9092";
        String topic = "trips";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, TripSerializer.class.getName());

        KafkaProducer<String, Trip> producer = new KafkaProducer<>(properties);

        List<Route> routes = getRoutes();
        int tripCounter = 1;
        Random rand = new Random();
        for (int i = 0; i < 10000; i++) {
            Route route = routes.get(rand.nextInt(routes.size()));
            String tripId = "trip-" + tripCounter++;
            String passengerId="passenger-"+(rand.nextInt(10000)+1);
            Trip trip=new Trip(tripId,route.getRouteId(),route.getOrigin(),route.getDestination(),route.getTransportType(),passengerId);
            ProducerRecord<String, Trip> record = new ProducerRecord<>(topic, tripId, trip);

            producer.send(record);
        }
        producer.flush();
        producer.close();
        log.info("All trips sent");
    }
    
}
