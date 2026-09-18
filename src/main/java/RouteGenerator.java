import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.Random;


import java.util.ArrayList;
import java.util.List;

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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import Objects.Route;
import serdes.serializers.RouteSerializer;

public class RouteGenerator {

    private static final Logger log = LoggerFactory.getLogger(RouteGenerator.class);

    public static void main(String[] args) throws Exception {
        generateRoutes();

    }

    public static List<String> getSuppliers() throws Exception {
        log.warn("Consuming suppliers from DBInfo topic");

        String bootstrapServers = "broker1:9092";
        String groupId = "suppliers";
        String topic = "dbinfo-route_suppliers";

        List<String> suppliers = new ArrayList<>();

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        // poll for new data
        Long startTime = System.currentTimeMillis();
        long timeout = 5000; // 5 seconds
        ObjectMapper mapper = new ObjectMapper();
        while (System.currentTimeMillis() - startTime < timeout) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                // log.warn("Key: " + record.key() + ", Value: " + record.value());

                JsonNode json = mapper.readTree(record.value());
                String name = json.get("payload").get("name").asText();

                if (!suppliers.contains(name)) {
                    suppliers.add(name);
                }
            }
        }
        consumer.close();
        return suppliers;

    }

    public static List<String> getCapacities() throws Exception {
        log.warn("Consuming capacities from DBInfo topic");

        String bootstrapServers = "broker1:9092";
        String groupId = "capacities";
        String topic = "dbinfo-route_capacities";

        List<String> capacities = new ArrayList<>();

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        // subscribe consumer to our topic(s)
        consumer.subscribe(Arrays.asList(topic));
        // poll for new data
        Long startTime = System.currentTimeMillis();
        long timeout = 5000; // 5 seconds
        ObjectMapper mapper = new ObjectMapper();
        while (System.currentTimeMillis() - startTime < timeout) {

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                // log.warn("Key: " + record.key() + ", Value: " + record.value());
                JsonNode json = mapper.readTree(record.value());
                String name = json.get("payload").get("capacity").asText();
                if (!capacities.contains(name)) {
                    capacities.add(name);
                }
            }
        }
        consumer.close();
        return capacities;

    }

    public static void generateRoutes() throws Exception {
        log.info("Generating Routes");

        String bootstrapServers = "broker1:9092";
        String topic = "routes";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, RouteSerializer.class.getName());

        KafkaProducer<String, Route> producer = new KafkaProducer<>(properties);

        List<String> suppliers = getSuppliers();
        List<String> capacities = getCapacities();
        List<String> transportTypes = Arrays.asList("Bus", "Taxi", "Train", "Metro", "Scooter");

        List<String> origins = Arrays.asList("Coimbra", "Porto", "Lisbon", "Aveiro");
        List<String> destinations = Arrays.asList("Porto", "Lisbon", "Braga", "Faro");
        int routeCounter = 1;
        Random rand = new Random();
        for (int i = 0; i < 1000; i++) {
            String operator = suppliers.get(rand.nextInt(suppliers.size()));
            int capacity = Integer.parseInt(capacities.get(rand.nextInt(capacities.size())));

            String routeId = "route-" + routeCounter++;
            String transportType = transportTypes.get(rand.nextInt(transportTypes.size()));
            String origin = origins.get(rand.nextInt(origins.size()));
            String destination = destinations.get(rand.nextInt(destinations.size()));

            Route route = new Route(routeId, origin, destination, transportType, operator, capacity);
            ProducerRecord<String, Route> record = new ProducerRecord<>(topic, routeId, route);

            producer.send(record);
        }
        producer.flush();
        producer.close();
    }

    

}
