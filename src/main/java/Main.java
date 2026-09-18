import generators.*;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import KafkaStreams.*;

public class Main {
    public static void main(String[] args) throws Exception {

        deleteTopic("routes");
        deleteTopic("trips");
        deleteTopic("results-req4");
        deleteTopic("results-req5");
        deleteTopic("results-req6");
        
        RouteGenerator.generateRoutes();
        Thread.sleep(1000);
        TripGenerator.generateTrips();
        Thread.sleep(1000);
        PerRoute.getOccupancy();
        
    }

    public static void deleteTopic(String topic) {
        Properties props = new Properties();
        props.put(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG,
                "broker1:9092");

        try (AdminClient admin = AdminClient.create(props)) {
            admin.deleteTopics(Collections.singletonList(topic)).all().get();
        } catch (Exception e) {
        }
    }
}
