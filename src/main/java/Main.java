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
        deleteTopic("results-req7");
        deleteTopic("results-req8");
        deleteTopic("results-req9");
        deleteTopic("results-req10");
        deleteTopic("results-req11");
        deleteTopic("results-req12");
        deleteTopic("results-req13");
        deleteTopic("results-req14");
        deleteTopic("results-req15");
        deleteTopic("results-req16");
        RouteGenerator.generateRoutes();
        Thread.sleep(1000);
        TripGenerator.generateTrips();
        Thread.sleep(1000);
        NameStats.getPassengerWithMostTrips();

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
