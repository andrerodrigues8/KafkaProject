# Urban Mobility Kafka Project

A Kafka-based distributed system developed to simulate an Urban Mobility platform, processing route and passenger trip data through Apache Kafka and Kafka Streams.

## Description

This project simulates an Urban Mobility company that relies heavily on Apache Kafka to manage route information, passenger trips, and real-time analytics.

The system is built around several Kafka topics. `DBInfo` topics provide route suppliers and passenger capacity information from the database, while the `Routes` topic continuously receives simulated routes from different transport operators. The `Trips` topic receives simulated passenger trips referencing available routes and transport types.

Kafka Streams applications consume these topics and perform the required real-time computations, including route occupancy, available seats, passenger averages, total trips, transport usage, and operator statistics. The resulting metrics are written to Kafka `Results` topics, from which Kafka Connectors automatically persist the results back into the database.

The project runs using Docker and includes a multi-broker Kafka setup to take advantage of Apache Kafka's fault-tolerance mechanisms. This allows the system to continue operating and preserve information even if one of the Kafka brokers becomes unavailable.

Kafka connectors are also configured to automatically extract information from the database and write computed results back to it.

## Getting Started

### Dependencies

* Docker: The system uses Docker containers to run the different services and applications.
* Apache Kafka: Used for communication between the applications and for stream processing.

### Installing

* Clone or download the repository to your local machine.
* Make sure Docker is installed and running.
* Configure the provided YAML and Docker configuration files according to the project environment.
* Optionally configure the `config` directory if additional service configuration is required.

### Executing program

* Open your terminal in the root project directory.
* Build and start all containers using Docker Compose:

```bash
docker-compose up --build
```

* Once all services are running, the system will start generating routes and passenger trips automatically.
* The Kafka Streams applications will process the incoming data and publish the calculated results to the appropriate Kafka topics.
* Kafka Connect will automatically transfer database information to the `DBInfo` topics and write computed results from the `Results` topics back to the database.

### Main Kafka Topics

The system uses the following main topic groups:

```text
DBInfo
```

Contains information extracted from the database, including route suppliers and route passenger capacities.

```text
Routes
```

Contains simulated routes. Each route includes information such as:

* Route identifier
* Passenger capacity
* Origin
* Destination
* Transport type
* Route operator

```text
Trips
```

Contains simulated passenger trips. Each trip includes:

* Route identifier
* Origin
* Destination
* Passenger identifier
* Transport type

```text
Results
```

Contains the metrics calculated by the Kafka Streams applications before they are written back to the database through Kafka Connect.

## Fault Tolerance

The Kafka infrastructure is configured with multiple brokers to provide fault tolerance and prevent data loss in the event of a broker failure.

## Help

* If the applications fail to communicate with Kafka, check that all Kafka brokers and required services are running.
* Check the Docker container logs for errors:

```bash
docker-compose logs
```

* To inspect the logs of a specific service:

```bash
docker-compose logs <service-name>
```

* If Kafka Streams is not producing results, verify that the required Kafka topics exist.
* If database results are not being updated, check the Kafka Connect configuration and connector logs.
* Ensure the required files are correctly placed in the `lib` and `config` directories.
* If messages stop being processed after a broker failure, verify the replication factor, broker configuration, and Kafka topic settings.

## Author

André Rodrigues

rodrigues.n.andre46@gmail.com
