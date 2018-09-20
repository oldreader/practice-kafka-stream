package ru.serdyuk.kafka;

import ru.serdyuk.kafka.avro.Customer;
import ru.serdyuk.kafka.avro.EnrichedOrder;
import ru.serdyuk.kafka.avro.Order;
import ru.serdyuk.kafka.avro.Product;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

/**
 * Demonstrates how to perform joins between  KStreams and GlobalKTables, i.e. joins that
 * don't require re-partitioning of the input io.confluent.examples.streams.
 * <p>
 * In this example, we join a stream of orders that reads from a topic named
 * "order" with a customers table that reads from a topic named "customer", and a products
 * table that reads fro a topic "product". The join produces an EnrichedOrder object.
 * <p>
 * <br>
 * HOW TO RUN THIS EXAMPLE
 * <p>
 * 1) Start Zookeeper, Kafka, and Confluent Schema Registry. Please refer to <a href='http://docs.confluent.io/current/quickstart.html#quickstart'>QuickStart</a>.
 * <p>
 * 2) Create the input/intermediate/output topics used by this example.
 * <pre>
 * {@code
 * $ bin/kafka-topics --create --topic order \
 *                    --zookeeper ZOO:2181 --partitions 4 --replication-factor 1
 * $ bin/kafka-topics --create --topic customer \
 *                    --zookeeper ZOO:2181 --partitions 3 --replication-factor 1
 * $ bin/kafka-topics --create --topic product \
 *                    --zookeeper ZOO:2181 --partitions 2 --replication-factor 1
 * $ bin/kafka-topics --create --topic enriched-order \
 *                    --zookeeper ZOO:2181 --partitions 4 --replication-factor 1
 * }</pre>
 * Note: The above commands are for the Confluent Platform. For Apache Kafka it should be
 * `bin/kafka-topics.sh ...`.
 * <p>
 * 3) Start this example application either in your IDE or on the command line.
 * <p>
 * If via the command line please refer to <a href='https://github.com/confluentinc/kafka-streams-examples#packaging-and-running'>Packaging</a>.
 * Once packaged you can then run:
 * <pre>
 * {@code
 * $ java -cp practice-5.0.0-standalone.jar ru.serdyuk.kafka.GlobalKTablesExample
 * }
 * </pre>
 * 4) Write some input data to the source topics (e.g. via {@link GlobalKTablesExampleDriver}). The
 * already running example application (step 3) will automatically process this input data and write
 * the results to the output topic.
 * <pre>
 * {@code
 * # Here: Write input data using the example driver. The driver will exit once it has received
 * # all EnrichedOrders
 * $ java -cp practice-5.0.0-standalone.jar ru.serdyuk.kafka.GlobalKTablesExampleDriver
 * }
 * </pre>
 * <p>
 * 5) Once you're done with your experiments, you can stop this example via {@code Ctrl-C}. If needed,
 * also stop the Confluent Schema Registry ({@code Ctrl-C}), then stop the Kafka broker ({@code Ctrl-C}), and
 * only then stop the ZooKeeper instance ({@code Ctrl-C}).
 */
public class GlobalKTablesExample {

  static final String ORDER_TOPIC = "order";
  static final String CUSTOMER_TOPIC = "customer";
  static final String PRODUCT_TOPIC =  "product";
  static final String CUSTOMER_STORE = "customer-store";
  static final String PRODUCT_STORE = "product-store";
  static final String ENRICHED_ORDER_TOPIC = "enriched-order";

  public static void main(String[] args) {
    final String bootstrapServers = args.length > 0 ? args[0] : "localhost:9092";
    final String schemaRegistryUrl = args.length > 1 ? args[1] : "http://localhost:8081";
    final KafkaStreams
        streams =
        createStreams(bootstrapServers, schemaRegistryUrl, "/tmp/kafka-io.confluent.examples.streams-global-tables");

    streams.cleanUp();
    streams.start();

    Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
  }

  public static KafkaStreams createStreams(final String bootstrapServers,
                                           final String schemaRegistryUrl,
                                           final String stateDir) {

    final Properties streamsConfiguration = new Properties();

    streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, "global-tables-example");
    streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, "global-tables-example-client");

    streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, stateDir);

    streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

    // create and configure the SpecificAvroSerdes required in this example
    final SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
    final Map<String, String> serdeConfig =
        Collections.singletonMap(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
            schemaRegistryUrl);
    orderSerde.configure(serdeConfig, false);

    final SpecificAvroSerde<Customer> customerSerde = new SpecificAvroSerde<>();
    customerSerde.configure(serdeConfig, false);

    final SpecificAvroSerde<Product> productSerde = new SpecificAvroSerde<>();
    productSerde.configure(serdeConfig, false);

    final SpecificAvroSerde<EnrichedOrder> enrichedOrdersSerde = new SpecificAvroSerde<>();
    enrichedOrdersSerde.configure(serdeConfig, false);

    final StreamsBuilder builder = new StreamsBuilder();

    // Get the stream of orders
    final KStream<Long, Order> ordersStream = builder.stream(ORDER_TOPIC, Consumed.with(Serdes.Long(), orderSerde));

    // Create a global table for customers. The data from this global table
    // will be fully replicated on each instance of this application.
    final GlobalKTable<Long, Customer>
        customers =
        builder.globalTable(CUSTOMER_TOPIC, Materialized.<Long, Customer, KeyValueStore<Bytes, byte[]>>as(CUSTOMER_STORE)
                .withKeySerde(Serdes.Long())
                .withValueSerde(customerSerde));

    // Create a global table for products. The data from this global table
    // will be fully replicated on each instance of this application.
    final GlobalKTable<Long, Product>
        products =
        builder.globalTable(PRODUCT_TOPIC, Materialized.<Long, Product, KeyValueStore<Bytes, byte[]>>as(PRODUCT_STORE)
                .withKeySerde(Serdes.Long())
                .withValueSerde(productSerde));

    // Join the orders stream to the customer global table. As this is global table
    // we can use a non-key based join with out needing to repartition the input stream
    final KStream<Long, CustomerOrder> customerOrdersStream = ordersStream.join(customers,
                                                                (orderId, order) -> order.getCustomerId(),
                                                                (order, customer) -> new CustomerOrder(customer,
                                                                                                       order));

    // Join the enriched customer order stream with the product global table. As this is global table
    // we can use a non-key based join without needing to repartition the input stream
    final KStream<Long, EnrichedOrder> enrichedOrdersStream = customerOrdersStream.join(products,
                                                                        (orderId, customerOrder) -> customerOrder
                                                                            .productId(),
                                                                        (customerOrder, product) -> new EnrichedOrder(
                                                                            product,
                                                                            customerOrder.customer,
                                                                            customerOrder.order));

    // write the enriched order to the enriched-order topic
    enrichedOrdersStream.to(ENRICHED_ORDER_TOPIC, Produced.with(Serdes.Long(), enrichedOrdersSerde));

    return new KafkaStreams(builder.build(), new StreamsConfig(streamsConfiguration));
  }


  // Helper class for intermediate join between
  // orders & customers
  private static class CustomerOrder {
    private final Customer customer;
    private final Order order;

    CustomerOrder(final Customer customer, final Order order) {
      this.customer = customer;
      this.order = order;
    }

    long productId() {
      return order.getProductId();
    }

  }
}
