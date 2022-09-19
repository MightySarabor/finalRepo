package de.hstr.bigdata;

import de.hstr.bigdata.Util.Json.JSONSerde;
import de.hstr.bigdata.Util.MyProducer;
import de.hstr.bigdata.Util.POJOGenerator;
import de.hstr.bigdata.Util.pojos.OrderPOJO;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Count_Order_By_Name {
    private static final int NUMBER_OF_CUSTOMERS = 2;
    private static final String[] customers = {"Peter Pan", "Hans Mueller", "Guenther Jauch"};
        static void runKafkaStreams(final KafkaStreams streams) {
            final CountDownLatch latch = new CountDownLatch(1);
            streams.setStateListener((newState, oldState) -> {
                if (oldState == KafkaStreams.State.RUNNING && newState != KafkaStreams.State.RUNNING) {
                    latch.countDown();
                }
            });

            streams.start();

            try {
                latch.await();
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            }

            System.err.println("Streams Closed");
        }
        public static Properties setProps(boolean cluster){
            Properties props = new Properties();
            if(cluster) {
                System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");


                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fleschm-final-pizzza");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                       "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);



                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("enable.auto.commit", "true");
                props.put("auto.commit.interval.ms", "1000");

            }
            else{
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fleschm-final-pizzza");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);

            }
            return props;
        }
        static Topology countOrderByName(String inputTopic, String outputTopic) {
            System.err.println("Count_Order_By_Name.java");
            System.err.println("-----Starting Processor-----");
            // Stream Logik
            final StreamsBuilder builder = new StreamsBuilder();

            final KStream<String, OrderPOJO> pizza = builder.stream(inputTopic,
                    Consumed.with(Serdes.String(), new JSONSerde<>()));

            pizza.peek((k, pv) -> System.err.println(pv.getCustomer()));

            pizza.groupBy((k, v) -> v.getCustomer()).count().toStream()
                    .to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));

            return builder.build();
        }
        static Topology aggregatePizzaByCustomer(String inputTopic, String outputTopic) {
        System.err.println("Count_Order_By_Name.java");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, OrderPOJO> orderStream =
                builder.stream(inputTopic, Consumed.with(Serdes.String(), new JSONSerde()));
                        //.peek((key, value) -> System.out.println("value " + value));
        orderStream
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeAndGrace(Duration.ofMinutes(1), Duration.ofSeconds(15)))
                .aggregate(() -> 0.0,
                        (key, order, total) -> total + order.getPizzas().size(),
                        Materialized.with(Serdes.String(), Serdes.Double()))
                // Don't emit results until the window closes HINT suppression
                .toStream()
                // When windowing Kafka Streams wraps the key in a Windowed class
                // After converting the table to a stream it's a good idea to extract the
                // Underlying key from the Windowed instance HINT: use map
                //.map((wk, value) -> KeyValue.pair(wk.key(),value))
                .peek((key, value) -> System.out.println("Outgoing record - key " +key +" value " + value));
                //.to(outputTopic, Produced.with(Serdes.String(), Serdes.Double()));


        return builder.build();
    }
        static Topology countCustomerInWindow(String inputTopic, String outputTopic){
        System.err.println("Count_Order_By_Name_in_Window.java");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, OrderPOJO> views = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        views.groupBy((k, v) -> v.getCustomer())
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofMinutes(5), Duration.ofSeconds(60)))
                .count().toStream()
                .map((wk, value) -> KeyValue.pair(wk.key(),value))
                .to(outputTopic, Produced.with(Serdes.String(),Serdes.Long()));
        return builder.build();
    }

    static Topology simpleReduce(String inputTopic, String outputTopic){
        System.err.println("Count_Order_By_Name_in_Window.java");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, OrderPOJO> orders = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), new JSONSerde<>()));

            Reducer<Long> reducer = (longValueOne, longValueTwo) -> longValueOne + longValueTwo;

            final KStream<String, Long> ordersByName =
                    orders.map((key, value) -> KeyValue.pair(value.getCustomer(), (long)value.getPizzas().size()));
                   ordersByName.peek((key, value) -> System.out.println("Incoming record - key " +key +" value " + value))
                    .groupByKey().reduce(reducer,
                                            Materialized.with(Serdes.String(), Serdes.Long()));
                                //.toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));


        return builder.build();
    }

    public static void main(String[] args) throws Exception {

        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(() -> MyProducer.produceOrder(customers, true), 1, 1, TimeUnit.SECONDS);
        Properties props = setProps(true);

        KafkaStreams kafkaStreams = new KafkaStreams(
                simpleReduce("fleschm-final-order", "fleschm-2"),
                props);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        System.err.println("Kafka Streams Started");
        runKafkaStreams(kafkaStreams);
        }
}