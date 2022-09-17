package de.hstr.bigdata;

import de.hstr.bigdata.Util.Json.JSONSerde;
import de.hstr.bigdata.Util.MyProducer;
import de.hstr.bigdata.Util.POJOGenerator;
import de.hstr.bigdata.Util.pojos.OrderPOJO;
import de.hstr.bigdata.Util.pojos.PizzaPOJO;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.unbounded;
import static org.apache.kafka.streams.kstream.Suppressed.untilWindowCloses;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Count_Order_By_Name {
    private static final int NUMBER_OF_CUSTOMERS = 2;

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


                props.put("security.protocol", "SASL_PLAINTEXT");
                props.put("enable.auto.commit", "true");
                props.put("auto.commit.interval.ms", "1000");

            }
            else{
                props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fleschm-final-pizzza");
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

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
    static Topology countOrderwithWindow(String inputTopic, String outputTopic) {
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
                        Materialized.with(Serdes.String(), Serdes.Double()>))
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

    public static void main(String[] args) throws Exception {

        List customers = POJOGenerator.generateCustomer(NUMBER_OF_CUSTOMERS);

        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(() -> MyProducer.produceOrder(customers, false), 1, 1, TimeUnit.SECONDS);
        Properties props = setProps(false);

        KafkaStreams kafkaStreams = new KafkaStreams(
                countOrderwithWindow("fleschm-final-order", "fleschm-2"),
                props);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        System.err.println("Kafka Streams 101 App Started");
        runKafkaStreams(kafkaStreams);
        }
}