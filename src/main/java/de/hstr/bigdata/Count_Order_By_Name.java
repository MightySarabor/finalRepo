package de.hstr.bigdata;

import de.hstr.bigdata.Util.Json.JSONSerde;
import de.hstr.bigdata.Util.MyProducer;
import de.hstr.bigdata.Util.POJOGenerator;
import de.hstr.bigdata.Util.pojos.OrderPOJO;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;

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
        public static Properties setProps(String[] args){


            System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

            Properties props = new Properties();
            //props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fleschm-final-pizzza");
            //props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
            //       "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());


            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
            if (args.length < 1) {
                throw new IllegalArgumentException("This program takes one argument: the path to a configuration file.");
            }
            try (InputStream inputStream = new FileInputStream(args[0])) {
                props.load(inputStream);
            } catch (IOException e) {
                throw new RuntimeException(e);
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
        System.err.println("-----Starting Processor-----");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, OrderPOJO> views = builder.stream("streams-pageview-input",
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        views.groupBy((k, v) -> v.getCustomer())
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(1)))
                .count().toStream()
                .peek((k, v) -> { System.out.println(k.key() + " " + k.window().startTime() + " " + k.window().endTime() + " " + v); });

        return builder.build();
    }

    public static void main(String[] args) throws Exception {

        List customers = POJOGenerator.generateCustomer(NUMBER_OF_CUSTOMERS);

        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(() -> MyProducer.produceOrder(customers), 1, 5, TimeUnit.SECONDS);
        Properties props = setProps(args);

        KafkaStreams kafkaStreams = new KafkaStreams(
                countOrderwithWindow("fleschm-final-order", "fleschm-2"),
                props);

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        System.err.println("Kafka Streams 101 App Started");
        runKafkaStreams(kafkaStreams);
        }
}