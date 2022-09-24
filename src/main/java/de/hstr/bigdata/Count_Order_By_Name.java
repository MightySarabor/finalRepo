package de.hstr.bigdata;

import de.hstr.bigdata.Util.Json.JSONSerde;
import de.hstr.bigdata.Util.MyProducer;
import de.hstr.bigdata.Util.POJOGenerator;
import de.hstr.bigdata.Util.pojos.OrderPOJO;
import de.hstr.bigdata.Util.pojos.PizzaPOJO;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static de.hstr.bigdata.Util.POJOGenerator.generateCustomer;


/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Count_Order_By_Name {

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
        public static Properties setProps(boolean cluster, String StreamID){
            Properties props = new Properties();
            if(cluster) {
                System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");
                //System.setProperty("com.sun.management.jmxremote.port", "1616");

                props.put(StreamsConfig.APPLICATION_ID_CONFIG, StreamID);
                props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                       "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
                props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
                props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 3);
                // cache deaktivieren, damit alle Ergebnisse angezeigt werden.
                props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);


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
            System.err.println("Count_Order_By_Name");
            System.err.println("-----Starting Processor-----");
            // Stream Logik
            final StreamsBuilder builder = new StreamsBuilder();

            final KStream<String, OrderPOJO> pizza = builder.stream(inputTopic,
                    Consumed.with(Serdes.String(), new JSONSerde<>()));
                    //pizza.peek((k, pv) -> System.err.println(pv.getCustomer()));
                    pizza.groupBy((k, v) -> v.getCustomer()).count().toStream()
                    .map((k, v) -> KeyValue.pair("Count", 1))
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer()))
                    .count()
                    .toStream()
                    .peek((k, v) -> System.err.println("ERGEBNIS " + k + " " + v))
                    .filter((key, value) -> (value % 3 == 0))
                    .peek((key, value) -> System.err.println(value));
                    //.to(outputTopic, Produced.with(Serdes.String(), Serdes.Long()));
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
        static Topology localStateStoreQueryExample(String inputTopic, String outputTopic) {
            System.err.println("LocalStateStoreQuery");
            System.err.println("-----Starting Processor-----");
            // Stream Logik
            final StreamsBuilder builder = new StreamsBuilder();
            KStream<String, String> order = builder.stream(inputTopic);

        // Define the processing topology (here: WordCount)
            KGroupedStream<String, String> groupedByWord = order
                    .flatMapValues(value -> Arrays.asList(value.toLowerCase().split("\\W+")))
                    .groupBy((key, word) -> word, Grouped.with(Serdes.String(), Serdes.String()));

            // Create a key-value store named "CountsKeyValueStore" for the all-time word counts
           // groupedByWord.count());





            return builder.build();
        }

    static Topology simpleReduce(String inputTopic, String outputTopic){
        System.err.println("reduce");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, OrderPOJO> orders = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), new JSONSerde<>()));

            Reducer<Integer> reducer = (ValueOne, ValueTwo) -> ValueOne + ValueTwo;


                    orders.map((key, value) -> KeyValue.pair(value.getCustomer(), value.getPizzas().size()))
                   .peek((key, value) -> System.err.println("Incoming record - key " + key + " value " + value))
                    .groupByKey(Grouped.with(Serdes.String(), Serdes.Integer())).reduce(reducer,
                                            Materialized.with(Serdes.String(), Serdes.Integer()))
                                .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Integer()));

        return builder.build();
    }

    static Topology simpleAggregate(String inputTopic, String outputTopic){
        System.err.println("aggregate");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String, OrderPOJO> orders = builder.stream(inputTopic,
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        Aggregator<String, OrderPOJO, Float> priceCountAgg=
                (key, value, total) -> value.getPizzas().get(0).getPrice() + total;

        orders.map((key, value) -> KeyValue.pair("Summe Preis der ersten Pizza von Kunde: " + value.getCustomer(), value))
                .peek((key, value) -> System.err.println("Incoming record - key " + key + " value " + value))
                .groupByKey(Grouped.with(Serdes.String(), new JSONSerde<>()))
                .aggregate(() -> 0f,
                        priceCountAgg,
                        Materialized.with(Serdes.String(), Serdes.Float()))
                .toStream().to(outputTopic, Produced.with(Serdes.String(), Serdes.Float()));

        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 4) {
            throw new IllegalArgumentException("Arguemente eingeben in der Form: StreamsID inputTopic outputTopic num_of_customers Methode");
        }
        //System.err.println("Erstelle Liste");
        //String[] num_of_customers = generateCustomer(Integer.parseInt(args[3]));
        //System.err.println("Liste erstellt");
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(() -> MyProducer.produceOrder(true, args[1]),
                1, 5, TimeUnit.SECONDS);

        Properties props = setProps(true, args[0]);
        KafkaStreams kafkaStreams = null;
        switch(args[3]) {
            case "reduce":
                kafkaStreams = new KafkaStreams(
                        simpleReduce(args[1], args[2]),
                        props);
                break;
            case "aggregate":
                kafkaStreams = new KafkaStreams(
                        simpleAggregate(args[1], args[2]),
                        props);
                break;
            case "countByCustomer":
                kafkaStreams = new KafkaStreams(
                        countOrderByName(args[1], args[2]),
                        props);
                break;
            case "countWithWindow":
                kafkaStreams = new KafkaStreams(
                        countCustomerInWindow(args[1], args[2]),
                        props);
                break;
            case "help":
                System.err.println("Methoden: reduce, aggregate, countByCustomer oder countWithWindow");
                break;
            default:
                System.err.println("default");
        }


        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        System.err.println("Kafka Streams Started");
        runKafkaStreams(kafkaStreams);
        }
}