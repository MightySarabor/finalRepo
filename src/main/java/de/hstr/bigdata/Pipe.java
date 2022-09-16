package de.hstr.bigdata;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import de.hstr.bigdata.Util.Json.JSONSerde;
import de.hstr.bigdata.Util.MyProducer;
import de.hstr.bigdata.Util.pojos.PizzaPOJO;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

    public static void main(String[] args) throws Exception {

        System.err.println("Pipe.java");
        
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
        }
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(MyProducer::produce, 1, 1, TimeUnit.SECONDS);

        System.err.println("-----Starting Processor-----");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PizzaPOJO> views = builder.stream("fleschm-final-pizza",
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        views.peek((k, pv) -> System.err.println(pv));

        views.groupBy((k, v) -> v.getName()).count().toStream()
                .to("fleschm-2", Produced.with(Serdes.String(), Serdes.Long()));



        //Topology
        final Topology topology = builder.build();
        final KafkaStreams streams = new KafkaStreams(topology, props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}