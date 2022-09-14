package de.hstr.bigdata;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

/**
 * In this example, we implement a simple LineSplit program using the high-level Streams DSL
 * that reads from a source topic "streams-plaintext-input", where the values of messages represent lines of text,
 * and writes the messages as-is into a sink topic "streams-pipe-output".
 */
public class Pipe {

    public static void main(String[] args) throws Exception {

        System.err.println("Pipe.java");
        
        System.setProperty("java.security.auth.login.config", "/home/schmi/kafka.jaas");

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "schmi-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        final StreamsBuilder builder = new StreamsBuilder();

        System.err.println("-----Starting Processor-----");
        
        builder.stream("schmi-1")
                .peek((k, v) -> { System.err.println("Peek: " + v);})
                .to("schmi-2");

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