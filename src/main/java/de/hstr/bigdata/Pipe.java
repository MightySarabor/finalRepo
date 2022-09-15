package de.hstr.bigdata;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import de.hstr.bigdata.Util.MyProducer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;

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
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fleschm-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");

        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(MyProducer::produce, 1, 1, TimeUnit.SECONDS);



        System.err.println("-----Starting Processor-----");
        // Stream Logik
        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PageViewSimpleDemo.PageView> views = builder.stream("fleschm-pizza",
                Consumed.with(Serdes.String(), new PageViewSimpleDemo.JSONSerde<>()));

        views.groupBy((k, v) -> v.user)
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(1)))
                .count().toStream()
                .peek((k, v) -> { System.out.println(k.key() + " " + k.window().startTime() + " " + k.window().endTime() + " " + v); })
                .map((k, v) -> new KeyValue<>(k.key(), v))
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