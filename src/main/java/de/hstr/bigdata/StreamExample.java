package de.hstr.bigdata;

import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 export KAFKA_OPTS="-Djava.security.auth.login.config=$PWD/kafka.jaas"

 /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh \
 --broker-list infbdt09:6667,infbdt10:6667,infbdt11:6667 \
 --topic schmi-1 \
 --producer-property security.protocol=SASL_PLAINTEXT

 /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh \
 --bootstrap-server infbdt06:6667,infbdt07:6667,infbdt08:6667 \
 --topic schmi-2 \
 --consumer-property security.protocol=SASL_PLAINTEXT
 */
public class StreamExample {

    private static final Logger LOG = LoggerFactory.getLogger(StreamExample.class);

    private static KafkaStreams streams;

    public static void main(String[] args) {
        // Achtung, Datei kafka.jaas muss angepasst werden!
        System.setProperty("java.security.auth.login.config", "/home/schmi/kafka.jaas");

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> lines = builder.stream("schmi-1");
        KStream<String, String> transformed = lines
                .map((k, v) -> new KeyValue<String, String>(k, "Transformed in Kafka Streams: " + v));
        transformed.to("schmi-2");

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-warnings");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put("enable.auto.commit", "true");
        config.put("auto.commit.interval.ms", "1000");

        LOG.info("Building topology.");
        Topology topology = builder.build();

        streams = new KafkaStreams(topology, config);

        LOG.info("Starting Kafka Streams.");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(StreamExample::shutdown));
    }

    public static void shutdown() {
        if (streams != null) {
            LOG.info("Shutting down Kafka Streams.");
            streams.close();
        }
    }
}