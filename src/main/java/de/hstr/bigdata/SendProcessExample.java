package de.hstr.bigdata;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

/**
 * Alle Ausgaben nach System.err, weil das direkt geflusht wird. System.out kann
 * in einer Applikation mit mehreren Threads manchmal zur Verwirrung fÃ¼hren,
 * weil sich Ausgaben gegenseitig Ã¼berholen.
 *
 * export KAFKA_OPTS="-Djava.security.auth.login.config=$PWD/kafka.jaas"
 *
 * /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh \ --broker-list
 * infbdt09:6667,infbdt10:6667,infbdt11:6667 \ --topic schmi-1 \
 * --producer-property security.protocol=SASL_PLAINTEXT
 *
 * /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh \
 * --bootstrap-server infbdt06:6667,infbdt07:6667,infbdt08:6667 \ --topic
 * schmi-2 \ --consumer-property security.protocol=SASL_PLAINTEXT
 */
public class SendProcessExample {
    private static final Random RND = new Random();

    private static KafkaStreams streams;
    private static KafkaProducer<String, String> producer;

    /**
     * Das ist unser Producer, der von dem ExecutorService unten in main()
     * regelmÃ¤ÃŸig in einem eigenen Thread aufgerufen wird.
     */
    public static void sendSomething() {
        String key = "some key";
        String value = "some value " + RND.nextInt();
        System.err.printf("Sent (%s, %s)\n", key, value);
        ProducerRecord<String, String> record = new ProducerRecord<>("fleschm-1", key, value);
        producer.send(record);
        // Direkt noch ein Flush hinterher, damit wir nicht auf Ausgabe warten
        // mÃ¼ssen.
        producer.flush();
    }

    public static void main(String[] args) {

        // Achtung, Datei kafka.jaas muss angepasst werden!
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

        StreamsBuilder builder = new StreamsBuilder();

        // Unsere eigentliche Verarbeitung: lies die Daten aus Topic 1, mache irgendwas mit
        // dem Key, und schreibe die Daten nach Topic 2.
        KStream<String, String> lines = builder.stream("fleschm-1");

        lines.map((k, v) -> new KeyValue<String, String>(k, "Transformed in Kafka Streams: " + v))
                .peek((k, v) -> { System.err.printf("(%s, %s)\n", k, v); })
                .to("fleschm-2");

        // Zum Nachvollziehen: Gib die Daten aus Topic 2 auf die Konsole aus.
        builder.stream("fleschm-2").peek((k, v) -> {
            System.err.printf("\t --> (%s, %s)\n", k, v);
        });

        // KonfigurationsgedÃ¶ns hier der Einfachheit halber fÃ¼r Streams und KafkaProducer gemeinsam.
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "filter-warnings");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put("security.protocol", "SASL_PLAINTEXT");
        config.put("enable.auto.commit", "true");
        config.put("auto.commit.interval.ms", "1000");

        System.err.println("Setting up producer thread");
        producer = new KafkaProducer<String, String>(config);
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(SendProcessExample::sendSomething, 1, 1, TimeUnit.SECONDS);

        System.err.println("Building topology.");
        Topology topology = builder.build();

        streams = new KafkaStreams(topology, config);

        System.err.println("Starting Kafka Streams.");
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(producer::close));
        Runtime.getRuntime().addShutdownHook(new Thread(StreamExample::shutdown));
    }

    public static void shutdown() {
        if (streams != null) {
            System.err.println("Shutting down Kafka Streams.");
            streams.close();
        }
    }
}
