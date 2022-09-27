package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.hstr.bigdata.Util.Json.Json;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static de.hstr.bigdata.Util.POJOGenerator.*;


public class MyProducer {

    private static KafkaProducer<String, String> producer;

    public static KafkaProducer clusterProducer(boolean cluster) {
        Properties props = new Properties();
        if (cluster) {
            System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

            //properties
            String bootstrapServers =
                    "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667";
            props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


            props.put("security.protocol", "SASL_PLAINTEXT");
            props.put("enable.auto.commit", "true");
            props.put("auto.commit.interval.ms", "1000");
        } else {
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
        KafkaProducer<String, String> my_producer =
                new KafkaProducer<String, String>(props);
        return my_producer;
    }

    public static void produceOrder(String inputTopic, String[] names) {
        String value = null;
        //generate OrderString
        try {
            value = Json.stringify(Json.toJson(generateOrder(names)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(inputTopic, value);
        //Sending data
        System.err.println(value);
        producer.send(record);
        producer.flush();

    }


    public static void main(String[] args) throws InterruptedException {
        String[] names = generateCustomer(2);
        System.err.println("---Starte Producer---");
        producer = clusterProducer(true);
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        exec.scheduleAtFixedRate(() -> produceOrder(args[0], names),
                1000,
                1000,
                TimeUnit.MILLISECONDS);
    }
}
