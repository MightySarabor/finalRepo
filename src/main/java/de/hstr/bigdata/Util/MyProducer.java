package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.hstr.bigdata.Util.Json.Json;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.List;
import java.util.Properties;

import static de.hstr.bigdata.Util.POJOGenerator.*;


public class MyProducer {

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

    public static void produceOrder(String[] customers, boolean cluster, String inputTopic) {
        KafkaProducer my_producer = clusterProducer(cluster);

        String value = null;

        //generate OrderString

        try {
            value = Json.stringify(Json.toJson(generateOrder(customers)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(inputTopic, value);
        //Sending data

        System.err.println(value);
        my_producer.send(record);
        my_producer.flush();

    }


    public static void main(String[] args) throws InterruptedException {
        String[] customers = {"Peter Pan", "Hans Mueller", "Guenther Jauch"};
        while (true) {
            Thread.sleep(5000);
            produceOrder(customers, true, args[0]);
            System.out.println("Sent Record");
        }
    }
}
