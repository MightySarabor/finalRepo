package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.hstr.bigdata.Util.Json.Json;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static de.hstr.bigdata.Util.POJOGenerator.generateOrder;
import static de.hstr.bigdata.Util.POJOGenerator.generatePizza;


public class MyProducer {
    public static void produce() {
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

        //properties
        String bootstrapServers =
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(props);
        final String inputTopic = "fleschm-final-pizza";
        //new NewTopic(inputTopic, 2, (short) 2);


            String value = null;
            try {
                value = Json.stringify(Json.toJson(generatePizza()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(inputTopic, value);
            //Sending data

            System.err.println(value);
            first_producer.send(record);
            System.err.println("Message sent");
            first_producer.flush();
    }
}
