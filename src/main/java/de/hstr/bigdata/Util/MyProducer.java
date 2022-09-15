package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.hstr.bigdata.Util.Json.Json;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Arrays;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static de.hstr.bigdata.Util.POJOGenerator.generateOrder;
import static de.hstr.bigdata.Util.POJOGenerator.generatePizza;


public class MyProducer {
    public static void produce() throws ExecutionException, InterruptedException, TimeoutException {
        System.setProperty("java.security.auth.login.config", "/home/fleschm/kafka.jaas");

        //properties
        String bootstrapServers =
                "infbdt07.fh-trier.de:6667,infbdt08.fh-trier.de:6667,infbdt09.fh-trier.de:6667";
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "100");

        KafkaProducer<String,String> first_producer = new KafkaProducer<String, String>(props);



        //while(true) {
            final String inputTopic = props.getProperty("fleschm-final-pizza");
            createTopics creator = new createTopics();
            creator.createTopics(
                    props,
                    Arrays.asList(
                            new NewTopic(inputTopic, 2, (short) 2)));



            String value = null;
            try {
                value = Json.stringify(Json.toJson(generatePizza()));
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(inputTopic, value);
            //Sending data

            //System.err.println(value);
            first_producer.send(record);
            //first_producer.flush();
       // }
            /*first_producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                    if (e == null) {
                        System.out.println("Successfully recieved the details as: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "TimeStamp:" + recordMetadata.timestamp());
                    } else {
                        System.err.println("Can't produce, getting Error: " + e);
                    }
                }
            }).get();
        } */
    }
    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {
        while(true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            produce();
        }
    }
}
