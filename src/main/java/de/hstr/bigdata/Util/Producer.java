package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import myapps.Util.Json.Json;
import myapps.pojos.OrderPOJO;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static myapps.Util.POJOGenerator.generateOrder;
import static myapps.Util.POJOGenerator.generatePizza;

public class Producer {
    public static void main(String[] args) throws JsonProcessingException, ExecutionException, InterruptedException {
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



        /* while(true) {*/
            String topic = "fleschm-seven";
            String value = Json.stringify(Json.toJson(generatePizza()));
            ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(topic, value);
            System.out.println(value);
            //Sending data
            Thread.sleep(10000);
            first_producer.send(record);
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
}
