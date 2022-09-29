package de.hstr.bigdata.Util;

import com.fasterxml.jackson.core.JsonProcessingException;
import de.hstr.bigdata.Util.Json.Json;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static de.hstr.bigdata.Util.POJOGenerator.*;


public class MyProducer {
    private static KafkaProducer<String, String> producer;
    private int minId;
    private int maxId;
    private Random rnd;
    

    public MyProducer(int minId, int maxId){
        this.minId = minId;
        this.maxId = maxId;
        this.rnd = new Random();
          producer = new KafkaProducer<>(setProps(true));
    }

    public static Properties setProps(boolean cluster) {
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
        return props;
    }



    public void produceOrder(String inputTopic) {

        int customerId = minId + rnd.nextInt(maxId-minId);
        String customerName = "Kunde" + customerId;
        String value = null;
        //generate OrderString
        try {
            value = Json.stringify(Json.toJson(generateOrder(customerName)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
        ProducerRecord<String, String> record =
                new ProducerRecord<>(inputTopic, value);
        //Sending data

        producer.send(record);
        producer.flush();
        System.err.println(value);
    }

    public static void main(String[] args){
        MyProducer[] prod = new MyProducer[5];
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        for(int i = 0; i < 5; i++){
            prod[i] = new MyProducer(200000*i, 200000* (i+1));
            int finalI = i;
            exec.scheduleAtFixedRate(() -> prod[finalI].produceOrder(args[0]), 10, 10, TimeUnit.MILLISECONDS);
        }

    }


}
