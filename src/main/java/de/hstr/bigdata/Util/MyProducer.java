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

    public MyProducer(int minId, int maxId){
        this.minId = minId;
        this.maxId = maxId;
        this.rnd = new Random();
    }

    public void produceOrder() {

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
                new ProducerRecord<>("fleschm-1", value);
        //Sending data

        producer.send(record);
        producer.flush();
        System.err.println(value);
    }

    public static void main(String[] args){
        ScheduledExecutorService exec = Executors.newScheduledThreadPool(10);
        MyProducer prod1 = new MyProducer(0,20000);
        MyProducer prod2 = new MyProducer(20000,40000);
        MyProducer prod3 = new MyProducer(40000,60000);
        MyProducer prod4 = new MyProducer(60000,80000);
        MyProducer prod5 = new MyProducer(80000,100000);
       // for(int i = 0; i <= 1000; i++) {
            exec.scheduleAtFixedRate(prod1::produceOrder, 1000, 1000, TimeUnit.MILLISECONDS);
           // exec.scheduleAtFixedRate(() -> prod2.produceOrder(args[0]), 2000, 1000, TimeUnit.MILLISECONDS);
            //exec.scheduleAtFixedRate(() -> prod3.produceOrder(args[0]), 3000, 1000, TimeUnit.MILLISECONDS);
            //exec.scheduleAtFixedRate(() -> prod4.produceOrder(args[0]), 4000, 1000, TimeUnit.MILLISECONDS);
            //exec.scheduleAtFixedRate(() -> prod5.produceOrder(args[0]), 5000, 1000, TimeUnit.MILLISECONDS);
       // }
    }


}
