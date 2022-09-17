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

    private static final String PIZZA_TOPIC = "fleschm-final-pizzas";
    private static final String ORDER_TOPIC = "fleschm-final-order";


    public static KafkaProducer clusterProducer(boolean cluster){
        Properties props = new Properties();
        if(cluster) {
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
        }

        else{
            props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
            props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        }
        KafkaProducer<String,String> my_producer =
                new KafkaProducer<String, String>(props);
        return my_producer;
    }
    public static void produceOrder(List customers, boolean cluster){
        KafkaProducer my_producer = clusterProducer(false);

        String value = null;

        //generate OrderString
        while(true){
        try {
            value = Json.stringify(Json.toJson(generateOrder(customers)));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        ProducerRecord<String, String> record =
                new ProducerRecord<String, String>(ORDER_TOPIC, value);
        //Sending data


        my_producer.send(record);
        my_producer.flush();
        }
    }

    public static void producePizza() {

            KafkaProducer my_producer = clusterProducer(false);
            String value = null;
            //generate PizzaString

        try {
            value = Json.stringify(Json.toJson(generatePizza()));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }

        ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>(PIZZA_TOPIC, value);
            //Sending data
            System.err.println(value);
            my_producer.send(record);
            my_producer.flush();
    }

    public static void main(String[] args) throws InterruptedException {
        List customers = generateCustomer(3);
        while(true){
            Thread.sleep(5000);
            produceOrder(customers, false);
            System.out.println("Sent Record");
        }

    }
}
