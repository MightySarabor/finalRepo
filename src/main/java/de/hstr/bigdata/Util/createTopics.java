package de.hstr.bigdata.Util;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TimeoutException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class createTopics {

    public void createTopics(final Properties allProps, List<NewTopic> topics)
            throws InterruptedException, ExecutionException, TimeoutException, java.util.concurrent.TimeoutException {
        try (final AdminClient client = AdminClient.create(allProps)) {
            System.err.println("Creating topics");

            client.createTopics(topics).values().forEach((topic, future) -> {
                try {
                    future.get();
                } catch (Exception ex) {
                    System.err.println(ex.toString());
                }
            });

            Collection<String> topicNames = topics
                    .stream()
                    .map(t -> t.name())
                    .collect(Collectors.toCollection(LinkedList::new));

            System.err.println("Asking cluster for topic descriptions");
            client
                    .describeTopics(topicNames)
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS)
                    .forEach((name, description) -> System.err.println("Topic Description: {}" + description.toString()));
        }
    }


}