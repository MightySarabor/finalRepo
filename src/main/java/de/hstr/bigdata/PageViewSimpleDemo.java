package de.hstr.bigdata;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serdes.StringSerde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.SlidingWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.processor.TimestampExtractor;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class PageViewSimpleDemo {
    public static class JsonTimestampExtractor implements TimestampExtractor {

        @Override
        public long extract(final ConsumerRecord<Object, Object> record, final long partitionTime) {
            if (record.value() instanceof PageViewSimpleDemo.PageView) {
                return ((PageViewSimpleDemo.PageView) record.value()).timestamp;
            }

            if (record.value() instanceof PageViewSimpleDemo.UserProfile) {
                return ((PageViewSimpleDemo.UserProfile) record.value()).timestamp;
            }

            if (record.value() instanceof JsonNode) {
                return ((JsonNode) record.value()).get("timestamp").longValue();
            }

            throw new IllegalArgumentException(
                    "JsonTimestampExtractor cannot recognize the record value " + record.value());
        }
    }

    /**
     * A serde for any class that implements {@link JSONSerdeCompatible}. Note that
     * the classes also need to be registered in the {@code @JsonSubTypes}
     * annotation on {@link JSONSerdeCompatible}.
     *
     * @param <T> The concrete type of the class that gets de/serialized
     */
    public static class JSONSerde<T extends JSONSerdeCompatible> implements Serializer<T>, Deserializer<T>, Serde<T> {
        private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

        @Override
        public void configure(final Map<String, ?> configs, final boolean isKey) {
        }

        @SuppressWarnings("unchecked")
        @Override
        public T deserialize(final String topic, final byte[] data) {
            if (data == null) {
                return null;
            }

            try {
                return (T) OBJECT_MAPPER.readValue(data, JSONSerdeCompatible.class);
            } catch (final IOException e) {
                throw new SerializationException(e);
            }
        }

        @Override
        public byte[] serialize(final String topic, final T data) {
            if (data == null) {
                return null;
            }

            try {
                return OBJECT_MAPPER.writeValueAsBytes(data);
            } catch (final Exception e) {
                throw new SerializationException("Error serializing JSON message", e);
            }
        }

        @Override
        public void close() {
        }

        @Override
        public Serializer<T> serializer() {
            return this;
        }

        @Override
        public Deserializer<T> deserializer() {
            return this;
        }
    }

    /**
     * An interface for registering types that can be de/serialized with
     * {@link JSONSerde}.
     */
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "_t")
    @JsonSubTypes({ @JsonSubTypes.Type(value = PageView.class, name = "pv"),
            @JsonSubTypes.Type(value = UserProfile.class, name = "up"),
            @JsonSubTypes.Type(value = PageViewByRegion.class, name = "pvbr"),
            @JsonSubTypes.Type(value = WindowedPageViewByRegion.class, name = "wpvbr"),
            @JsonSubTypes.Type(value = RegionCount.class, name = "rc") })
    public interface JSONSerdeCompatible {

    }

    // POJO classes
    static public class PageView implements JSONSerdeCompatible {
        public String user;
        public String page;
        public Long timestamp;
        @Override
        public String toString() {
            return "PageView [user=" + user + ", page=" + page + ", timestamp=" + timestamp + "]";
        }
    }

    static public class UserProfile implements JSONSerdeCompatible {
        public String region;
        public Long timestamp;
    }

    static public class PageViewByRegion implements JSONSerdeCompatible {
        public String user;
        public String page;
        public String region;
    }

    static public class WindowedPageViewByRegion implements JSONSerdeCompatible {
        public long windowStart;
        public String region;
    }

    static public class RegionCount implements JSONSerdeCompatible {
        public long count;
        public String region;
    }

    @SuppressWarnings("resource")
    public static void main1(String[] args) {
        PageView pv = new PageView();
        pv.user = "hans";
        pv.page = "news";
        pv.timestamp = 12908734094L;

        JSONSerde<JSONSerdeCompatible> serde = new JSONSerde<>();
        byte[] serialize = serde.serialize("foo", pv);
        JSONSerdeCompatible deserialize = serde.deserialize("foo", serialize);
        System.out.println(deserialize);
    }

    public static void main(final String[] args) {
        final Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pageview-typed");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, JsonTimestampExtractor.class);
        // props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, JSONSerde.class);
        // props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, JSONSerde.class);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, StringSerde.class);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000L);

        // setting offset reset to earliest so that we can re-run the demo code with the
        // same pre-loaded data
        // props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        final StreamsBuilder builder = new StreamsBuilder();

        final KStream<String, PageView> views = builder.stream("fleschm-pv",
                Consumed.with(Serdes.String(), new JSONSerde<>()));

        views.groupBy((k, v) -> v.user)
                .windowedBy(SlidingWindows.ofTimeDifferenceAndGrace(Duration.ofSeconds(10), Duration.ofSeconds(1)))
                .count().toStream()
                .peek((k, v) -> { System.out.println(k.key() + " " + k.window().startTime() + " " + k.window().endTime() + " " + v); })
                .map((k, v) -> new KeyValue<>(k.key(), v))
                .to("fleschm-2", Produced.with(Serdes.String(), Serdes.Long()));

        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-pipe-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (final Throwable e) {
            e.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
}