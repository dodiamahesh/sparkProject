package com.de.cdc;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Reads MongoDB change events from one Kafka topic and applies them to the target database.
 * Offsets are committed only after the writes succeed (at-least-once; writes are idempotent).
 *
 * Usage: java -jar mongo-cdc-subscriber.jar [path\to\subscriber.properties]
 */
public class SubscriberApp {

    public static void main(String[] args) throws IOException {
        Properties cfg = loadConfig(args);
        // slf4j-simple reads its settings from system properties; set them before the first logger is created
        cfg.stringPropertyNames().stream()
                .filter(k -> k.startsWith("org.slf4j."))
                .forEach(k -> System.setProperty(k, cfg.getProperty(k)));
        Logger log = LoggerFactory.getLogger(SubscriberApp.class);

        String topic = cfg.getProperty("kafka.topic");
        String dlqTopic = cfg.getProperty("kafka.dlq.topic");

        try (MongoClient mongo = MongoClients.create(cfg.getProperty("mongo.uri"));
             KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(cfg));
             KafkaProducer<String, String> dlq = new KafkaProducer<>(producerProps(cfg))) {

            ChangeEventApplier applier = new ChangeEventApplier(mongo,
                    cfg.getProperty("target.database"),
                    Boolean.parseBoolean(cfg.getProperty("apply.drop", "false")));

            // Ctrl+C: break out of poll() and close everything cleanly
            Thread mainThread = Thread.currentThread();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                consumer.wakeup();
                try {
                    mainThread.join();
                } catch (InterruptedException ignored) {
                    Thread.currentThread().interrupt();
                }
            }));

            consumer.subscribe(List.of(topic));
            log.info("Subscribed to '{}', writing to database '{}'", topic, cfg.getProperty("target.database"));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    continue;
                }
                applier.apply(records, (rec, err) -> {
                    log.error("Bad message at partition {} offset {} -> {}", rec.partition(), rec.offset(), dlqTopic, err);
                    ProducerRecord<String, String> out = new ProducerRecord<>(dlqTopic, rec.key(), rec.value());
                    out.headers().add("error", String.valueOf(err).getBytes(UTF_8));
                    out.headers().add("source", (rec.topic() + "-" + rec.partition() + "-" + rec.offset()).getBytes(UTF_8));
                    dlq.send(out);
                });
                dlq.flush();
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            log.info("Stopped");
        }
    }

    private static Properties loadConfig(String[] args) throws IOException {
        Properties p = new Properties();
        try (InputStream in = args.length > 0
                ? Files.newInputStream(Path.of(args[0]))
                : SubscriberApp.class.getResourceAsStream("/subscriber.properties")) {
            p.load(in);
        }
        return p;
    }

    private static Properties consumerProps(Properties cfg) {
        Properties p = new Properties();
        p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getProperty("kafka.bootstrap.servers"));
        p.put(ConsumerConfig.GROUP_ID_CONFIG, cfg.getProperty("kafka.group.id"));
        p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        p.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");    // commit only after MongoDB writes succeed
        p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");  // first run: read the topic from the start
        p.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return p;
    }

    private static Properties producerProps(Properties cfg) {
        Properties p = new Properties();
        p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getProperty("kafka.bootstrap.servers"));
        p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.put(ProducerConfig.ACKS_CONFIG, "all");
        return p;
    }
}
