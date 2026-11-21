package com.de.cdc;

import com.de.cdc.ChangeEventApplier.FailedMessage;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
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
 * Entry point of the subscriber. It repeats these 4 steps:
 * <pre>
 *   1. Read a batch of change events from the Kafka topic (for example "de_source_mongo").
 *   2. Apply them to the target MongoDB database (see ChangeEventApplier).
 *   3. Send the messages that could not be read to the DLQ topic.
 *   4. Tell Kafka that the batch is done (commit the offsets).
 * </pre>
 * Step 4 runs only after step 2 succeeded. If the app crashes, Kafka gives the same messages
 * again after the restart, so no change is lost. Applying a message twice is safe.
 *
 * Usage: java -jar mongo-cdc-subscriber-1.0.0-jar-with-dependencies.jar [path\to\subscriber.properties]
 */
public class SubscriberApp {

    public static void main(String[] args) throws IOException {
        Properties config = loadConfig(args);
        setUpLogging(config);
        Logger log = LoggerFactory.getLogger(SubscriberApp.class);

        String topic = config.getProperty("kafka.topic");
        String dlqTopic = config.getProperty("kafka.dlq.topic");
        String targetDbName = config.getProperty("target.database");
        boolean applyDrop = Boolean.parseBoolean(config.getProperty("apply.drop", "false"));

        // try-with-resources: MongoDB, the consumer and the producer are closed automatically at the end
        try (MongoClient mongo = MongoClients.create(config.getProperty("mongo.uri"));
             KafkaConsumer<String, String> consumer = createConsumer(config);
             KafkaProducer<String, String> dlqProducer = createDlqProducer(config)) {

            ChangeEventApplier applier = new ChangeEventApplier(mongo.getDatabase(targetDbName), applyDrop);
            stopOnCtrlC(consumer);

            consumer.subscribe(List.of(topic));
            log.info("Reading topic '{}' and writing to database '{}'", topic, targetDbName);

            while (true) {
                // 1. Read the next batch (waits up to 1 second for new messages)
                ConsumerRecords<String, String> messages = consumer.poll(Duration.ofSeconds(1));
                if (messages.isEmpty()) {
                    continue;
                }

                // 2. Apply the changes to MongoDB
                List<FailedMessage> failedMessages = applier.applyAll(messages);

                // 3. Send the messages that could not be read to the DLQ topic
                for (FailedMessage failed : failedMessages) {
                    ConsumerRecord<String, String> message = failed.message();
                    log.error("Could not read message (partition {}, offset {}), sending it to '{}'",
                            message.partition(), message.offset(), dlqTopic, failed.error());
                    sendToDlq(dlqProducer, dlqTopic, failed);
                }
                dlqProducer.flush();

                // 4. Mark the batch as done
                consumer.commitSync();
            }
        } catch (WakeupException e) {
            // poll() throws this after Ctrl+C (see stopOnCtrlC)
            log.info("Stopped");
        }
    }

    /** Reads subscriber.properties from the path given on the command line, or from inside the jar. */
    private static Properties loadConfig(String[] args) throws IOException {
        Properties config = new Properties();
        if (args.length > 0) {
            try (InputStream input = Files.newInputStream(Path.of(args[0]))) {
                config.load(input);
            }
        } else {
            try (InputStream input = SubscriberApp.class.getResourceAsStream("/subscriber.properties")) {
                config.load(input);
            }
        }
        return config;
    }

    /**
     * slf4j-simple reads its settings from system properties, so copy the "org.slf4j.*" lines
     * from the config there. This must happen before the first logger is created.
     */
    private static void setUpLogging(Properties config) {
        for (String key : config.stringPropertyNames()) {
            if (key.startsWith("org.slf4j.")) {
                System.setProperty(key, config.getProperty(key));
            }
        }
    }

    private static KafkaConsumer<String, String> createConsumer(Properties config) {
        Properties settings = new Properties();
        settings.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.bootstrap.servers"));
        // Kafka remembers how far this group has read, so a restart continues from there
        settings.put(ConsumerConfig.GROUP_ID_CONFIG, config.getProperty("kafka.group.id"));
        // key and value are plain JSON text
        settings.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        settings.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // we commit ourselves, only after the MongoDB writes succeeded
        settings.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        // a new group starts at the beginning of the topic
        settings.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        // at most 500 messages per batch
        settings.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500");
        return new KafkaConsumer<>(settings);
    }

    private static KafkaProducer<String, String> createDlqProducer(Properties config) {
        Properties settings = new Properties();
        settings.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getProperty("kafka.bootstrap.servers"));
        settings.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        settings.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // wait until Kafka has safely stored the DLQ message
        settings.put(ProducerConfig.ACKS_CONFIG, "all");
        return new KafkaProducer<>(settings);
    }

    /** Copies the bad message to the DLQ topic, with two headers: the error, and where the message came from. */
    private static void sendToDlq(KafkaProducer<String, String> producer, String dlqTopic, FailedMessage failed) {
        ConsumerRecord<String, String> original = failed.message();
        String source = original.topic() + "-" + original.partition() + "-" + original.offset();

        ProducerRecord<String, String> copy = new ProducerRecord<>(dlqTopic, original.key(), original.value());
        copy.headers().add("error", String.valueOf(failed.error()).getBytes(UTF_8));
        copy.headers().add("source", source.getBytes(UTF_8));
        producer.send(copy);
    }

    /**
     * Ctrl+C runs this hook. consumer.wakeup() makes the waiting poll() throw WakeupException,
     * so the main loop ends and everything is closed normally. The hook waits until that is done.
     */
    private static void stopOnCtrlC(KafkaConsumer<String, String> consumer) {
        Thread mainThread = Thread.currentThread();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            consumer.wakeup();
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }));
    }
}
