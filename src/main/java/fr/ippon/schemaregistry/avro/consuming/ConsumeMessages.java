package fr.ippon.schemaregistry.avro.consuming;


import fr.ippon.schemaregistry.avro.Consumer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumeMessages {


    private static final String TOPIC = "topic.consumers.eu";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";

    private static final Logger log = LoggerFactory.getLogger(ConsumeMessages.class);

    public static void main(String[] args) {


        Properties props = new Properties();

        props.setProperty("group.id", "eu.group.1");
        props.setProperty("enable.auto.commit", "true");
        props.setProperty("auto.offset.reset", "earliest");
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);

        KafkaConsumer<String, Consumer> kafkaConsumer = new KafkaConsumer<String, Consumer>(props);
        kafkaConsumer.subscribe(Arrays.asList(TOPIC));


        while (true) {
            ConsumerRecords<String, Consumer> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            log.info("Number of Records : " + records.count());

            if (records.count() > 0)
                for (ConsumerRecord<String, Consumer> record : records) {

                    log.info("Offset = %d, Key = %s, Value = %s %n", record.offset(), record.key(), record.value());

                }
        }


    }
}
