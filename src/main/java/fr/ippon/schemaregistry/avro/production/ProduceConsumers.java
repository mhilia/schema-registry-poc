package fr.ippon.schemaregistry.avro.production;


import fr.ippon.schemaregistry.avro.Civility;
import fr.ippon.schemaregistry.avro.Consumer;
import fr.ippon.schemaregistry.avro.consuming.ConsumeMessages;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class ProduceConsumers {


    // CONFIG PART
    private static final String TOPIC = "topic.consumers.eu";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final Class<StringSerializer> KEY_STRING_SERIALIZER_CLASS = StringSerializer.class;
    private static final Class<KafkaAvroSerializer> VALUE_AVRO_SERIALIZER = KafkaAvroSerializer.class;
    private static final int NUM_RECORD = 10000;

    private static final Logger log = LoggerFactory.getLogger(ProduceConsumers.class);



    public static void main(String[] args) throws ExecutionException, InterruptedException {



        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_STRING_SERIALIZER_CLASS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_AVRO_SERIALIZER);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put("acks" , "all");


        String consumerAvroSchema = "{\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"Consumer\",\n" +
                " \"namespace\": \"org.ippon.schemaregistry.avro\",\n" +
                " \"fields\": [\n" +
                "   { \"name\": \"consumer_id\" , \"type\": \"string\"},\n" +
                "   { \"name\": \"last_name\"   ,\"type\": \"int\"},\n" +
                "   { \"name\": \"first_name\", \"type\": \"string\" },\n" +
                "   { \"name\": \"city\", \"type\": \"string\"},\n" +
                "   { \n" +
                "\"name\": \"civility\", \n" +
                "\"type\": {\"type\": \"enum\",\"name\": \"Civility\",\"symbols\": [\"MISS\", \"MR\", \"MS\"]}\n" +
                "   },\n" +
                "   {\"name\": \"country\",\"type\": \"int\" },\n" +
                "   {\"name\": \"phoneNumber\", \"type\": [ \"string\",\"null\" ]}\n" +
                " ]\n" +
                "}\n";

        long start = System.currentTimeMillis();

        KafkaProducer<String, Consumer> producer = new KafkaProducer<>(props);
        Random r  = new Random();
        for(int i=0; i< NUM_RECORD ; i++){

            String consumerId = "eu#" + r.nextInt(5) + ( System.currentTimeMillis() - Long.MAX_VALUE);
            System.out.println(consumerId);

            final Consumer consumer = Consumer.newBuilder()
                    .setConsumerId(consumerId)
                    .setCivility(Civility.MISS)
                    .setLastName("Z")
                    .setFirstName("X")
                    .setCountry("FR")
                    .setCity("Paris")
                    .setEmail("*****@ippon.tech")
                    .setPhoneNumber("+336xxxxxxxx")
                    .build();

            final ProducerRecord<String, Consumer> record = new ProducerRecord<>(TOPIC, consumer.getConsumerId().toString(), consumer);
            Future<RecordMetadata> future = producer.send(record);
            log.info("Message : TOPIC : %s, OFFSET : %s, PARTITION : %s%n", future.get().topic(), future.get().offset(), future.get().partition());
            producer.flush();
        }

       long end = System.currentTimeMillis();
       log.info("End in : " + ((end - start)/1000) + " seconds") ;

    }
}
