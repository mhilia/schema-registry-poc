package fr.ippon.schemaregistry.avro.production;

import com.kafka.clients.production.Payment;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.math.BigInteger;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class ProduceMessages {


    // CONFIG PART
    private static final String TOPIC = "test.replicator.down";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String SCHEMA_REGISTRY_URL = "http://localhost:8081";
    private static final Class<StringSerializer> KEY_STRING_SERIALIZER_CLASS = StringSerializer.class;
    private static final Class<KafkaAvroSerializer> VALUE_AVRO_SERIALIZER = KafkaAvroSerializer.class;
    private static final int NUM_RECORD = 100;


    public static void main(String[] args) throws ExecutionException, InterruptedException {


        String EH_SASL = "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"test\" password=\"test123\";";

        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        //props.put("sasl.mechanism", "PLAIN");
        //props.put("security.protocol", "SASL_PLAINTEXT");
        //props.put("sasl.jaas.config", EH_SASL);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KEY_STRING_SERIALIZER_CLASS);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, VALUE_AVRO_SERIALIZER);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRY_URL);
        props.put("acks" , "all");

        String schemaAvro = "{\"namespace\": \"com.kafka.clients.production\",\n" +
                " \"type\": \"record\",\n" +
                " \"name\": \"Payment\",\n" +
                " \"fields\": [\n" +
                "     {\"name\": \"id\", \"type\": \"string\"},\n" +
                "     {\"name\": \"amount\", \"type\": \"double\"}\n" +
                " ]\n" +
        "}";

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


        Future<RecordMetadata> future;
        KafkaProducer<String, Payment> producer = new KafkaProducer<String, Payment>(props);


        long start = System.currentTimeMillis();



        for(int i=0; i< NUM_RECORD ; i++){

            String orderId = "replicator-down-bis" + i;
            System.out.println(orderId);
            final Payment payment = Payment.newBuilder()
                    .setId(orderId)
                    .setAmount(System.currentTimeMillis() % 10000000)
                    .build();

            final ProducerRecord<String, Payment> record = new ProducerRecord<String, Payment>(TOPIC, payment.getId().toString(), payment);
            future = producer.send(record);
            System.out.printf("Message : TOPIC : %s, OFFSET : %s, PARTITION : %s%n", future.get().topic(),future.get().offset(),future.get().partition());
            producer.flush();
        }
        long end = System.currentTimeMillis();

        System.out.println("End in : " + ((end - start)/1000) + " seconds") ;

    }
}
