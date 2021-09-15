package prod.rahul;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.message.DescribeUserScramCredentialsRequestData;
import org.apache.kafka.clients.producer.KafkaProducer;


import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.Properties;
import java.util.Random;

import static org.graalvm.compiler.options.OptionType.User;

public class producer {

    public static <User> void main(String[] args) {

        //create properties object for producer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");


        Random random = new Random();
        int minAge = 18;
        int maxAge = 28;
        int age;

        Producer<String, String> producer = new KafkaProducer<String, String>(properties);
        try {
            for (int i = 0; i < 6; i++) {
                age = random.nextInt((maxAge - minAge) + 1) + minAge;
                User user = new User(i, "Rahul kumar", age, "BTech");
                System.out.println(user);
                ProducerRecord<String, String> record = new ProducerRecord<String, String>("User", user);
                producer.send(record);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {

            producer.close();

            System.out.println("SimpleProducer complete.");
        }
    }
}