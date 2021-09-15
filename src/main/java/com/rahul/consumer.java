package com.rahul;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.function.Consumer;

public class consumer {
    public static void main(String[] args) {
        ConsumerListener c = new ConsumerListener();
        Thread thread = new Thread(c);
        thread.start();
    }
    public static <User> void consumer() {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "com.rahul.deserializer.UserDeserializer");
        properties.put("group.id", "test-group");

        KafkaConsumer<String, User> kafkaConsumer = new KafkaConsumer(properties);
        List topics = new ArrayList();
        topics.add("user");
        kafkaConsumer.subscribe(topics);
        try{
            // Message1
            while (true){
                ConsumerRecords<String, User> records = kafkaConsumer.poll(10000);
                for (ConsumerRecord<String, User> record: records){
                    System.out.println(record.value());

                    BufferedWriter buffer = new BufferedWriter(new FileWriter("Data.txt", true));
                    buffer.write(record.value()+"\n"); // this code will write the user records in Data.txt file.
                    buffer.close();
                }
            }
        }catch (Exception e){
            System.out.println(e.getMessage());
        }finally {
            kafkaConsumer.close();
        }
    }
}

class ConsumerListener implements Runnable {


    @Override
    public void run() {;
    }
}

