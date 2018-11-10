package com.mycompany.databasetopology;

/**
 *
 * @author shivaraj2
 */
import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AvProducer {

    Properties props = new Properties();
    String topicName = "str-sales-ord";

    AvProducer() {

        props.put("bootstrap.servers", "192.168.56.102:9092,192.168.56.103:9092,192.168.56.105:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
    }

    void Produce(String value) {
        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        producer.send(new ProducerRecord<String, String>(topicName, value));
        System.out.println("Sent");

        producer.close();

    }
}
