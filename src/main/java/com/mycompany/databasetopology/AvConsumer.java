package com.mycompany.databasetopology;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import org.apache.avro.generic.GenericRecord;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import java.util.Arrays;
import java.util.Properties;

/**
 *
 * @author shivaraj
 */
public class AvConsumer {

    public static void main(String args[]) {
        Properties props = new Properties();

        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.56.102:9092,192.168.56.103:9092,192.168.56.105:9092");

        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group");

        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());
        props.put("schema.registry.url", "http://192.168.56.105:8081");

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        Gson gson = new Gson();

        final KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<String, GenericRecord>(props);
        consumer.subscribe(Arrays.asList("sales-ord"));
        AvProducer ap = new AvProducer();
        try {
            while (true) {
                ConsumerRecords<String, GenericRecord> records = consumer.poll(100);
                for (ConsumerRecord<String, GenericRecord> record : records) {
                    System.out.printf("topic = %s offset = %d, key = %s, value = %s \n", record.topic(), record.offset(), record.key(), record.value());

                    String JsonString = record.value().toString();
                    Sale saleObject = gson.fromJson(JsonString, Sale.class);
                    String salesValue = saleObject.id + " " + saleObject.supid + " " + saleObject.custid + " " + saleObject.amt + " " + saleObject.sup_name;
                    ap.Produce(salesValue);
                }
            }
        } finally {
            consumer.close();
        }
    }
}
