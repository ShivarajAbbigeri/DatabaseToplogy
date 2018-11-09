/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.databasetopology;

/**
 *
 * @author shivaraj2
 */
import java.util.Properties;
import java.util.Scanner;

import java.net.HttpURLConnection;

import java.net.URL;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;

//Create java class named “SimpleProducer”
public class AvProducer {
   
 Properties props = new Properties();
 String topicName= "str-sales-ord";
    
 AvProducer(){
      //Assign topicName to string variable
      
      
      // create instance for properties to access producer configs   
      
      
      //Assign localhost id
      props.put("bootstrap.servers", "192.168.56.102:9092,192.168.56.103:9092,192.168.56.105:9092");
      
      //Set acknowledgements for producer requests.      
      props.put("acks", "all");
      
      //If the request fails, the producer can automatically retry,
      props.put("retries", 0);
      
      //Specify buffer size in config
      props.put("batch.size", 16384);
      
      //Reduce the no of requests less than 0   
      props.put("linger.ms", 1);
      
      //The buffer.memory controls the total amount of memory available to the producer for buffering.   
      props.put("buffer.memory", 33554432);
      
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");  
      props.put("value.serializer", 
         "org.apache.kafka.common.serialization.StringSerializer");
    }
    
    void Produce(String value){
      Producer<String,String> producer = new KafkaProducer<String,String>(props);
       
        producer.send(new ProducerRecord<String,String>(topicName, value));
        System.out.println("Sent");

      producer.close();  
    
   }      
}
