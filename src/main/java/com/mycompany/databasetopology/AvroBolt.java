/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.databasetopology;
import java.io.File;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.utils.Utils;
import kafka.message.Message;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.storm.kafka.KafkaConfig;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.storm.tuple.Values;



public class AvroBolt extends BaseRichBolt {

        OutputCollector _collector;
        Schema _schema;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            _collector = outputCollector;
            Schema.Parser parser = new Schema.Parser();
            Schema.Parser parser2 = new Schema.Parser();
            try {
                _schema = new Schema.Parser().parse(new File("/home/shivaraj2/Downloads/sales.avsc"));

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public void execute(Tuple tuple) {
            Message message = new Message((byte[])((TupleImpl) tuple).get("bytes"));
            //tuple.
            ByteBuffer bb = message.payload();

            byte[] b = new byte[bb.remaining()];
            bb.get(b, 0, b.length);

            try {

                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(b, null);
                GenericRecord result = reader.read(null, decoder);
                System.out.println("Id: "+ result.get("id"));
                System.out.println("supId: "+ result.get("supid"));
                System.out.println("amt: " +result.get("amt") );
                System.out.println("custId: "+ result.get("cust_id"));
                System.out.println("amt: " +result.get("sup_name") );
                //Format formatter = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
                //String s = formatter.format((Long) result.get("timeStamp"));
                //System.out.println("timeStamp: " + s);
                int id=(int)result.get("id");
                int supid=(int)result.get("supid");
                int custid=(int)result.get("cust_id");
                int amt=(int)result.get("amt");
                String supName=result.get("sup_name").toString();
                
                
                String v=id+" "+supid+" "+custid+" "+amt+" "+supName;
                System.out.println("value  "+v);
               _collector.emit(new Values(v));

                //System.out.println("PLine Text: " + ((GenericRecord) result.get("subrecord")).get("text"));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }



            _collector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("word"));
        }

    
}
