/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.databasetopology;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;

import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;


import java.util.UUID;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.utils.Utils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;

import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.KeyValueScheme;
import org.apache.storm.kafka.KeyValueSchemeAsMultiScheme;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.spout.RawMultiScheme;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
/**
 *
 * @author shivaraj1
 */
public class AvroKafkaSpout {
    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException {

        //Add those lines to prevent too much logging noise in the console
        Logger.getLogger("storm.kafka.PartitionManager").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.storm").setLevel(Level.ERROR);
        Logger.getLogger("org.apache.storm.kafka").setLevel(Level.ERROR);

        TopologyBuilder builder = new TopologyBuilder();
        String zkConnString = "192.168.56.102:2080,192.168.56.103:2080,192.168.56.105:2080";
        String topic = "str-sales-ord";
        BrokerHosts hosts = new ZkHosts(zkConnString);

        
        //SpoutConfig kafkaConfig = new SpoutConfig(KafkaConfig.StaticHosts.fromHostString(hosts, partitions), topic, offsetPath, consumerId);
        SpoutConfig kafkaSpoutConfig = new SpoutConfig (hosts, topic, "/" + topic,UUID.randomUUID().toString());
        //KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);
        //builder.setSpout("spout", kafkaSpout);
       //kafkaSpoutConfig.scheme = new RawMultiScheme(new StringScheme());
       //kafkaSpoutConfig.scheme = new KeyValueSchemeAsMultiScheme(KeyValueScheme s);
       kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
       //kafkaSpoutConfig.fetchSizeBytes = 5000000;
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
        //builder.setBolt("des-bolt", new SimpleBolt()).shuffleGrouping("kafka-spout");
       builder.setBolt("db-bolt", new ToDbBolt()).shuffleGrouping("kafka-spout");
        //builder.setBolt("file-bolt", new SimpleBolt()).shuffleGrouping("kafka-spout");

        Config conf = new Config();
        conf.setDebug(true);

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("AvroKafkaSpout", conf, builder.createTopology());
            Utils.sleep(100000);
            //cluster.killTopology("AvroKafkaSpout");
            //cluster.shutdown();
}
}
