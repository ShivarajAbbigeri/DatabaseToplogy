package com.mycompany.databasetopology;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import java.util.UUID;
import org.apache.kafka.common.utils.Utils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
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
        SpoutConfig kafkaSpoutConfig = new SpoutConfig(hosts, topic, "/" + topic, UUID.randomUUID().toString());

        kafkaSpoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        builder.setSpout("kafka-spout", new KafkaSpout(kafkaSpoutConfig));
        builder.setBolt("db-bolt", new ToDbBolt()).shuffleGrouping("kafka-spout");

        Config conf = new Config();
        conf.setDebug(true);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("AvroKafkaSpout", conf, builder.createTopology());
        Utils.sleep(100000);
        //cluster.killTopology("AvroKafkaSpout");
        //cluster.shutdown();
    }
}
