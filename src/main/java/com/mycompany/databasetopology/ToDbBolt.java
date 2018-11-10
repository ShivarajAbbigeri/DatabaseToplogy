package com.mycompany.databasetopology;

import com.google.gson.Gson;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.storm.tuple.Tuple;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.task.TopologyContext;

public class ToDbBolt implements IRichBolt {

    private OutputCollector collector;
    Gson gson;
    Connection conn = null;
    PreparedStatement stmt = null;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {
        this.collector = collector;
        gson = new Gson();
        try {

            Class.forName("com.mysql.jdbc.Driver");

            conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/credits", "root", "root");

            String sql = "insert into credit values(?,?,?,?,?)";
            stmt = conn.prepareStatement(sql);

        } catch (SQLException se) {
            //Handle errors for JDBC
            se.printStackTrace();
        } catch (Exception e) {
            //Handle errors for Class.forName
            e.printStackTrace();
        }//end try
    }

    @Override
    public void execute(Tuple input) {
        String val = input.getString(0);

        String[] v = val.split(" ");

        try {
            stmt.setInt(1, Integer.parseInt(v[0]));
            stmt.setInt(2, Integer.parseInt(v[1]));
            stmt.setInt(3, Integer.parseInt(v[2]));
            stmt.setInt(4, Integer.parseInt(v[3]));
            stmt.setString(5, v[4]);

            stmt.executeUpdate();
        } catch (SQLException ex) {
            Logger.getLogger(ToDbBolt.class.getName()).log(Level.SEVERE, null, ex);
        }

        collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

    @Override
    public void cleanup() {

        try {
            stmt.close();
            conn.close();
        } catch (SQLException ex) {
            Logger.getLogger(ToDbBolt.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }

}
