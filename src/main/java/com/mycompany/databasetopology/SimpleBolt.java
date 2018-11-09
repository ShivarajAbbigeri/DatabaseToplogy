/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.mycompany.databasetopology;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.storm.shade.org.apache.zookeeper.server.ServerConfig;
	
	import org.apache.storm.tuple.Tuple;
	import org.apache.storm.tuple.Fields;
	import org.apache.storm.tuple.Values;
	
	import org.apache.storm.task.OutputCollector;
	import org.apache.storm.topology.OutputFieldsDeclarer;
	import org.apache.storm.topology.IRichBolt;
	import org.apache.storm.task.TopologyContext;
	
	
	public class SimpleBolt implements IRichBolt {
	private OutputCollector collector;
        PrintWriter pw;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context,
	OutputCollector collector) {
	this.collector = collector;
            try {
                pw = new PrintWriter("output.txt","UTF-8");
            } catch (FileNotFoundException ex) {
                Logger.getLogger(SimpleBolt.class.getName()).log(Level.SEVERE, null, ex);
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(SimpleBolt.class.getName()).log(Level.SEVERE, null, ex);
            }
	}
	
	@Override
	public void execute(Tuple input) {
           //input.getByte(0);
           //System.out.println("Fields   "+input.getValue(0).toString());
	//System.out.println("String value  "+input.getValue(0));
        String data = input.getString(0);
        
        //HashMap map = this.makeMapOfMessage(data);
        //map.put("key", "value");    
        
        
        
        
        //String line = (String)input.getValue(0);
        //char[] chars = line.toCharArray();
        //String line2 = (String)input.getValue(1);
        //String line3 = (String)input.getValue(2);
        //String line4 = (String)input.getValue(3);
        //System.out.println("String value : "  + chars[0]);
        //System.out.println("String value : "  + chars[1]); 
        //System.out.println("String value : "  + chars[2]); 
        //System.out.println("String value : "  + chars[3]); 
        //System.out.println("String value : "  + chars[4]); 
        //System.out.println("String value : "  + chars[5]); 
        //System.out.println("String value : "  + chars[6]); 
        //System.out.println("String value : "  + chars[7]); 
        //System.out.println("String value : "  + chars[8]); 
        //System.out.println("String value : "  + chars[9]);
        //System.out.println("String value : "  + chars[10]); 
        //System.out.println("String value : "  + chars[11]); 
        //System.out.println("String value : "  + chars[12]); 
        //System.out.println("String value : "  + chars[13]); 
        //System.out.println("String value : "  + chars[14]); 
        //System.out.println("String value : "  + chars[15]); 
        //System.out.println("String value : "  + chars[16]); 
        //System.out.println("String value : "  + chars[17]); 
        
        //System.out.println("Input Size:  "  + input.size());
        //System.out.println("Map Values: "  + map.toString());
        //String sales = null;
        //input.�
        //Fields fields = input.getFields();
        System.out.println("fffffffffffff    "+data);
        /*
            try {
                sales = new String((byte[]) input.getValueByField(fields.get(0)), "UTF-8");
                String[] stockData = sales.split(",");
                for(int i=0;i<stockData.length;i++)
                    System.out.println("array "+stockData[i]);
            } catch (UnsupportedEncodingException ex) {
                Logger.getLogger(SimpleBolt.class.getName()).log(Level.SEVERE, null, ex);
            }
            */
        /*
        byte[] valueBytes = (byte[]) input.getValueByField("bytes");
        
        String ss=new String(valueBytes);
        */
        //System.out.println("value    "+sales);
      
        //pw.append(val);
	//int sig = 0;
	//collector.emit(new Values(input.getString(0), sig));
	collector.ack(input);
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	declarer.declare(new Fields("word"));
	}
	
	@Override
	public void cleanup() {}
	
	@Override
	public Map<String, Object> getComponentConfiguration() {
	return null;
	}
        
        
	private HashMap makeMapOfMessage(String message){
            
            String[] fields = message.split(" ");
            HashMap<String,String> map = new HashMap<>();
            try {
                int count = 0;
                String first = "bogey1", second = "bogey2";
                for(String field : fields){
                    if(count%2==0){
                        first = field;
                    }
                    else{
                        second = field;
                    }
                    count++;
                    map.put(first, second);
                }
                
            }
            catch(ArrayIndexOutOfBoundsException e){
                e.printStackTrace();
            }
            return map;
        }
        
        
}


        
