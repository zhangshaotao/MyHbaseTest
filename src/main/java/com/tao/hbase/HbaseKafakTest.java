package com.tao.hbase;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import scala.util.parsing.json.JSONObject;

import com.tao.bean.MatchInfo;
import com.tao.bean.UserInfo;

public class HbaseKafakTest extends HBaseUserPojo{
	
	private static Producer<String, String> producer;
	
	public static void main(String[] args)
	{
		testHBaseCoprocessor();
	}
	
	public Result getResult(String rowKey, String family) throws IOException {
		return super.getResult(rowKey, family);
	}
	
	public void initProduer()
	{
		Properties props = new Properties();
		props.put("metadata.broker.list", "172.16.3.64:9092,172.16.3.74:9092,172.16.3.77:9092,172.16.3.87:9092,172.16.3.93:9092");
	//	props.put("partitioner.class", "com.baihe.hadoop.kafka.SimplePartitionerByUserID");
		props.put("zookeeper.connect", "172.16.3.64:2181,172.16.3.74:2181,172.16.3.77:2181,172.16.3.87:2181,172.16.3.93:2181/kafka");
	//	props.put("metadata.broker.list", "172.16.4.201:9092,172.16.4.128:9092,172.16.4.104:9092,172.16.4.214:9092, 172.16.3.195:9092,172.16.3.88:9092,172.16.3.107:9092,172.16.3.192:9092,172.16.3.123:9092");
		//props.put("partitioner.class", "com.baihe.hadoop.kafka.SimplePartitionerByUserID");
	//	props.put("zookeeper.connect", "172.16.3.107:2181,172.16.3.192:2181,172.16.3.123:2181");
		props.put("request.required.acks", "1");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("producer.type", "async");
		
		ProducerConfig config = new ProducerConfig(props);

		this.producer = new Producer<String, String>(config);
	}
	
	public static void testHBaseToKafka()
	{
		HbaseKafakTest test = new HbaseKafakTest();
		Result result = null;
		String userId = "7217444_uid";
		//String userId = "613201_uid";
		try {
			result = test.getResult(userId, UserInfo.ROW_FAMILY);
			
			List<KeyValue> userInfolist = result.list();
			UserInfo userInfo = new UserInfo();
			userInfo = HbaseRowConvertToBean.convertHbaseDataTobean(userInfo,userId,userInfolist);
			
			
			MatchInfo matchInfo = new MatchInfo();
			matchInfo = HbaseRowConvertToBean.convertHbaseDataTobean(matchInfo,userId,userInfolist);
			
			//result = test.getResult(userId, BhdpUserCfMatchInfo.FAMILY);
			//List<KeyValue> matchInfolist = result.list();
			//ConvertHbaseDataToBean.convertHbaseDataTobean(oldData,userId,matchInfolist);
			
	        //JSONObject jsonOldData = new JSONObject(oldData);
	        //JSONObject jsonNewData = new JSONObject(newData);
	        
	       // Put put = ConvertHbaseDataToBean.resultToOnlinePut(newData, 0);
	        
	      //  String newMapJson = ConvertHbaseDataToBean.putToJson(put);
	        
	        KeyedMessage<String, String> data1 = null;
			
			data1 = new KeyedMessage<String, String>("test", userInfo.getUserID(),"----userInfo---"+userInfo.toString()+"\n"+
					"--------matchInfo-------"+matchInfo.toString());
	        
	        test.initProduer();
	        producer.send(data1);
	        
//			System.out.println("---new---new----"+jsonNewData);
//			
//	        System.out.println("---old---old----"+jsonOldData);
	      //  System.out.println("---map---map----"+newMapJson);
	        //logger.info("---new---new----"+jsonNewData);
			
		} catch (IOException e) {
			
			e.printStackTrace();
		}
	}
	
	public static void testHBaseCoprocessor()
	{
		Put put = new Put("7217444_uid".getBytes());
		put.add("userInfo".getBytes(), "height".getBytes(), "173".getBytes());
		
		//String newMapJson = ConvertHbaseDataToBean.putToJson(put);
		
		//System.out.println(newMapJson);
	}
}
