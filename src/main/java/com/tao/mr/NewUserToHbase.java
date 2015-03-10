package com.tao.mr;

import java.io.IOException;

import net.sf.json.JSONException;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class NewUserToHbase extends Configured implements Tool {
	
	private static Logger logger = Logger.getLogger(NewUserToHbase.class);
	
	private static String ONLINE_HBASE_TABLE = "baihe_userCloud";


	public int run(String[] args) throws Exception {
		
		String inputPath = args[0];
		
		Job job = Job.getInstance(this.getConf(), "NewUserToHbase");
		job.setJarByClass(NewUserToHbase.class);

		job.setMapperClass(NewUserToHbaseMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Put.class);
		FileInputFormat.setInputPaths(job, inputPath);		

		TableMapReduceUtil.initTableReducerJob(ONLINE_HBASE_TABLE, NewUserToHbaseReduce.class, job);
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		
		return result;
	}

	
	public static class NewUserToHbaseMapper extends Mapper<LongWritable, Text, Text,NullWritable> {
		@Override
		public void map(LongWritable key, Text line, Context context) 
					throws IOException, InterruptedException {
			String newUserData = line.toString();
			
			context.write(new Text(newUserData),NullWritable.get());
		}
	}
	
	public static class NewUserToHbaseReduce extends TableReducer<Text, NullWritable, NullWritable> {
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			Put put = null;
			try {
					JSONObject newDataJSON = JSONObject.fromObject(key.toString());
					
					//NewUserData newData = (NewUserData)JSONObject.toBean(newDataJSON,NewUserData.class);
					//put = ConvertHbaseDataToBean.resultToOnlinePut(newData, 1);
					if(put != null && !put.isEmpty())
					{	
						context.write(NullWritable.get(), put);
					}	
			} 
			catch (JSONException e)
			{
				e.printStackTrace();
			}
			catch (IOException e)
			{
				if (put == null) {
					logger.error("put is null");
				} else {
					logger.error("put is not null");
					logger.info(put.getClass().getName());
					logger.info("input is : " + key.toString());
				}
				throw e;
			}
		}

	}

	private static void usage() {
		System.err.println("输入参数: <-Dmapred.reduce.tasks=n> <SolrPushType[PUSH_ALL/PUSH_PART/PUSH_ALL_NO_DEFAULT_VALUE]> <Hbase起始�? <Hbase结束�?");
		System.exit(1);
	}

	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		//conf.set("hbase.zookeeper.quorum", EnvConfigUtil.getProperty("ONLINE_HBASE_ZOOKEEPER_QUORUM"));
	//	conf.set("hbase.zookeeper.property.clientPort", EnvConfigUtil.getProperty("ONLINE_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT"));
		int res = ToolRunner.run(conf, new NewUserToHbase(), args);
		System.exit(res);

	}
}