package com.tao.mr;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class OldUserFromHbaseToHDFS extends Configured implements Tool {
	
	private static Logger logger = Logger.getLogger(OldUserFromHbaseToHDFS.class);
	
	private static String BI_HBASE_TABLE = "bhdp_user_temp";

	private static String BI_HBASE_ZOOKEEPER_QUORUM = null;//EnvConfigUtil.getProperty("BI_HBASE_ZOOKEEPER_QUORUM");
	private static String BI_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = null;//EnvConfigUtil.getProperty("BI_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT");


	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "OldUserFromHbaseToHDFS");
		job.setJarByClass(OldUserFromHbaseToHDFS.class);

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("userInfo"));
		//scan.addFamily(Bytes.toBytes("matchInfo"));
		// scan.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("userName"));

		scan.setCaching(100);
		scan.setCacheBlocks(false);

		String outputPath = args[0];
		
		if (args.length == 3) {
			logger.info("StartRow:" + args[1] + ",StopRow:" + args[2]);
			scan.setStartRow(Bytes.toBytes(args[1]));
			scan.setStopRow(Bytes.toBytes(args[2]));
		}
		TableMapReduceUtil.initTableMapperJob(BI_HBASE_TABLE, scan, OldUserFromHbaseToHDFSMapper.class, Text.class, NullWritable.class,
				job);
		
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		job.setNumReduceTasks(0);
		int result = job.waitForCompletion(true) ? 0 : 1;
		
		return result;
	}

	public static class OldUserFromHbaseToHDFSMapper extends TableMapper<Text,NullWritable> {

		public void map(ImmutableBytesWritable key, Result result,
				Context context) throws InterruptedException, IOException {

			String userId = new String(result.getRow());
			// 若rowkey不是以_uid结尾的，则不处理
			if (!userId.endsWith("_uid"))
				return;

			List<KeyValue> list = result.list();
			//OldUserData oldData = new OldUserData();
			//ConvertHbaseDataToBean
			//		.convertHbaseDataTobean(oldData, userId, list);
			
			//JSONObject jsonOldData = new JSONObject(oldData);
			
			//logger.info("---old---old----"+jsonOldData);
			
			//context.write(new Text(jsonOldData.toString()),NullWritable.get());
		}

	}


	private static void usage() {
		System.err.println("输入参数: <-Dmapred.reduce.tasks=n> <SolrPushType[PUSH_ALL/PUSH_PART/PUSH_ALL_NO_DEFAULT_VALUE]> <Hbase起始�? <Hbase结束�?");
		System.exit(1);
	}

	public static void main(String[] args) throws Exception {
		/*if (args.length != 2 && args.length != 4) {
			usage();
		}
		int pushType = -1;
		if (args[1].equals("PUSH_ALL"))
			pushType = SolrOperateCommon.PUSH_ALL;
		else if (args[1].equals("PUSH_PART"))
			pushType = SolrOperateCommon.PUSH_PART;
		else if (args[1].equals("PUSH_ALL_NO_DEFAULT_VALUE"))
			pushType = SolrOperateCommon.PUSH_ALL_NO_DEFAULT_VALUE;
		else
			usage();
		*/	

		Configuration conf = new Configuration();
		conf.setLong(HConstants.HBASE_REGIONSERVER_LEASE_PERIOD_KEY, 120000);
		conf.set("hbase.zookeeper.quorum", BI_HBASE_ZOOKEEPER_QUORUM);
		conf.set("hbase.zookeeper.property.clientPort", BI_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT);

		int result = ToolRunner.run(conf, new OldUserFromHbaseToHDFS(), args);
		System.exit(result);

	}
}