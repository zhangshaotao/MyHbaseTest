package com.tao.mr;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class PushDataFromHbaseToHbaseOnline extends Configured implements Tool {
	
	private static Logger logger = Logger.getLogger(PushDataFromHbaseToHbaseOnline.class);
	
	private static String BI_HBASE_TABLE = "bhdp_user_new";
	private static String ONLINE_HBASE_TABLE = "baihe_userCloud";

	private static String BI_HBASE_ZOOKEEPER_QUORUM = null;//EnvConfigUtil.getProperty("BI_HBASE_ZOOKEEPER_QUORUM");
	private static String BI_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT = null;// EnvConfigUtil.getProperty("BI_HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT");

	private static String ONLINE_HBASE_ZOOKEEPER_QUORUM_ADDRESS = null;//EnvConfigUtil.getProperty("ONLINE_HBASE_ZOOKEEPER_QUORUM_ADDRESS");
	//private static String ONLINE_HBASE_ZOOKEEPER_QUORUM_ADDRESS = EnvConfigUtil.getProperty("BI_HBASE_ZOOKEEPER_QUORUM_ADDRESS");
	
	private static enum PushDataStatus
	{
		SUCCESS_NUM,FAIL_NUM
	}

	
	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "PushDataFromHbaseToHbaseOnline");
		job.setJarByClass(PushDataFromHbaseToHbaseOnline.class);

		Scan scan = new Scan();
		scan.addFamily(Bytes.toBytes("userInfo"));
		scan.addFamily(Bytes.toBytes("matchInfo"));
		// scan.addColumn(Bytes.toBytes("userInfo"), Bytes.toBytes("userName"));

		scan.setCaching(100);
		scan.setCacheBlocks(false);

		if (args.length == 2) {
			logger.info("StartRow:"+args[0]+",StopRow:"+args[1]);
			scan.setStartRow(Bytes.toBytes(args[0]));
			scan.setStopRow(Bytes.toBytes(args[1]));
		}
		
		TableMapReduceUtil.initTableMapperJob(BI_HBASE_TABLE, scan, PushDataFromHbaseToHbaseOnlineMapper.class, ImmutableBytesWritable.class, Result.class,
				job);

		TableMapReduceUtil.initTableReducerJob(ONLINE_HBASE_TABLE, PushDataFromHbaseToHbaseOnlineReducer.class, job, null,
				ONLINE_HBASE_ZOOKEEPER_QUORUM_ADDRESS, null, null);
		
		int result = job.waitForCompletion(true) ? 0 : 1;
		
		//输出计数结果
		Counters counters = job.getCounters();
		Counter successNum = counters.findCounter(PushDataStatus.SUCCESS_NUM);
		Counter failNum = counters.findCounter(PushDataStatus.FAIL_NUM);
		System.out.println(successNum.getDisplayName() + ":" + successNum.getName() + "=" + successNum.getValue());
		System.out.println(failNum.getDisplayName() + ":" + failNum.getName() + "=" + failNum.getValue());
		
		return result;
	}

	public static class PushDataFromHbaseToHbaseOnlineMapper extends TableMapper<ImmutableBytesWritable, Result> {

		@Override
		public void map(ImmutableBytesWritable key, Result result, Context context) throws InterruptedException, IOException {
			context.write(key, result);
		}

	}


	public static class PushDataFromHbaseToHbaseOnlineReducer extends TableReducer<ImmutableBytesWritable, Result, NullWritable> {
		private int pushType;

		private Counter successNum;
		private Counter failNum;
		
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			//Configuration conf = context.getConfiguration();
			//pushType = Integer.valueOf(conf.get("push.type"));
		}

		@Override
		public void reduce(ImmutableBytesWritable key, Iterable<Result> values, Context context) throws IOException, InterruptedException {
			for (Result result : values) {
				Put put = null;//ConvertHbaseDataToBean.resultToOnlinePut(result, 1);
				if (put != null)
				{	
					context.write(NullWritable.get(), put);
					
					successNum = context.getCounter(PushDataStatus.SUCCESS_NUM);
					successNum.increment(1);
				}
				else
				{
					failNum = context.getCounter(PushDataStatus.FAIL_NUM);
					failNum.increment(1);
				}
			}
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

		int result = ToolRunner.run(conf, new PushDataFromHbaseToHbaseOnline(), args);
		System.exit(result);

	}
}