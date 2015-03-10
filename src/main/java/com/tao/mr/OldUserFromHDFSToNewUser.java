package com.tao.mr;

import java.io.IOException;

import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Counters;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class OldUserFromHDFSToNewUser extends Configured implements Tool {

	private static String inputPath = "/user/root/bh_data_platform/pushdata/userinfo_olduser";
	private static String outputPath = "/user/root/bh_data_platform/pushdata/userinfo_newuser";

	private static enum PushDataStatus {
		CONVERT_SUCCESS_NUM, CONVERT_FAIL_NUM, EXCEPTION_NUM
	}


	public int run(String[] args) throws Exception {
		Job job = Job.getInstance(this.getConf(), "OldUserFromHDFSToNewUser");
		job.setJarByClass(OldUserFromHDFSToNewUser.class);

		job.setJarByClass(OldUserFromHDFSToNewUser.class);
		job.setMapperClass(OldUserFromHDFSToNewUserMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setNumReduceTasks(0);

		FileInputFormat.setInputPaths(job, new Path(inputPath));
		FileOutputFormat.setOutputPath(job, new Path(outputPath));

		int result = job.waitForCompletion(true) ? 0 : 1;

		// 输出计数结果
		Counters counters = job.getCounters();
		Counter successNum = counters.findCounter(PushDataStatus.CONVERT_SUCCESS_NUM);
		Counter failNum = counters.findCounter(PushDataStatus.CONVERT_FAIL_NUM);
		Counter exceptionNum = counters.findCounter(PushDataStatus.EXCEPTION_NUM);
		System.out.println(successNum.getDisplayName() + ":" + successNum.getName() + "=" + successNum.getValue());
		System.out.println(failNum.getDisplayName() + ":" + failNum.getName() + "=" + failNum.getValue());
		System.out.println(exceptionNum.getDisplayName() + ":" + exceptionNum.getName() + "=" + exceptionNum.getValue());

		return result;
	}

	public static class OldUserFromHDFSToNewUserMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		private static Logger logger = Logger.getLogger(OldUserFromHDFSToNewUserMapper.class);

		// 转换成功并发送成功数�?
		private Counter successNum;
		// 失败数量
		private Counter failNum;
		// 异常数量
		private Counter exceptionNum;

		@Override
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			int callback = 0;
			String oldUserJson = value.toString();

			try {

				JSONObject oldUserJsonObj = JSONObject.fromObject(oldUserJson);
//				OldUserData OldUserData = (OldUserData) JSONObject.toBean(oldUserJsonObj, OldUserData.class);

//				NewUserData newData = new NewUserData();
//				callback = ConvertLogic.convertOldToNew(OldUserData, newData);

				if (callback == 0) {
					successNum = context.getCounter(PushDataStatus.CONVERT_SUCCESS_NUM);
					successNum.increment(1);

				} else if (callback == -1) {
					failNum = context.getCounter(PushDataStatus.CONVERT_FAIL_NUM);
					failNum.increment(1);
					return;

				}

//				JSONObject newDataJson = JSONObject.fromObject(newData);
//				context.write(new Text(newDataJson.toString()), NullWritable.get());

			} catch (Exception e) {
				exceptionNum = context.getCounter(PushDataStatus.EXCEPTION_NUM);
				exceptionNum.increment(1);

				logger.error("Convert Error!!!" + oldUserJson);
				e.printStackTrace();
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		int result = ToolRunner.run(conf, new OldUserFromHDFSToNewUser(), args);
		System.exit(result);

	}
}