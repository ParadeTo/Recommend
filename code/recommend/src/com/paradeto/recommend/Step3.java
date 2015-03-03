package com.paradeto.recommend;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.paradeto.recommend.Step1.Step1_ToItemPreMapper;
import com.paradeto.recommend.Step1.Step1_ToUserVectorReducer;

/**
 * 对评分向量和共现矩阵进行整理
 *
 */
public class Step3 {
	 public static class Step31_UserVectorSplitterMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
		private final static IntWritable k = new IntWritable();
		private final static Text v = new Text();

		@Override
		public void map(LongWritable key, Text values, Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			for (int i = 1; i < tokens.length; i++) {
				String[] vector = tokens[i].split(":");
				int itemID = Integer.parseInt(vector[0]);
				String pref = vector[1];

				k.set(itemID);
				v.set(tokens[0] + ":" + pref);
				context.write(k, v);
			}
		}
	}
	public static void run1(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
		//获得配置信息
    	Configuration conf = Recommend.config();
    	//得到输入输出路径
        Path input = new Path(path.get("Step3Input1"));
        Path output = new Path(path.get("Step3Output1"));
        //删除上一次的输出
        HDFSFile hdfs = new HDFSFile(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //设置作业参数
        Job job = new Job(conf,"Step3_1");
		job.setJarByClass(Step3.class);
		
		job.setMapperClass(Step31_UserVectorSplitterMapper.class);
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//运行作业
		job.waitForCompletion(true);
	}
	
	 public static class Step32_CooccurrenceColumnWrapperMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
		private final static Text k = new Text();
		private final static IntWritable v = new IntWritable();

		@Override
		public void map(LongWritable key, Text values,Context context)
				throws IOException, InterruptedException {
			String[] tokens = Recommend.DELIMITER.split(values.toString());
			k.set(tokens[0]);
			v.set(Integer.parseInt(tokens[1]));
			context.write(k, v);
		}
	}

	public static void run2(Map<String, String> path) throws IOException,
		ClassNotFoundException, InterruptedException {
		// 获得配置信息
		Configuration conf = Recommend.config();
		// 得到输入输出路径
		Path input = new Path(path.get("Step3Input2"));
		Path output = new Path(path.get("Step3Output2"));
		// 删除上一次的输出
		HDFSFile hdfs = new HDFSFile(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// 设置作业参数
		Job job = new Job(conf, "Step3_2");
		job.setJarByClass(Step3.class);

		job.setMapperClass(Step32_CooccurrenceColumnWrapperMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, input);
		FileOutputFormat.setOutputPath(job, output);
		// 运行作业
		job.waitForCompletion(true);
	}
}
