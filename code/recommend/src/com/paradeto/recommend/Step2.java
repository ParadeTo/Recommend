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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.paradeto.recommend.Step1.Step1_ToItemPreMapper;
import com.paradeto.recommend.Step1.Step1_ToUserVectorReducer;

/**
 * 由用户评分向量得到共现矩阵
 *
 */
public class Step2 {
	
	public static class Step2_UserVectorToCooccurrenceMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        private final static Text k = new Text();
        private final static IntWritable v = new IntWritable(1);

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            String[] tokens = Recommend.DELIMITER.split(values.toString());
            for (int i = 1; i < tokens.length; i++) {
                String itemID = tokens[i].split(":")[0];
                for (int j = 1; j < tokens.length; j++) {
                    String itemID2 = tokens[j].split(":")[0];
                    k.set(itemID + ":" + itemID2);
                    context.write(k, v);
                }
            }
        }
    }

    public static class Step2_UserVectorToConoccurrenceReducer extends Reducer <Text, IntWritable, Text, IntWritable> {
    	private IntWritable result = new IntWritable();
        
        @Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context)
				throws IOException, InterruptedException {
        	int sum = 0;
            for (IntWritable value:values) {
                sum += value.get();
            }
            result.set(sum);
            context.write(key, result);
        }

    }
    public static void run(Map<String, String> path) throws IOException, ClassNotFoundException, InterruptedException {
    	//获得配置信息
    	Configuration conf = Recommend.config();
    	//得到输入输出路径
        Path input = new Path(path.get("Step2Input"));
        Path output = new Path(path.get("Step2Output"));
        //删掉上次输出结果
        HDFSFile hdfs = new HDFSFile(new Path(Recommend.HDFS));
        hdfs.delFile(output);
        //设置作业参数
        Job job = new Job(conf,"Step2");
		job.setJarByClass(Step2.class);
		
		job.setMapperClass(Step2_UserVectorToCooccurrenceMapper.class);
		job.setCombinerClass(Step2_UserVectorToConoccurrenceReducer.class);
		job.setReducerClass(Step2_UserVectorToConoccurrenceReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job,input);
		FileOutputFormat.setOutputPath(job,output);
		//运行作业
		job.waitForCompletion(true);
    }
}
