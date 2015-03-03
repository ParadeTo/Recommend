package com.paradeto.recommend;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * 共现矩阵乘评分向量的加法部分
 * 优化了原始代码
 *
 */
public class Step4_Update2 {

    public static class Step4_RecommendMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {
            //原来的，修改之前
        	//String[] tokens = Recommend.DELIMITER.split(values.toString());
            //Text k = new Text(tokens[0]);
            //Text v = new Text(tokens[1]+","+tokens[2]);
            //context.write(k, v);
            //修改后
        	String[] tokens = Recommend.DELIMITER.split(values.toString());
            Text k = new Text(tokens[0]+","+tokens[1]);
            Text v = new Text(tokens[2]);
            context.write(k, v);
        }
    }

    public static class Step4_RecommendReducer extends Reducer<Text, Text, Text, Text> {
        
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //修改前
        	/*System.out.println(key.toString() + ":");
            Map<String, Double> map = new HashMap<String, Double>();// 结果
            
            for (Text line : values) {
                System.out.println(line.toString());
                String[] tokens = Recommend.DELIMITER.split(line.toString());
                String itemID = tokens[0];
                Double score = Double.parseDouble(tokens[1]);
                
                 if (map.containsKey(itemID)) {
                     map.put(itemID, map.get(itemID) + score);// 矩阵乘法求和计算
                 } else {
                     map.put(itemID, score);
                 }
            }
            
            Iterator<String> iter = map.keySet().iterator();
            while (iter.hasNext()) {
                String itemID = iter.next();
                double score = map.get(itemID);
                Text v = new Text(itemID + "," + score);
                context.write(key, v);
            }*/
        	//修改后
        	System.out.println(key+"---");
        	double score=0.0;
        	for(Text line : values){
        		System.out.println(line);
        		score += Double.valueOf(line.toString());
        	}
        	String[] tokens = Recommend.DELIMITER.split(key.toString());
        	String userID = tokens[0];
        	String itemID = tokens[1];
        	Text k = new Text(userID);
        	Text v = new Text(itemID + "," + String.valueOf(score));
        	context.write(k, v);
        }
    }

    public static void run(Map<String, String> path) throws IOException, InterruptedException, ClassNotFoundException {
		// 获得配置信息
		Configuration conf = Recommend.config();
		// 得到输入输出路径
		Path input = new Path(path.get("Step4_2Input"));
		Path output = new Path(path.get("Step4_2Output"));
		// 删除上一次的输出
		HDFSFile hdfs = new HDFSFile(new Path(Recommend.HDFS));
		hdfs.delFile(output);
		// 设置作业参数
        Job job = new Job(conf);
        job.setJarByClass(Step4_Update2.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Step4_Update2.Step4_RecommendMapper.class);
        job.setReducerClass(Step4_Update2.Step4_RecommendReducer.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.setInputPaths(job, input);
        FileOutputFormat.setOutputPath(job, output);

        job.waitForCompletion(true);
    }

}
