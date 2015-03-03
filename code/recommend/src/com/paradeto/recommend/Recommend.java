package com.paradeto.recommend;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;

/**
 * 基于物品的协同推荐系统
 */
public class Recommend {
	//HDFS文件路径
	public static final String HDFS = "hdfs://master:9000";
	//MapReduce分割符为\t和,
    public static final Pattern DELIMITER = Pattern.compile("[\t,]");
    //入口函数
    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
    	Map<String, String> path = new HashMap<String, String>();
    	//本地数据路径
    	path.put("data", "/home/youxingzhi/workspace/Lesson5/small2.csv");
    	//步骤1的输入输出路径
        path.put("Step1Input", HDFS + "/recommend");
        path.put("Step1Output", path.get("Step1Input") + "/step1");
        //步骤2的输入输出路径
        path.put("Step2Input", path.get("Step1Output"));
        path.put("Step2Output", path.get("Step1Input") + "/step2");
        //步骤3_1的输入输出路径
        path.put("Step3Input1", path.get("Step1Output"));
        path.put("Step3Output1", path.get("Step1Input") + "/step3_1");
        //步骤3_2的输入输出路径
        path.put("Step3Input2", path.get("Step2Output"));
        path.put("Step3Output2", path.get("Step1Input") + "/step3_2");
        //步骤4的输入输出路径
        path.put("Step4_1Input1", path.get("Step3Output1"));
        path.put("Step4_1Input2", path.get("Step3Output2"));
        path.put("Step4_1Output", path.get("Step1Input") + "/step4_1");      
        path.put("Step4_2Input", path.get("Step4_1Output"));
        path.put("Step4_2Output", path.get("Step1Input") + "/step4_2");
        //步骤5的输入输出路径
        path.put("Step5Input1", path.get("Step4_2Output"));
        path.put("Step5Input2", path.get("Step1Input")+"/small2.csv");
        path.put("Step5Output", path.get("Step1Input") + "/step5");
        

        //Step1.run(path);
        //Step2.run(path);
        //Step3.run1(path);
        //Step3.run2(path);   
        //Step4_Update.run(path);
        //Step4_Update2.run(path);
        Step5.run(path);
        
        //输出结果到终端
        HDFSFile hdfs = new HDFSFile(new Path(HDFS));
        //Step1的输出结果
        //System.out.println(path.get("Step1Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step1Output")+"/part-r-00000"));
        //Step2的输出结果
        //System.out.println(path.get("Step2Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step2Output")+"/part-r-00000"));
        //Step3_1的输出结果
        //System.out.println(path.get("Step3Output1")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step3Output1")+"/part-r-00000"));
        //Step3_2的输出结果
        //System.out.println(path.get("Step3Output2")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step3Output2")+"/part-r-00000"));
        //Step4_1的输出结果
        //System.out.println(path.get("Step4_1Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step4_1Output")+"/part-r-00000"));
        //System.exit(0);
        //Step4_2的输出结果
        //System.out.println(path.get("Step4_2Output")+"/part-r-00000");
        //hdfs.readFile(new Path(path.get("Step4_2Output")+"/part-r-00000"));
        //Step5的输出结果
        System.out.println(path.get("Step5Output")+"/part-r-00000");
        hdfs.readFile(new Path(path.get("Step5Output")+"/part-r-00000"));
        
        System.exit(0);
    
    }
    public static Configuration config() {
    	Configuration conf = new Configuration();
        return conf;
    }
}
