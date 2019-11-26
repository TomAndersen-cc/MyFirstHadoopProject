package com.TomAndersen.hadoop.BayesClassification;

import com.TomAndersen.hadoop.HDFSTools.BayesTools;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 用于启动所有的Job，本类中只进行作业调度
 */
public class JobsInitiator {

    // 单机测试下的本地文件系统相对路径，千万注意此路径集群中不能使用，集群中路径前面需要加/
    public static final String Job1_OutputPath = "src/Output/Job1/";
    public static final String Job2_OutputPath = "src/Output/Job2/";
    public static final String Job3_OutputPath = "src/Output/Job3/";


    /*// 集群测试下的HDFS文件路径
    public static final String Job1_OutputPath = "/src/Output/Job1/";
    public static final String Job2_OutputPath = "/src/Output/Job2/";
    public static final String Job3_OutputPath = "/src/Output/Job3/";
    */

    public static void main(String[] args) throws Exception {
        // args中有两个参数，第一个为训练集，第二个为测试集
        /*try {
            if (args.length < 2)
                throw new IllegalArgumentException("***************At least two parameters are required!***************");
            else if (args.length > 2)
                throw new IllegalArgumentException("***************Only two parameters are required!***************");
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
            System.exit(1);
        }*/
        if (args.length < 1) {
            System.out.println("***************At least one parameters are required!***************");
            System.exit(1);
        } else if (args.length > 1) {
            System.out.println("***************Only one parameters are required!***************");
            System.exit(1);
        }

        // 第一个参数为训练集文档路径
        final String TrainDataSetPath = args[0];
        // final String TestDataSetPath = args[1];
        int JobexitCode = 0;
        BayesTools.CheckOutputPath(Job1_OutputPath);//检查Job1输出路径是否为空
        JobexitCode += ToolRunner.run(new Job1(), new String[]{TrainDataSetPath, Job1_OutputPath});
        //int Job1exitCode = 0;
        // System.exit(Job1exitCode);// 当参数为0时表示正常终止JVM，为非0时表示异常终止


        BayesTools.CheckOutputPath(Job2_OutputPath);//检查Job2输出路径是否为空
        JobexitCode += ToolRunner.run(new Job2(), new String[]{TrainDataSetPath, Job2_OutputPath});
        //int Job2exitCode = 0;
        //System.exit(Job2exitCode);// 当参数为0时表示正常终止JVM，为非0时表示异常终止

        BayesTools.CheckOutputPath(Job3_OutputPath);//检查Job2输出路径是否为空
        JobexitCode += ToolRunner.run(new Job3(), new String[]{Job1_OutputPath, Job3_OutputPath});

        System.exit(JobexitCode);// 当参数为0时表示正常终止JVM，为非0时表示异常终止

        // 使用训练好的模型进行分类
        // BayesTools.BayesClassifier(TestDataSetPath);
        // 将分类器建模和分类器测试分为两个程序比较好，就不放在这里运行了

    }
}
