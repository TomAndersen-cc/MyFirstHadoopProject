package com.TomAndersen.hadoop.HDFSTools;

import com.TomAndersen.hadoop.BayesClassification.JobsInitiator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.ToolRunner;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 用于测试工具包中的各个方法是否可用
 */
public class ToolTestDemo {
    public static void main(String[] args) throws Exception {
        // 测试CheckOutputPath方法
        /*String uri = "D:/TestData/Document1";
        // 记得使用String创建URI里面不能包含特殊字符，不能使用转义字符的形式如：D:\\TestData\\Document1
        // 而应该写成/的形式，如：D:/TestData/Document1
        BayesTools.CheckOutputPath(uri);*/

        /*// 测试getKeyValuesByReadFile方法
        Configuration configuration = new Configuration();
        HashMap[] myMaps = null;
        String filePath = JobsInitiator.Job2_OutputPath + "part-r-00000";
        myMaps = BayesTools.getKeyValuesByReadFile(filePath, configuration, "\t");
        for (HashMap map : myMaps) {
            System.out.println(map);
        }*/

        /*FSDataInputStream fsDataInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        String fileLine = null;
        String[] fileText = null;

        try {
            FileSystem fs = FileSystem.get(configuration);
            fsDataInputStream = fs.open(new Path(filePath));
            inputStreamReader = new InputStreamReader(fsDataInputStream);
            bufferedReader = new BufferedReader(inputStreamReader);
            while (bufferedReader.ready()) {
                fileLine = bufferedReader.readLine();
                fileText = fileLine.split("\r\n|\n|\r|\u0000+");
                System.out.println(Arrays.toString(fileText));
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }*/


        /*// 尝试一次性读取HDFS文件中所有内容(测试成功)
        String remoteSrc = JobsInitiator.Job1_OutputPath + "/part-r-00000"; // 设置读取路径
        Configuration configuration = new Configuration(); // 获取配置信息
        FileSystem fs = FileSystem.get(configuration); // 获取配置信息对应的文件系统
        FileStatus fileStatus = fs.getFileStatus(new Path(remoteSrc));

        InputStream in = null; // 打开文件获取输入流
        try {
            in = fs.open(fileStatus.getPath());
            // 方式1：直接将输入流拷贝到系统输出流中
            // IOUtils.copyBytes(in, System.out, fileStatus.getLen(), false);
            // 方式2：将输入流拷贝到缓冲区，然后输出
            byte[] buffer = new byte[(int) fileStatus.getLen()];
            IOUtils.readFully(in, buffer, 0, buffer.length);
            System.out.println(new String(buffer));//

        } finally {
            IOUtils.closeStream(in);
        }
        System.out.println();*/

        /*// 测试SequenceFile写入是否正常运行（测试成功，原类已经更改，此测试无效）
        String TrainSetInputPath = "src/Input/TrainSet1";
        String SequnceFileOutputPath = "src/Output/SequenceFile/TrainSet1";
        BayesTools.CheckOutputPath(SequnceFileOutputPath);//检查SequenceFile输出路径是否为空
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileConverter(),
                new String[]{TrainSetInputPath, SequnceFileOutputPath});
        System.exit(exitCode);*/

        // 测试BayesClassifier，对整个测试集进行分类（测试成功）
        // 测试集路径
        String TestSetPath = args[0];
        // 启动分类器
        BayesTools.BayesClassifier(TestSetPath);



        /*// 测试PPT上的实例文档分类过程（测试结果正确）
        // 获取配置信息
        Configuration configuration = new Configuration();
        // 获取文件路径
        Path path = new Path("src/Input/TestSet1/555.txt");
        BayesTools.Init(JobsInitiator.Job2_OutputPath, JobsInitiator.Job3_OutputPath, configuration);
        System.out.println("p(c=yes|d5): " + BayesTools.getRelativePosibility(path, "yes", configuration));
        System.out.println("p(c=no|d5): " + BayesTools.getRelativePosibility(path, "no", configuration));*/


    }
}

