package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.Arrays;
import java.util.HashMap;

/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 用于测试工具包中的函数是否可用
 */
public class ToolTestDemo {
    public static void main(String[] args) throws IOException {
        // 测试CheckOutputPath方法
        /*String uri = "D:/TestData/Document1";
        // 记得使用String创建URI里面不能包含特殊字符，不能使用转义字符的形式如：D:\\TestData\\Document1
        // 而应该写成/的形式，如：D:/TestData/Document1
        BayesTools.CheckOutputPath(uri);*/

        // 测试getKeyValuesByReadFile方法
        Configuration configuration = new Configuration();
        HashMap[] myMaps = null;
        String filePath = "C:/Users/DELL/Desktop/HadoopProjects/Job2-part-r-00000";
        myMaps = BayesTools.getKeyValuesByReadFile(filePath, configuration);
        for (HashMap map : myMaps) {
            System.out.println(map);
        }
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

    }
}

