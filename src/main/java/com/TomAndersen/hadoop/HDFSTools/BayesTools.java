package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 用于判断HDFS中某路径下是否已经存在文件夹，若存在则清空文件夹内文件，以免每次测试都需要手动删除文件夹内容
 */
public class BayesTools {
    // demo测试通过
    public static void CheckOutputPath(String outputPath) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outputPath);

        if (fs.exists(outPath)) {
            /*fs.deleteOnExit(outPath);// 此方法为在程序退出时删除，应该是在当场就删除
            fs.close();*/
            fs.delete(outPath, true);
            fs.close();

            System.out.println("Delete " + outputPath + " succeed!");
        } else {
            System.out.println(outputPath + " does not exist!");
        }
    }
}
