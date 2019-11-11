package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;


/**
 * @Author
 * @Version
 * @Date 2019/11/5
 */
public class BayesTools {
    // demo测试通过
    // 用于判断HDFS中某路径下是否已经存在文件夹，若存在则清空文件夹内文件，以免每次测试都需要手动删除文件夹内容
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


    // 读取指定文档中的每一列，将第一列作为Key值，其他列作为Value，返回HashMap数组
    // 反正之后都要转换成其他类型，索性全都读取成String类型
    // demo测试通过
    public static HashMap[] getKeyValuesByReadFile(String filePath, Configuration conf,String separator)
            throws IOException {

        FSDataInputStream fsDataInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        String fileLine = null;
        String[] fileText = null;
        HashMap<String, String>[] multipleKeyValues = null;
        HashMap<String, String> KeyValues = null;

        try {

            FileSystem fs = FileSystem.get(conf);
            fsDataInputStream = fs.open(new Path(filePath));
            inputStreamReader = new InputStreamReader(fsDataInputStream);
            bufferedReader = new BufferedReader(inputStreamReader);

            while (bufferedReader.ready()) {
                fileLine = bufferedReader.readLine();
                fileText = fileLine.split(separator);
                if (multipleKeyValues == null) multipleKeyValues = new HashMap[fileText.length - 1];
                for (int i = 1, fileTextLength = fileText.length; i < fileTextLength; i++) {
                    String text = fileText[i];
                    if (multipleKeyValues[i - 1] != null) {
                        multipleKeyValues[i - 1].put(fileText[0], text);
                    } else {
                        KeyValues = new HashMap<>();
                        KeyValues.put(fileText[0], text);
                        multipleKeyValues[i - 1] = KeyValues;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
        return multipleKeyValues;
    }
}
