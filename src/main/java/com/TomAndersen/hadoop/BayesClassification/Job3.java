package com.TomAndersen.hadoop.BayesClassification;

import com.TomAndersen.hadoop.HDFSTools.BayesTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.Tool;

import java.io.IOException;
import java.util.HashMap;

/**
 * @Author
 * @Version
 * @Date 2019/11/8
 * Job3：
 * 在Job3的run函数中读取Job2的输出路径下的HDFS文档，将其中的文档类别和单词总数作为键值对压入Job3
 * 类的静态HashMap中，读入的文档名为含有"part"，使用正则表达式进行匹配。即获取到了
 * <文档类别><单词总数>以及<文档类别><文档总数>的两个HashMap<String,String>
 * 输入路径：Job1的输出路径
 * 输出路径：自定义Job3输出路径，如：/OutPut/Job3/
 * Mapper：
 * 输入：<LongWritable，<文档类别   单词  sum>>
 * 输出：<文档类别 单词，概率p>
 * 在Mapper中读取对应文档类别的单词总数，计算单词出现概率
 * No Reducer
 */
public class Job3 extends Configured implements Tool {

    public static HashMap fileClassToSumOfWords = null;// 存放文档类别到单词总数的映射
    // public static HashMap fileClassToSumOfFiles = null;// 存放文档类别到文档总数的映射

    @Override
    public int run(String[] strings) throws Exception {
        Configuration configuration = new Configuration();
        HashMap[] myMaps = BayesTools.getKeyValuesByReadFile(JobsInitiator.Job2_OutputPath, configuration);
        fileClassToSumOfWords = myMaps[0];
        // fileClassToSumOfFiles = myMaps[1];

        //下面是对Job3的配置

        return 0;
    }

    public static class Job3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final static Text KEYOUT = new Text();//预定义KeyOut，避免多次New
        private final static DoubleWritable VALUEOUT = new DoubleWritable();//预定义ValueOut，避免多次New

        public void map(LongWritable KeyIn, Text ValueIn, Context context)
                throws IOException, InterruptedException {

            // 对输入的一行文本数据进行分割，KeyIn格式为<文档类别 单词  单词总数>,ValueIn没niao用
            String[] ValuesIn = ValueIn.toString().split("\t");
            String fileClass = ValuesIn[0];// 获取文档类别
            String word = ValuesIn[1];// 获取当前单词
            Double sumOfWord = Double.valueOf(ValuesIn[2]);// 获取当前单词总数
            // 获取对应类别所有单词的总数
            Double sumOfClassWord = Double.valueOf((String)
                    Job3.fileClassToSumOfWords.get(fileClass));
            // 计算单词的条件概率
            VALUEOUT.set(sumOfWord / sumOfClassWord);// 设置输出的Value值，为单词条件概率
            KEYOUT.set(fileClass + "\t" + word);// 设置输出的Key值，为<文档类别 单词>
            context.write(KEYOUT, VALUEOUT);
        }
    }
}
