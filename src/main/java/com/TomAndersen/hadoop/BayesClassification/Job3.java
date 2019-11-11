package com.TomAndersen.hadoop.BayesClassification;

import com.TomAndersen.hadoop.HDFSTools.BayesTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
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

    private static HashMap fileClassToSumOfWords = null;// 存放文档类别到单词总数的映射，键值对类型为<String,String>
    // public static HashMap fileClassToSumOfFiles = null;// 存放文档类别到文档总数的映射

    @Override
    public int run(String[] args) throws Exception {

        Configuration configuration = new Configuration();
        // 读取Job2的输出文件，获取文档类别到单词总数的映射
        HashMap[] myMaps = BayesTools.getKeyValuesByReadFile(JobsInitiator.Job2_OutputPath, configuration);

        fileClassToSumOfWords = myMaps[0];// 因为知道文档输出类型及内容，所以直接取第一个Map即可
        // fileClassToSumOfFiles = myMaps[1];

        //下面是对Job3的配置
        String InputPath = args[0]; // 输入路径为Job1的输出路径
        String OutputPath = args[1];// 输出路径

        Configuration configuration1 = new Configuration();
        Job job = Job.getInstance(configuration, this.getClass().getName());

        // 设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        // 设置Job3的主类
        job.setJarByClass(Job3.class);
        // 设置Mapper
        job.setMapperClass(Job3Mapper.class);
        // No Combiner,no Reducer
        // 设置Mapper的输出Key-value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);
        // 添加输入路径
        FileInputFormat.addInputPath(job, new Path(InputPath));
        // 设置输出路径
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));
        // 返回执行状态
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static class Job3Mapper extends Mapper<NullWritable, Text, Text, DoubleWritable> {

        private final static Text KEYOUT = new Text();//预定义KeyOut，避免多次New
        private final static DoubleWritable VALUEOUT = new DoubleWritable();//预定义ValueOut，避免多次New

        // 输入：<NullWritable，<文档类别   单词  单词总数>>
        // 输出：<文档类别 单词  条件概率>
        public void map(NullWritable KeyIn, Text ValueIn, Context context)
                throws IOException, InterruptedException {

            // 对输入的一行文本数据按制表符 \t 进行分割
            // KeyIn格式为<文档类别 单词  单词总数>，ValueIn为空
            String[] ValuesIn = ValueIn.toString().split("\t");
            String fileClass = ValuesIn[0];// 获取文档类别
            String word = ValuesIn[1];// 获取当前单词
            Double sumOfWords = Double.valueOf(ValuesIn[2]);// 获取当前单词总数
            // 获取对应类别所有单词的总数
            Double sumOfClassWord = Double.valueOf((String)
                    Job3.fileClassToSumOfWords.get(fileClass));
            // 计算单词的条件概率
            // 设置输出的Key值，为<文档类别 单词>
            KEYOUT.set(fileClass + "\t" + word);
            // 设置输出的Value值，为单词条件概率
            VALUEOUT.set(sumOfWords / sumOfClassWord);
            context.write(KEYOUT, VALUEOUT);
        }
    }
}
