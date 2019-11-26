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
 * 在Job3的setup函数中读取Job2的输出路径下的HDFS文档，将其中的文档类别和单词总数作为键值对压入Job3类的静态
 * HashMap中，读入的文档名为含有"part"，可以使用正则表达式进行匹配。
 * （改：直接读取Job2输出的part文档，因为分类最多也不会超出Reducetask设置的默认阈值上限，保险起见手动设置
 * ReduceTask的数量，保证输出的part文件只有一个，直接读取指定文件，而不读取文件夹路径，读取文件夹路径还需
 * 要进一步判断	，十分麻烦）
 *
 * 输入路径：Job1的输出路径（MapReduce程序会自动忽略以下划线"_"开头的文件，不用担心_success文件）
 * 输出路径：自定义Job3输出路径，如：/OutPut/Job3/
 * Mapper：
 * 输入：<nullwritable，<文档类别 单词 sum>>
 * 输出：<文档类别 单词，条件概率p>
 * 在Mapper中读取对应文档类别的单词总数，计算单词出现概率
 * No Reducer
 */
public class Job3 extends Configured implements Tool {

    // public static HashMap fileClassToSumOfWords = null;// 存放文档类别到单词总数的映射，键值对类型为<String,String>
    // public static HashMap fileClassToSumOfFiles = null;// 存放文档类别到文档总数的映射

    @Override
    public int run(String[] args) throws Exception {

        // 获取配置信息
        Configuration configuration = new Configuration();
        //下面是对Job3的配置
        String InputPath = args[0]; // 输入路径为Job1的输出路径
        String OutputPath = args[1];// 输出路径
        // 获取Job实例
        Job job = Job.getInstance(configuration, this.getClass().getName());

        // 设置输入格式
        job.setInputFormatClass(TextInputFormat.class);
        // 设置Job3的主类
        job.setJarByClass(Job3.class);
        // 设置Mapper
        job.setMapperClass(Job3Mapper.class);
        // No Combiner,no Reducer，使用默认的Reducer
        job.setNumReduceTasks(1);// 设置单个ReduceTask，确保输出文件只有一个，便于后面的Job进行读取
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

    public static class Job3Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

        private final static Text KEYOUT = new Text();//预定义KeyOut，避免多次New
        private final static DoubleWritable VALUEOUT = new DoubleWritable();//预定义ValueOut，避免多次New
        private static HashMap fileClassToSumOfWords = null;// 存放文档类别到单词总数的映射，键值对类型为<String,String>

        // 重载setup方法，在setup方法中初始化fileClassToSumOfWords，setup方法每次启动Task时由run方法调用，只执行一次
        @Override
        public void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            Configuration configuration = context.getConfiguration();
            HashMap[] myMaps = BayesTools.getKeyValuesByReadFile(JobsInitiator.Job2_OutputPath + "part-r-00000",
                    configuration, "\t");
            fileClassToSumOfWords = myMaps[0];
        }

        // 输入：<NullWritable，<文档类别   单词  单词总数>>
        // 输出：<文档类别 单词  条件概率>
        @Override
        public void map(LongWritable KeyIn, Text ValueIn, Context context)
                throws IOException, InterruptedException {
            // 对输入的一行文本数据按制表符 \t 进行分割
            // ValueIn格式为<文档类别  单词  单词总数>，KeyIn为记录偏移量
            String[] ValuesIn = ValueIn.toString().split("\t");
            String fileClass = ValuesIn[0];// 获取文档类别
            String word = ValuesIn[1];// 获取当前单词
            Double sumOfWords = Double.valueOf(ValuesIn[2]);// 获取当前单词总数
            // 获取对应类别所有单词的总数
            Double sumOfClassWord = Double.valueOf((String)
                    Job3Mapper.fileClassToSumOfWords.get(fileClass));
            // 计算单词的条件概率
            // 设置输出的Key值，为<文档类别-单词>，虽然实现了TextPair，为了便于后续读取还是直接用“-”分割
            KEYOUT.set(fileClass + "-" + word);
            // 设置输出的Value值，为单词条件概率
            VALUEOUT.set(sumOfWords / sumOfClassWord);
            context.write(KEYOUT, VALUEOUT);
        }
    }
}
