package com.TomAndersen.hadoop.BayesClassification;

import com.TomAndersen.hadoop.HDFSTools.CombineSmallfileInputFormat;
import com.TomAndersen.hadoop.HDFSTools.WholeFileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @Author
 * @Version
 * @Date 2019/11/4
 * @Description Job1：统计出各类文档中各种单词的数目
 * Job1:
 * 将一个文档作为一个记录处理
 * 输入路径：训练集
 * 输出路径：自定义Job1输出路径，如：/OutPut/Job1/
 * Mapper：
 * 输入：<文档名,整个文档记录>，输出：<文档类别-单词，1>
 * 其中文档类别通过读取文件名前缀获得，单词通过对text划分可得
 * Conbiner：即Reducer
 * Reducer：
 * 输入：<文档类别-单词，{1,1,1,1,1,1……}>，输出：<文档类别-单词，sum>
 */

public class Job1 extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        // 重写run方法，外部使用ToolRunner启动Job
        // 第一个参数为训练集，第二个参数为输出路径
        String InputPath = args[0];
        String OutputPath = args[1];
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, this.getClass().getName());

        // 设置输入格式
        //job.setInputFormatClass(WholeFileInputFormat.class);//使用整个文件内容作为记录输入
        //job.setInputFormatClass(CombineSmallfileInputFormat.class);//使用整合小文件block的方式输入
        job.setInputFormatClass(SequenceFileInputFormat.class);//使用SequenceFile作为输入

        job.setJarByClass(Job1.class);//设置主类

        job.setMapperClass(Job1Mapper.class);// 设置Mapper
        job.setCombinerClass(Job1Reducer.class);// 设置Combiner
        job.setReducerClass(Job1Reducer.class);// 设置Reducer

        // 默认使用的OutputFormat是TextOutputFormat，使用时一定要指定Map输出的Key-Value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置job输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置job输出的value类型
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(InputPath));// 添加输入路径
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));// 设置输出路径


        return job.waitForCompletion(true) ? 0 : 1;
    }

    //Mapper
    public static class Job1Mapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        private final static Text KEYOUT = new Text();//预定义KeyOut
        private final static IntWritable VALUEOUT = new IntWritable(1);//预定义ValueOut

        @Override
        public void map(Text KeyIn, BytesWritable ValueIn, Context context)
                throws IOException, InterruptedException {//KeyIn是文档名，ValueIn是文档内容
            // Text, BytesWritable, Text, IntWritable输入输出键值对类型
            // 将文档内容按照回车分割，即分割成一个个单词
            String[] contents = new String(ValueIn.copyBytes()).split("\r\n|\n|\r");
            // 以各种系统的换行形式以及Unicode字符的null作为分隔符进行切分
            // 不知道为什么原数据集中那么多Unicode的空
            // （之前之所以产生了这么多Unicode空是因为读取BytesWritable时使用的是getBytes而非copyBytes，存在错误）

            /*// 也可以使用以下方式获取文件名，但这样就无法使用CombineInputFormat
            // 因为CombineFileSplit无法转换成FileSplit
            FileSplit fileSplit = (FileSplit) context.getInputSplit();
            String filename = fileSplit.getPath().getName();
            String fileClass = filename.split("-")[0];*/

            // 获取文档类别，文档名中已有类别戳
            String fileClass = KeyIn.toString().split("-")[0];


            for (String word : contents) {
                // 对每个单词加上类别名
                KEYOUT.set(fileClass + "\t" + word);//设置KeyOut，使用制表符来间隔文档类别和单词
                //输出格式为<类别名 单词，“1”>
                context.write(KEYOUT, VALUEOUT);
            }

        }
    }

    //Reducer
    public static class Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static IntWritable VALUEOUT = new IntWritable();//预定义ValueOut

        @Override
        public void reduce(Text KeyIn, Iterable<IntWritable> ValuesIn, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable Value : ValuesIn) {
                sum += Value.get();// 要使用combiner就不能只是单纯的计数+1，而应该是取值相加
            }
            // 输出<类别名  单词，单词总数>
            VALUEOUT.set(sum);
            context.write(KeyIn, VALUEOUT);

        }
    }

}




