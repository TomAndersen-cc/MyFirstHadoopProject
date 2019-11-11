package com.TomAndersen.hadoop.BayesClassification;

import com.TomAndersen.hadoop.HDFSTools.CombineSmallfileInputFormat;
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
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

/**
 * @Author
 * @Version
 * @Date 2019/11/8
 * Job2：
 * 输入路径：训练集
 * 输出路径：自定义Job2输出路径，如：/OutPut/Job2/
 * Mapper：
 * 输入：<文档名,整个文档>，输出：<文档类别，本文档中单词总数sum>
 * Reducer：
 * 输入：<文档类别，{sum1，sum2……}>，输出：<文档类别 本类文档单词总数，本类文档个数>
 * 其中“本类文档个数”由sum的个数来计算，每有一个sum则说明有一个对应的文档属于当前类别，因为是一个文档作为一个记录进行处理
 */
public class Job2 extends Configured implements Tool {


    @Override
    public int run(String[] args) throws Exception {
        // 重写run方法，外部使用ToolRunner启动Job

        // 第一个参数为训练集，第二个参数为输出路径
        String InputPath = args[0];
        String OutputPath = args[1];

        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, this.getClass().getName());

        // 设置输入格式,使用整合小文件block的方式输入
        job.setInputFormatClass(CombineSmallfileInputFormat.class);

        job.setJarByClass(Job2.class);//设置主类

        job.setMapperClass(Job2Mapper.class);// 设置Mapper
        // 不能设置combiner
        job.setReducerClass(Job2Reducer.class);// 设置Reducer

        // TextOutputFormat 默认是Key是LongWritable类型，Value是Text类型
        // 当自定义InputFormat时一定也要自定义Map输出键值对类型，否则会使用默认类型从而报错
        // 设置Map输出Key Value类型
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

    public static class Job2Mapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        private final static Text KEYOUT = new Text();//预定义KeyOut，避免多次New
        private final static IntWritable VALUEOUT = new IntWritable();//预定义ValueOut，避免多次New

        @Override
        public void map(Text KeyIn, BytesWritable ValueIn, Context context)
                throws IOException, InterruptedException {
            // 对整个文档内容按回车、换行、空字符进行分割，分割产生的碎片数量即为本文档中单词总数
            String[] contents = new String(ValueIn.getBytes()).split("\r\n|\n|\r|\u0000+");
            // 本文档单词总数作为ValueOut
            VALUEOUT.set(contents.length);

            // 文件名中包含类别信息，需要分离出来
            // 获取文档类别，文档名中已有类别戳
            String fileClass = KeyIn.toString().split("-")[0];
            // 文档类别作为KeyOut
            KEYOUT.set(fileClass);
            context.write(KEYOUT, VALUEOUT);

        }
    }


    public static class Job2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private final static Text KEYOUT = new Text();// 预定义KeyOut
        private final static IntWritable VALUEOUT = new IntWritable();// 预定义ValueOut

        //不能使用combiner
        public void reduce(Text KeyIn, Iterable<IntWritable> ValuesIn, Context context)
                throws IOException, InterruptedException {
            // 本类别文档单词总数
            int sumOfWords = 0;
            // 本类别文档总数
            int sumOfFiles = 0;
            for (IntWritable value : ValuesIn) {
                //每有一个value则有一个此类别文档
                sumOfWords += value.get();
                sumOfFiles += 1;
            }
            // KeyOut为<文档类别 本类单词总数>,KeyIn为文档类别
            String fileClass = KeyIn.toString();
            // 以制表符 \t 作为分隔符
            KEYOUT.set(fileClass + "\t" + sumOfWords);

            // ValueOut为本类文档个数
            VALUEOUT.set(sumOfFiles);

            context.write(KEYOUT, VALUEOUT);
        }
    }
}
