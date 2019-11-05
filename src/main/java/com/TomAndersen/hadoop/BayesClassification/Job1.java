package com.TomAndersen.hadoop.BayesClassification;

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
 * @Date 2019/11/4
 * Job1：统计出各类文档中各种单词的数目
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
    private final static Text KEYOUT = new Text();
    private final static IntWritable VALUEOUT = new IntWritable(1);

    class Job1Mapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
        @Override
        public void map(Text KeyIn, BytesWritable ValueIn, Context context)
                throws IOException, InterruptedException {//KeyIn是文档名，ValueIn是文档内容
            // Text, BytesWritable, Text, IntWritable输入输出键值对类型
            // 将文档内容按照回车分割，即分割成一个个单词
            String[] contents = new String(ValueIn.getBytes()).split("\\n");
            // 获取文档类别，文档名中已有类别戳
            String fileClass = KeyIn.toString().split("-")[0];

            for (String word : contents) {
                // 对每个单词加上类别名
                KEYOUT.set(fileClass + "-" + word);//设置KeyOut
                context.write(KEYOUT, VALUEOUT);
            }
        }
    }

    class Job1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

        @Override
        public void reduce(Text KeyIn, Iterable<IntWritable> ValuesIn, Context context)
                throws IOException, InterruptedException {

            int sum = 0;
            for (IntWritable Value : ValuesIn) {
                sum += Value.get();
            }
            // 输出<文档类别-单词，单词总数>
            context.write(KeyIn, new IntWritable(sum));
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        // 重写run方法，外部使用ToolRunner启动Job
        // 第一个参数为训练集，第二个参数为输出路径
        String InputPath = args[0];
        String OutputPath = args[1];
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration, this.getClass().getName());

        job.setInputFormatClass(WholeFileInputFormat.class);

        job.setJarByClass(Job1.class);// 设置Job运行主类
        job.setMapperClass(Job1Mapper.class);// 设置Mapper
        job.setCombinerClass(Job1Reducer.class);// 设置Combiner
        job.setReducerClass(Job1Reducer.class);// 设置Reducer

        FileInputFormat.addInputPath(job, new Path(InputPath));// 添加输入路径
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));// 设置输出路径

        return job.waitForCompletion(true) ? 0 : 1;
    }

}
