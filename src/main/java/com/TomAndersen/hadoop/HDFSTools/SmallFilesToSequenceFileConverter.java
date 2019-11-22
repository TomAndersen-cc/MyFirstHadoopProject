package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;


import java.io.IOException;


/**
 * @Author TomAndersen
 * @Date 2019/11/20
 * @Version
 * @Description
 */
public class SmallFilesToSequenceFileConverter {


    public static void main(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("参数数量设置错误");
            System.exit(-1);
        }
        // 获取输入输出路径
        String InputPath = args[0];
        String OutputPath = args[1];
        // 检查输出路径
        BayesTools.CheckOutputPath(OutputPath);

        // 获取配置信息
        Configuration configuration = new Configuration();
        // 创建Job实例
        Job job = Job.getInstance(configuration, SmallFilesToSequenceFileConverter.class.getName());

        // 设置输入格式
        //job.setInputFormatClass(WholeFileInputFormat.class);//使用整个文件内容作为记录输入
        job.setInputFormatClass(CombineSmallfileInputFormat.class);// 使用整合小文件block的方式输入

        // 使用自带的SequenceFileOutputFormat进行输出生成Sequence
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 当然也可以将这段代码注释掉，不使用自带的SequenceFileOutputFormat，即不利用SequenceFile的压缩策略
        // 如果设置成TextOutputFormat，这样就只是利用了小文件合并的思想，而并不是SequenceFile

        // 设置最终的输出Key类型
        job.setOutputKeyClass(Text.class);
        // 设置最终输出的Value类型
        job.setOutputValueClass(BytesWritable.class);
        // 设置主类
        job.setJarByClass(SmallFilesToSequenceFileConverter.class);//设置主类，必须要设置否则集群中不识别类
        // 设置Job采用的Mapper和Reducer
        job.setMapperClass(SequenceFileMapper.class);
        // 也可以不用设置Reducer，会采用默认的Reducer原样输出，反正都写了就设置一下吧
        job.setReducerClass(SequenceFileReducer.class);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(InputPath));
        FileOutputFormat.setOutputPath(job, new Path(OutputPath));

        // 等待job执行完成
        int exitcode = job.waitForCompletion(true) ? 0 : 1;
        System.exit(exitcode);

    }

    public static class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
        // 因为在WholeFileInputFormat中已经设置为传入的格式为<文件名，文件内容>
        // 所以在这里直接原样压入即可
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    public static class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
        // 同样的Reducer也只是原样输出不作变换
        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            for (BytesWritable value : values) {
                context.write(key, value);
            }
        }
    }
}
