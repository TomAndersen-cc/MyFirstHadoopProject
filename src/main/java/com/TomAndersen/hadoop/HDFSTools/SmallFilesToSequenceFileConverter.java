package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import org.apache.hadoop.util.Tool;

import java.io.IOException;


/**
 * @Author TomAndersen
 * @Date 2019/11/20
 * @Version
 * @Description
 */
public class SmallFilesToSequenceFileConverter extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 2) {
            System.out.println("参数数量设置错误");
            return -1;
        }
        // 获取配置信息
        Configuration configuration = new Configuration();
        // 创建Job
        Job job = Job.getInstance(configuration, this.getClass().getName());
        // 使用整个文件作为记录输入，并将小文件尽量整合成一个Split
        job.setInputFormatClass(CombineSmallfileInputFormat.class);

        // 使用自带的SequenceFileOutputFormat进行输出生成Sequence
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        // 当然也可以将这段代码注释掉，不使用自带的SequenceFileOutputFormat，即不利用SequenceFile的压缩策略
        // 如果设置成TextOutputFormat，这样就只是利用了小文件合并的思想，而并不是SequenceFile

        // 设置最终的输出Key类型
        job.setOutputKeyClass(Text.class);
        // 设置最终输出的Value类型
        job.setOutputValueClass(BytesWritable.class);
        // 设置Job采用的Mapper和Reducer
        job.setMapperClass(SequenceFileMapper.class);
        // 也可以不用设置Reducer，会采用默认的Reducer原样输出，反正都写了就设置一下吧
        job.setReducerClass(SequenceFileReducer.class);
        // 设置输入输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 等待Job执行完成返回执行状态
        return job.waitForCompletion(true) ? 0 : 1;

    }

    static class SequenceFileMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
        // 因为在WholeFileInputFormat中已经设置为传入的格式为<文件名，文件内容>
        // 所以在这里直接原样压入即可
        @Override
        protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
            context.write(key, value);
        }
    }

    static class SequenceFileReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
        // 同样的Reducer也只是原样输出不作变换
        @Override
        protected void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
            for (BytesWritable value : values) {
                context.write(key, value);
            }
        }
    }
}
