package com.TomAndersen.hadoop.MyFirstProject;


import com.TomAndersen.hadoop.HDFSTools.BayesTools;
import com.TomAndersen.hadoop.HDFSTools.CombineSmallfileInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.io.Text;


/**
 *
 */
public class WordCount {
    //static enum CounterNums {INPUT_WORDS}
    public static void main(String[] args) throws Exception {
        //源文件输入路径，就是读取的本地文本文件的路径
        String InputPath = args[0];
        //经过MapReduce框架处理后输出的文件路径
        String OutPutPath = args[1];
        //检查输出路径，如果输出路径已经存在则删除
        BayesTools.CheckOutputPath(args[1]);
        //获取MapReduce运行配置
        Configuration conf = new Configuration();

        /*conf.setInt("mapreduce.map.memory.mb", 256);
        conf.setInt("mapreduce.job.maps", 4);*/

        //创建MapReduce的job对象
        Job job = Job.getInstance(conf, WordCount.class.getName());
        //设置job运行时的程序入口主类WordCount
        job.setJarByClass(WordCount.class);
        //-----------------------------------------
        //通过job设置输入/输出格式为文本格式，我们目前操作的基本都是文本类型
        //job.setInputFormatClass(TextInputFormat.class);

        //使用CombineTextInputFormat将小文件聚集成一个Split，使用一个Map进行处理
        job.setInputFormatClass(CombineSmallfileInputFormat.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        //-----------------------------------------
        //设置map函数的实现类对象
        job.setMapperClass(TokenizerMapper.class);
        //设置combiner，作用是在mapper之后先将键值对进行一个简单的整合，减少mapper和reducer之间的数据传输量
        //注意combiner相当于一个局部的reducer，这样整体的程序就相当于进行了两次reduce，在有些业务中
        //不允许进行两次reduce，例如求平均值等
        job.setCombinerClass(IntSumReducer.class);
        //设置reduce函数的实现类对象
        job.setReducerClass(IntSumReducer.class);
        //设置job输出的key类型
        job.setOutputKeyClass(Text.class);
        //设置job输出的value类型
        job.setOutputValueClass(IntWritable.class);



        //设置输入文件的路径
        FileInputFormat.addInputPath(job, new Path(InputPath));
        //设置输出文件的路径
        FileOutputFormat.setOutputPath(job, new Path(OutPutPath));
//      //提交运行job
        int num = job.waitForCompletion(true) ? 0 : 1;
        //根据job执行返回的结果退出程序
        System.exit(num);
        //or //System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}
