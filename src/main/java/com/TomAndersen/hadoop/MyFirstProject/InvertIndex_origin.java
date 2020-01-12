package com.TomAndersen.hadoop.MyFirstProject;

import java.io.IOException;
import java.util.HashMap;
import java.util.StringTokenizer;

import com.TomAndersen.hadoop.HDFSTools.BayesTools;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class InvertIndex_origin {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        private Text keyInfo = new Text(); // 存储单词和URL组合
        private Text valueInfo = new Text(); // 存储词频
        private FileSplit split; // 存储Split对象

        // 实现map函数
        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            // 获得<key,value>对所属的FileSplit对象
            split = (FileSplit) context.getInputSplit();
            StringTokenizer itr = new StringTokenizer(value.toString());
            String fileName = split.getPath().getName();// 通过分片Split获取文件名
            while (itr.hasMoreTokens()) {
                // key值由单词和文件名组成, value 值初始化为 1. 组成key-value对:
                // 如: (MapReduce:file1.txt, 1)
                /**********Begin**********/
                // 创建Key
                keyInfo.set(itr.nextToken() + ":" + fileName);
                /*// 创建Value
                valueInfo.set("1");*/
                // 将Key-Value压入到context中
                context.write(keyInfo, valueInfo);
                /**********End**********/
            }
        }
    }

    public static class Combine extends Reducer<Text, Text, Text, Text> {
        private Text info = new Text();

        // 实现reduce函数， 将相同key值的value加起来
        // 并将(单词:文件名, value) 转换为 （单词， 文件名:value）
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /**********Begin**********/
            // 存储ValueOut
            Text valueOut = new Text();
            // 统计词频
            int count = 0;
            for (Text text : values) {
                count++;
            }
            // 重新设置value值由URL和词频组成
            String[] words = key.toString().split(":");
            String fileName = words[words.length - 1];
            valueOut.set(fileName + ":" + count);
            // 重新设置key值为单词
            // 为了避免前面单词中出现":"，在这里对String数组前面的String进行组合
            StringBuilder stringBuilder = new StringBuilder();
            for (int i = 0; i < words.length - 1; i++) {
                stringBuilder.append(words[i]);
            }
            info.set(stringBuilder.toString());
            context.write(info, valueOut);
            /**********End**********/

        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        private Text result = new Text();

        // 实现reduce函数, 将相同单词的value聚合成一个总的value，每个value之间用`;`隔开, 最后以`;`结尾
        @Override
        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            /**********Begin**********/
            // 为了避免当文件过大时使用的是不同的Mapper处理的同一个文件，从而导致后来的Combiner处理之后在ValuesIn
            // 中可能存在文件名重复的情况，这个时候就需要辨别，所以在这里使用HashMap
            HashMap<String, Integer> hashMap = new HashMap<>();
            StringBuilder stringBuilder = new StringBuilder();
            Text valueOut = new Text();
            for (Text valueIn : values) {
                String[] words = valueIn.toString().split(":");
                Integer count = hashMap.get(words[0]);

                if (count != null) {
                    Integer sum = Integer.valueOf(words[1]) + count;
                    hashMap.put(words[0], sum);
                } else hashMap.put(words[0], Integer.valueOf(words[1]));
            }

            for (String fileName : hashMap.keySet()) {
                stringBuilder.append(fileName).append(":").append(hashMap.get(fileName)).append(";");
            }
            valueOut.set(stringBuilder.toString());
            /**********End**********/
            context.write(key, valueOut);

        }
    }

    public static void main(String[] args) throws Exception {
        // 第一个参数为 输入文件目录路径， 第二个参数为输出结果路径
        Configuration conf = new Configuration();

        if (args.length != 2) {
            System.err.println("Usage: Inverted Index <in> <out>");
            System.exit(2);
        }

        Job job = new Job(conf, "Inverted Index");
        job.setJarByClass(InvertIndex_origin.class);

        // 设置Map、Combine和Reduce处理类
        job.setMapperClass(Map.class);
        job.setCombinerClass(Combine.class);
        job.setReducerClass(Reduce.class);

        // 设置Map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        // 设置Reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        // 设置输入和输出目录
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // 检查路径是否存在，若存在则删除，记得提交作业的将本行代码删除，否则不识别
        BayesTools.CheckOutputPath(args[1]);

        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);

    }
}

