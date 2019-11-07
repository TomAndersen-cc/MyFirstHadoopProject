package com.TomAndersen.hadoop.MyFirstProject;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * 本类继承自Reducer类，负责重写父类中的Reduce函数
 * Reducer<Text,IntWritable,Text,IntWritable>泛型的参数分别表示reduce函数输入和输出的键值对类型
 * reduce函数的参数列表中只传入输入的键值对和上下文，输出的键值对在函数中处理
 */
public class IntSumReducer
        extends Reducer<Text, IntWritable, Text, IntWritable> {
    /**
     * reduce函数主要负责对map函数处理后的中间结果做最后的处理
     * 参数KeyIn是map函数处理完后输出的中间结果键值对的键值
     * 参数ValuesIn是map函数处理完后输出的中间结果值对应的列表
     * 因为在MapReduce框架中会自动对map处理后的键值对针对键值进行分组，同一个组中的value组成一个集合，原来的word和对应的集合形成
     * 新的键值对<word,values>作为中间结果，即reduce函数中的<KeyIn,ValuesIn>
     * context是MR框架的上下文对象，可以存放公共类型的数据，如map和reduce处理完后的键值对
     * 随后可以将context对象中的数据输出到HDFS或者控制台或者其他地方
     */


    @Override
    public void reduce(Text KeyIn, Iterable<IntWritable> ValuesIn, Context context)
            throws IOException, InterruptedException {
        //输出map的记录数
        //System.out.println(TokenizerMapper.sumOfTerms);
        //初始化一个int类型局部变量sum，统计每个单词出现的次数
        int sum = 0;
        //循环遍历KeyIn所对应的ValuesIn列表中的所有value值，然后进行累加
        // 在本列表中每个元素都是IntWritable类型的1，即在TokenizerMapper中定义的WORD_VALUE
        for (IntWritable value : ValuesIn) {
            sum += value.get();
        }
        //将reduce处理完后的结果输出
        context.write(KeyIn, new IntWritable(sum));

    }
}
