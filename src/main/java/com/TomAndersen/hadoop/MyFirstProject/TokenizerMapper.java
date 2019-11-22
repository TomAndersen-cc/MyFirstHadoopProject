package com.TomAndersen.hadoop.MyFirstProject;


import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 本类继承自Mapper类，负责重写父类Mapper方法中的map函数
 * Mapper<LongWritable,Text,Text,IntWritable>泛型的参数含义分别为：
 * LongWritable表示偏移量相当于读取文本的地址，由MapReduce框架自动根据文件获取,同时也是KeyIn
 * 第一个Text表示读取的文件中的某一行文本，同时也是ValueIn；第二个Text表示的map函数输出的键值对<KeyOut,ValueOut>中的KeyOut，也就是单词
 * IntWritable指的是map函数输出的键值对中的ValueOut，在本例中也就是一个数字1，即map最终输出的是<word,1>这样的键值对
 */

public class TokenizerMapper
        extends Mapper<Text, BytesWritable, Text, IntWritable> {
    //定义一个IntWritable类型的私有静态常量WORD_VALUE，并初始化为1，用于生成 <word,value> 键值对
    // 便于后续的Reduce操作直接将value相加，进而统计相同word的数量
    private final static IntWritable WORD_VALUE = new IntWritable(1);
    //定义一个Text类型的私有静态常量WORD，用于记录word
    private final static Text WORD = new Text();

    /**
     * map函数主要负责对读取的文件内容进行映射处理、
     * KeyIn是从文本文件中读取的每行文本的偏移量地址
     * ValueIn值是文本文件中的一行文本，从MapReduce框架负责传入
     * context是MapReduce中的上下文对象，可以存放公共类型的数据，比如map函数处理完
     * 的中间结果可以保存到context上下文对象中，MapReduce框架再根据上下文对象中的数
     * 据将其进行按Key分组，最后进行Reduce操作
     */
    //用于统计总共记录数
    //public static int sumOfTerms = 0;
    @Override
    public void map(Text KeyIn, BytesWritable ValueIn, Context context)
            throws IOException, InterruptedException {
        //sumOfTerms++;
        //将读取的一行Text文本转化为Java的字符串String类型

        //String line = ValueIn.toString();
        String line = new String(ValueIn.copyBytes());
        //按照空格符切分出一行字符串中包含的所有单词，并存储到字符串数组中
        String[] words = line.split("\r\n|\n|\r|\u0000+");
        //循环遍历字符串数组words，将其中的每个单词作为KeyOut值，上面定义的IntWritable类型常量WORD_VALUE作为ValueOut值
        for (String word : words) {
            //用Text类型的静态常量WORD记录单词word
            WORD.set(word);
            //单词word是keyOut，其对应的ValueOut在前面已经确定为IntWritable类型的静态常量WORD_VALUE，故不需要再设置
            //直接写入上下文context中，随后会自动根据word的值给<word,WORD_VALUE>分组，形成新的键值对<word,values>，注意第二个values
            //指的是根据word分组之后的value的集合，这样便于之后的Reduce操作
            context.write(WORD, WORD_VALUE);
        }

        /*context.write(new Text(line),WORD_VALUE);*/
    }

}
