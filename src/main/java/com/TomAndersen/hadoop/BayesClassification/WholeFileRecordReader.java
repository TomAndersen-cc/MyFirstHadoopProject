package com.TomAndersen.hadoop.BayesClassification;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @Author
 * @Version
 * @Date 2019/11/4
 */
public class WholeFileRecordReader extends RecordReader<Text, BytesWritable> {
    //自定义RecordReader，每次将一整个文档作为一个record的Value，将文档名作为Key
    private FileSplit fileSplit;    //保存输入的分片，它将被转换成一条(key,value)记录
    private Configuration conf;     //配置对象
    private BytesWritable value = new BytesWritable();  //value对象，内容为空
    private Text key = new Text();
    private boolean processed = false;  //被处理标识

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.fileSplit = (FileSplit) inputSplit;
        this.conf = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (!processed) {//如果记录没有被处理过则读取记录生成K-V
            //从fileSplit对象获取split的字节数，创建byte数组contents用于存储split内容
            byte[] contents = new byte[(int) fileSplit.getLength()];
            //从fileSplit对象中获取输入文件路径信息
            Path filePath = fileSplit.getPath();
            //获取文件系统对象
            FileSystem fileSystem = filePath.getFileSystem(conf);
            //定义文件输入流
            FSDataInputStream in = null;
            //打开文件输入流，将文件中内容一次性读出作为Value
            try {
                in = fileSystem.open(filePath);
                IOUtils.readFully(in, contents, 0, contents.length);
                //将contents内容全部放入value中
                value.set(contents, 0, contents.length);
                //将文件名作为key
                String fileName = fileSplit.getPath().getName();
                key.set(fileName);
            } finally {
                //最后关闭输入流
                IOUtils.closeStream(in);
            }
            processed = true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return processed ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        //前面读取完成的时候已经关闭了输入流，此处不必做事
    }
}
