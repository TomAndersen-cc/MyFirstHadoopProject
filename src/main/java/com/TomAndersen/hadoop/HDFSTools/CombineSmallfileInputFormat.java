package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * @Author
 * @Version
 * @Date 2019/11/7
 * CombineFileInputFormat主要原理是：一个RecordReader中包含有一个内部RecordReader
 * 每次内部RecordReader读取完一条记录后，更新内部RecordReader，直到将所有的记录都读取完成
 * 然后形成一个大的split交给Mapper处理，简而言之就是整合小文件的block形成大split，即CombineFileSplit
 * 这样就能大幅度减少maptask的数量，不用再将大量的时间浪费在小文件处理时资源频繁的申请和释放阶段
 */
public class CombineSmallfileInputFormat extends CombineFileInputFormat<Text, BytesWritable> {

    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext context) throws IOException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) inputSplit;
        CombineFileRecordReader<Text, BytesWritable> recordReader = new CombineFileRecordReader<Text, BytesWritable>(combineFileSplit, context, CombineSmallfileRecordReader.class);
        try {
            recordReader.initialize(combineFileSplit, context);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return recordReader;
    }
}
