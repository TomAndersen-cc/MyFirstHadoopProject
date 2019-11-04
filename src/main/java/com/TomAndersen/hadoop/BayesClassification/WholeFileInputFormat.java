package com.TomAndersen.hadoop.BayesClassification;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;


import java.io.IOException;

/**
 * @Author
 * @Version
 * @Date 2019/11/4
 * 自定义InputFormat，将整个文件作为一条record，并将文件名作为KeyIn
 */
public class WholeFileInputFormat extends FileInputFormat<Text, BytesWritable> {
    //自定义InputFromat，其中传给Map的KeyIn-ValueIn类型分别为Text和BytesWritable
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader
            (InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        WholeFileRecordReader wholeFileRecordReader = new WholeFileRecordReader();
        wholeFileRecordReader.initialize(inputSplit, taskAttemptContext);
        return wholeFileRecordReader;
    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;//返回false即不允许分片
    }
}
