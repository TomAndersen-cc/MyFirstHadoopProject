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
