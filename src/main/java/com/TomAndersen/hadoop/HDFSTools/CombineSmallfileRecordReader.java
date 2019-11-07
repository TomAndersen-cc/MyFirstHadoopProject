package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;


import java.io.IOException;

/**
 * @Author
 * @Version
 * @Date 2019/11/7
 */
public class CombineSmallfileRecordReader extends RecordReader<Text, BytesWritable> {
    private CombineFileSplit combineFileSplit;
    private WholeFileRecordReader wholeFileRecordReader = new WholeFileRecordReader();
    //private Path[] paths;
    private int totalLength;
    private int currentIndex;
    private float currentProgress = 0;
    private Text currentKey = new Text();
    private BytesWritable currentValue = new BytesWritable();

    public CombineSmallfileRecordReader(CombineFileSplit combineFileSplit, TaskAttemptContext context, Integer index) throws IOException {
        super();
        this.combineFileSplit = combineFileSplit;
        this.currentIndex = index; // 当前要处理的小文件Block在CombineFileSplit中的索引
    }

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext context) throws IOException, InterruptedException {
        this.combineFileSplit = (CombineFileSplit) inputSplit;
        // 处理CombineFileSplit中的一个小文件Block，因为使用LineRecordReader，需要构造一个FileSplit对象，然后才能够读取数据
        FileSplit fileSplit = new FileSplit(combineFileSplit.getPath(currentIndex), combineFileSplit.getOffset(currentIndex), combineFileSplit.getLength(currentIndex), combineFileSplit.getLocations());
        wholeFileRecordReader.initialize(fileSplit, context);

        Path[] paths = combineFileSplit.getPaths();
        totalLength = paths.length;
        context.getConfiguration().set("map.input.file.name", combineFileSplit.getPath(currentIndex).getName());
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (currentIndex >= 0 && currentIndex < totalLength) {
            return wholeFileRecordReader.nextKeyValue();
        } else {
            return false;
        }
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        currentKey.set(wholeFileRecordReader.getCurrentKey());
        return currentKey;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        byte[] contents = wholeFileRecordReader.getCurrentValue().getBytes();
        currentValue.set(contents, 0, contents.length);
        return this.currentValue;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        if (currentIndex >= 0 && currentIndex < totalLength) {
            currentProgress = (float) currentIndex / totalLength;
            return currentProgress;
        }
        return currentProgress;
    }

    @Override
    public void close() throws IOException {
        wholeFileRecordReader.close();//实际上啥都没干
    }
}
