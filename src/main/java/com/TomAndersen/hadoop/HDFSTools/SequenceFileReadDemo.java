package com.TomAndersen.hadoop.HDFSTools;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;


/**
 * @Author TomAndersen
 * @Date 2019/11/20
 * @Version
 * @Description 读取SequenceFile的小demo（测试成功）
 */
public class SequenceFileReadDemo {
    public static void main(String[] args) throws Exception {
        // 设置SequenceFile的输入路径
        String sequenceFileUri = "src/Output/SequenceFile/TrainSet/part-r-00000";
        // 获取配置信息
        Configuration configuration = new Configuration();
        // 获取文件系统
        FileSystem fs = FileSystem.get(configuration);
        // 设置文件路径
        Path path = new Path(sequenceFileUri);
        // SequenceFile文件有自己独特的编排方式，必须使用特定的Reader进行读取
        SequenceFile.Reader reader = null;
        try {
            // 创建Reader对象，注意此方法已经被弃用，不推荐使用此方法
            // reader = new SequenceFile.Reader(fs, path, configuration);

            // 官方文档中也没有写，自己看源码琢磨的新版方法（测试成功）
            SequenceFile.Reader.Option optionfile = SequenceFile.Reader.file(path);
            reader = new SequenceFile.Reader(configuration, optionfile);


            // 利用反射机制获取SequenceFile文件中的Key和Value的元类和实例对象，因为必定是Writable的子类故声明为Writable
            Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), configuration);
            BytesWritable valueBytes = (BytesWritable) ReflectionUtils.newInstance(reader.getValueClass(), configuration);

            // 获取当前读取的字节位置
            long position = reader.getPosition();
            while (reader.next(key, valueBytes)) {// next方法若能取到值则返回true

                // 这种转换方式存在很大漏洞，不能使用getBytes方法，必须使用copyBytes方法返回的才是真实存储的字节数组
                //String value = new String(valueBytes.getBytes());
                String value = new String(valueBytes.copyBytes());


                String syncSeen = reader.syncSeen() ? "*" : "";// 是否遇到同步点
                System.out.printf("[%s%s]\t%s\t%s\n", position, syncSeen, key, value);
                position = reader.getPosition();
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }
}
