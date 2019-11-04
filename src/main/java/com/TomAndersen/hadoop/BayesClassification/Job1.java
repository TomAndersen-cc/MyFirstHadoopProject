package com.TomAndersen.hadoop.BayesClassification;

/**
 * @Author
 * @Version
 * @Date 2019/11/4
 * Job1：统计出各类文档中各种单词的数目
 * Job1:
 * 将一个文档作为一个记录处理
 * 输入路径：训练集
 * 输出路径：自定义Job1输出路径，如：/OutPut/Job1/
 * Mapper：
 * 输入：<nullwritable,单词>，输出：<文档类别_单词，1>
 * 其中文档类别通过读取文件名前缀获得，单词通过对text划分可得
 * Conbiner：即Reducer
 * Reducer：
 * 输入：<文档类别_单词，{1,1,1,1,1,1……}>，输出：<文档类别_单词，sum>
 */

public class Job1 {
    public static void main(String[] args) {

    }
}
