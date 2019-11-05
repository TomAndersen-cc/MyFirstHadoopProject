package com.TomAndersen.hadoop.HDFSTools;

import java.io.IOException;
import java.net.URI;

/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 用于测试工具包中的函数是否可用
 */
public class ToolTestDemo {
    public static void main(String[] args) throws IOException {
        String uri = "D:/TestData/Document1";
        // 记得使用String创建URI里面不能包含特殊字符，不能使用转义字符的形式如：D:\\TestData\\Document1
        // 而应该写成/的形式，如：D:/TestData/Document1
        BayesTools.CheckOutputPath(uri);
    }
}
