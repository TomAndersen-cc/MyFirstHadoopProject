package com.TomAndersen.hadoop.BayesClassification;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author
 * @Version
 * @Date 2019/11/5
 */
public class HelloWorld {
    private final static String SEPARATOR = "-----------------------------------";

    public static void main(String[] args) {

        // 测试String byte[] BytesWritable Text各数据类型之间的转换
        String myName = "TomAndersen";
        byte[] bytes = myName.getBytes();
        myName = new String(bytes);
        System.out.println(myName);
        System.out.println(Arrays.toString(bytes));

        BytesWritable bytesWritable = new BytesWritable(bytes);
        System.out.println(bytesWritable);
        Text text = new Text(bytesWritable.getBytes());
        System.out.println(text);

        System.out.println(SEPARATOR);
        // 测试split分类效果
        String content = "Beg your pardon sir,i did't quite catch your meaning.\n Can you repeat it?";
        System.out.println(content);
        String[] contents = content.split("\\n");
        // 为什么使用双引号"分割就是 \" ，为什么使用回车分割就是 \\n, 为什么???
        // 答：对于正则表达式中的特殊字符都是\\代表一个转义字符\
        // 对于非正则表达式中的特殊字符使用单个\即可代表一个转移字符\
        for (String str : contents) {
            System.out.println(str);
        }

        System.out.println(SEPARATOR);
        String spaces = "\t\t";
        String str = "  s ";
        Pattern pattern = Pattern.compile("^\\s+|\t+$");
        Matcher matcher1 = pattern.matcher(spaces);
        Matcher matcher2 = pattern.matcher(str);
        System.out.println(matcher1.matches());
        System.out.println(matcher2.matches());

        System.out.println(SEPARATOR);
        String unicodeChar = "\u0000";//这是一个Unicode中的空，一个4位16进制表示一个Unicode
        System.out.println(unicodeChar);

        System.out.println(SEPARATOR);
        myName = "TomAndersen";
        BytesWritable bytesWritable1 = new BytesWritable(myName.getBytes());
        System.out.println(bytesWritable1.toString());
        System.out.println(new String(bytesWritable1.getBytes()));

        // 测试系统退出状态是否只有非0和0两种选项，结果表面0代表正常退出，非0都代表不正常退出
        // System.exit(-4);

    }
}
