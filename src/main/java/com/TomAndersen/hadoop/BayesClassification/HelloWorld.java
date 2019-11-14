package com.TomAndersen.hadoop.BayesClassification;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.fs.Path;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 用于测试各种不太确定的知识点
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

        HashMap<String, String> myMap = new HashMap<>();
        String[] words = new String[10];
        HashMap[] KeyValues = new HashMap[10];// 创建HashMap数组的形式，不需要定义泛型
        HashMap<String, String>[] myMaps = null;
        myMaps = new HashMap[10];

        myMap.put("Tom", "24");
        myMap.put("Alise", "22");
        System.out.println(myMap);
        /*System.out.println(KeyValues[0]);// 未赋值即为null
        System.out.println(KeyValues[1]);// 未赋值即为null*/

        System.out.println(SEPARATOR);
        Text text1 = new Text();
        text1.set("TomAndersen" + 13);
        System.out.println(text1);

        System.out.println(SEPARATOR);
        String Job2OutpUtPath = "C:\\Users\\DELL\\Desktop\\HadoopProjects\\MyFirstHadoopProject\\src\\Input";
        Path path = new Path(Job2OutpUtPath);
        System.out.println(path);

        // 测试HashMap中KeySet的用法
        System.out.println(SEPARATOR);
        HashMap<String, String> myMap1 = new HashMap<>();
        myMap1.put("Student1", "Tom");
        myMap1.put("Student2", "Alise");
        myMap1.put("Student3", "Fransic");
        myMap1.put("Student4", "Bruce");
        System.out.println(myMap1);

        // 遍历KeySet的方式1：使用for-each
        Set<String> stringKeySet = myMap1.keySet();
        for (String it : stringKeySet) {
            System.out.print(it + " ");
        }
        System.out.println();
        // 遍历KeySet的方式2：使用Iterator
        Iterator<String> iterator = stringKeySet.iterator();
        while (iterator.hasNext()) {
            String key = iterator.next();
            System.out.print(key + " ");
        }

        // 测试二维数组的初始化
        System.out.println(SEPARATOR);
        // 在定义时初始化
        int[][] temp = new int[][]{{0, 0, 0}, {0, 0}};
        System.out.println(Arrays.toString(temp));
        System.out.println(temp[0].length);
        // 定义完之后，遍历赋值初始化（这里就不再赘述了）

        // 测试Double浮点数与String字符串之间的转换
        System.out.println(SEPARATOR);
        String string = "4.748901816454945E-4";
        System.out.println(Double.valueOf(string));

        // 测试double强转
        System.out.println(SEPARATOR);
        int x = 54;
        int y = 95;
        double z1 = (double) (x / y);// 这样强转是先计算x/y，然后再强转
        System.out.println(z1);
        double z2 = (double) x / y;// 这样是将x先强转，然后再计算x/y
        System.out.println(z2);
        // 直接计算x/y时，由于是整型变量，所以计算结果会取整
        System.out.println(x / y);

        // 注意MIN_VALUE指的是double数据类型正方向的范围最小值，MAX_VALUE表示范围最大值
        // 其中double数据范围是关于原点对称的，其他数据类型同理
        double m = Double.MAX_VALUE;
        System.out.println(m);
    }
}
