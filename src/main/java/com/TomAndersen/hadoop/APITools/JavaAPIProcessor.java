package com.TomAndersen.hadoop.APITools;

/**
 * @Author
 * @Version
 * @Date 2019/10/8
 */

//用于提供方法调用的工具类
//此类可以被继承从而实现自定义化，也可以添加方法而不用修改本文件
public class JavaAPIProcessor {

    //将本地文件上传到HDFS指定路径中
    public static boolean Put(String[] args) {
        if (args.length != 2) {
            System.out.println("参数错误！");
            return false;
        }
        //------------------------------------------------
        //以下是具体实现（直接调用HDFS JAVA API即可）：


        //------------------------------------------------
        //注意：有可能上面的代码块抛出异常
        return true;
    }

    //将HDFS指定路径的文件写入到本地指定路径文件中
    public static boolean Get(String[] args) {
        if (args.length != 2) {
            System.out.println("参数错误！");
            return false;
        }
        //------------------------------------------------
        //以下是具体实现（直接调用HDFS JAVA API即可）：


        //------------------------------------------------
        //注意：有可能上面的代码块抛出异常
        return true;
    }

    //在HDFS中创建目录
    public static boolean Mkdirs(String[] args) {
        if (args.length != 1) {
            System.out.println("参数错误！");
            return false;
        }
        //------------------------------------------------
        //以下是具体实现（直接调用HDFS JAVA API即可）：


        //------------------------------------------------
        //注意：有可能上面的代码块抛出异常
        return true;
    }

    //在HDFS中删除目录
    public static boolean Rmdirs(String[] args) {
        if (args.length != 1) {
            System.out.println("参数错误！");
            return false;
        }
        //------------------------------------------------
        //以下是具体实现（直接调用HDFS JAVA API即可）：


        //------------------------------------------------
        //注意：有可能上面的代码块抛出异常
        return true;
    }
}
