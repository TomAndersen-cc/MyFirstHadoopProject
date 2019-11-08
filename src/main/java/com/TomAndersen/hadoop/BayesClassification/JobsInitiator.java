package com.TomAndersen.hadoop.BayesClassification;

import com.TomAndersen.hadoop.HDFSTools.BayesTools;
import org.apache.hadoop.util.ToolRunner;


/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 用于启动所有的Job，本类中只进行作业调度
 */
public class JobsInitiator {
    private static final String Job1_OutputPath = "/OutPut/Job1/";
    private static final String Job2_OutputPath = "/OutPut/Job2/";
    private static final String Job3_OutputPath = "/OutPut/Job3/";
    private static final String Job4_OutputPath = "/OutPut/Job4/";

    public static void main(String[] args) throws Exception {
        // args中有两个参数，第一个为训练集，第二个为测试集
        /*try {
            if (args.length < 2)
                throw new IllegalArgumentException("***************At least two parameters are required!***************");
            else if (args.length > 2)
                throw new IllegalArgumentException("***************Only two parameters are required!***************");
        } catch (IllegalArgumentException ex) {
            ex.printStackTrace();
            System.exit(1);
        }*/
        if (args.length < 2) {
            System.out.println("***************At least two parameters are required!***************");
            System.exit(1);
        } else if (args.length > 2) {
            System.out.println("***************Only two parameters are required!***************");
            System.exit(1);
        }

        /*BayesTools.CheckOutputPath(Job1_OutputPath);//检查Job1输出路径是否为空
        //Job1输入路径为训练集，输出路径为自定义路径
        int Job1exitCode = ToolRunner.run(new Job1(), new String[]{args[0], Job1_OutputPath});
*/
        BayesTools.CheckOutputPath(Job2_OutputPath);//检查Job1输出路径是否为空
        int Job2exitCode = ToolRunner.run(new Job2(), new String[]{args[0], Job2_OutputPath});

        System.exit(Job2exitCode);//当参数为0时表示正常终止JVM，为非0时表示异常终止

    }
}
