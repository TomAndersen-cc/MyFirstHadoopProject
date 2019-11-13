package com.TomAndersen.hadoop.HDFSTools;

import com.TomAndersen.hadoop.BayesClassification.JobsInitiator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Set;


/**
 * @Author
 * @Version
 * @Date 2019/11/5
 * 切记：工具包和调用者之间应该是绝对的低耦合，各种参数都应该由调用者决定
 */
public class BayesTools {
    // demo测试通过
    // 用于判断HDFS中某路径下是否已经存在文件夹，若存在则清空文件夹内文件，以免每次测试都需要手动删除文件夹内容
    public static void CheckOutputPath(String outputPath) throws IOException {

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path outPath = new Path(outputPath);

        if (fs.exists(outPath)) {
            /*fs.deleteOnExit(outPath);// 此方法为在程序退出时删除，应该是在当场就删除
            fs.close();*/
            fs.delete(outPath, true);
            fs.close();

            System.out.println("Delete " + outputPath + " succeed!");
        } else {
            System.out.println(outputPath + " does not exist!");
        }
    }


    // 读取指定文档中的每一列，将第一列作为Key值，其他列作为Value，返回HashMap数组
    // 反正之后都要转换成其他类型，索性全都读取成String类型
    // demo测试通过
    public static HashMap[] getKeyValuesByReadFile(String filePath, Configuration conf, String separator)
            throws IOException {

        FSDataInputStream fsDataInputStream = null;
        InputStreamReader inputStreamReader = null;
        BufferedReader bufferedReader = null;
        String fileLine = null;
        String[] fileText = null;
        HashMap<String, String>[] multipleKeyValues = null;
        HashMap<String, String> KeyValues = null;

        try {

            FileSystem fs = FileSystem.get(conf);
            fsDataInputStream = fs.open(new Path(filePath));
            inputStreamReader = new InputStreamReader(fsDataInputStream);
            bufferedReader = new BufferedReader(inputStreamReader);

            while (bufferedReader.ready()) {
                fileLine = bufferedReader.readLine();
                fileText = fileLine.split(separator);
                if (multipleKeyValues == null) multipleKeyValues = new HashMap[fileText.length - 1];
                for (int i = 1, fileTextLength = fileText.length; i < fileTextLength; i++) {
                    String text = fileText[i];
                    if (multipleKeyValues[i - 1] != null) {
                        multipleKeyValues[i - 1].put(fileText[0], text);
                    } else {
                        KeyValues = new HashMap<>();
                        KeyValues.put(fileText[0], text);
                        multipleKeyValues[i - 1] = KeyValues;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bufferedReader != null) {
                bufferedReader.close();
            }
        }
        return multipleKeyValues;
    }

    // 以下方法和调用者耦合度很高，不应该放置在工具类中实现
/*
    private static HashMap fileClassToSumOfWords = null;
    private static HashMap fileClassToSumOfFiles = null;
    private static HashMap fileClassAndWordToPossibility = null;

    // 初始化以上的各项Map，即通过读取对应的文件获取计算所需数据
    public static void InitCalculator(String filePath1, String filePath2) {

    }

    // 用于计算某个文档属于某个类别的概率
    public static double calculatePossibility(String filePath, String fileClass) {
        double Posiibility = 0D;
        return Posiibility;
    }*/


    private static HashMap<String, String> fileClassToSumOfWords = null; // 测试集文档类别到单词总数的映射，用于计算单词缺省概率
    private static HashMap<String, String> fileClassToSumOfFiles = null; // 测试集文档类别到文档总数的映射，用于计算文档类别的先验概率
    private static HashMap<String, String> fileClassAndWordToPossibility = null; // 测试集文档类别-单词到条件概率的映射
    private static int sumOfTrainSetFiles = 0;// 训练集中文档总数，用于计算文档类别的先验概率
    private static int sumOfTestSetFiles = 0;// 测试集中文档总数，用于计算宏平均评价矩阵

    private static void Init(String filePath1, String filePath2, Configuration configuration) throws IOException {
        // 通过读取第一个文档，获取文档类别到单词总数、文档类别到文档总数的映射
        // 通过读取第二个文档，获取文档类别-单词到条件概率的映射
        // 在本次实现中，第一个文档路径为Job2的输出路径，第二文档的路径为Job3的输出路径
        HashMap[] hashMaps_1 = BayesTools.getKeyValuesByReadFile(filePath1, configuration, "\t");
        HashMap[] hashMaps_2 = BayesTools.getKeyValuesByReadFile(filePath2, configuration, "\t");
        // 因为之前Job的输出内容都是已知的，所以此函数的读取也是已知的，直接赋值即可
        // 要想避免这些类型未检查的warn，可以尝试直接全都换成object类型，然后使用的时候进行强转成String
        BayesTools.fileClassToSumOfWords = hashMaps_1[0];
        BayesTools.fileClassToSumOfFiles = hashMaps_1[1];
        BayesTools.fileClassAndWordToPossibility = hashMaps_2[0];

    }


    // 使用已经训练好的模型对整个测试集进行分类，并建立评价矩阵，然后控制台输出评估分类结果
    public static void BayesClassifier(String TranSetPath, Configuration configuration) throws IOException {

        // 第一个参数为测试集HDFS路径，第二参数为输出文件HDFS路径，即对于分类的整体评价
        // 在读取之前务必进行数据初始化，即调用Init函数
        BayesTools.Init(JobsInitiator.Job2_OutputPath, JobsInitiator.Job3_OutputPath, configuration);
        // 对每个类别构建评价矩阵
        Set<String> fileClasses = BayesTools.fileClassToSumOfFiles.keySet();// 获取所有的文档类别
        // 评价矩阵采用2维方阵的形式
        HashMap<String, int[][]> classEvaluationMatrix = new HashMap<>();// 所有类别的评价矩阵
        HashMap<String, Integer> classToSumOfTestSetFiles = new HashMap<>();// 用于统计测试集中各个类别的文档数量
        FileSystem fs = FileSystem.get(configuration);
        // 获取文件夹目录下所有文件状态信息
        FileStatus[] fileStatuses = fs.listStatus(new Path(TranSetPath));
        // 获取测试集文档总数
        BayesTools.sumOfTestSetFiles = fileStatuses.length;
        // 宏平均评价矩阵：TP=[0][0],FP=[1][0],FN=[0][1],TN=[1][1]
        int[][] MacroAverage = new int[][]{{0, 0}, {0, 0}};
        // 分类结果准确率
        double accuracy = 0;
        // 分类结果精确率
        double precision = 0;
        // 召回率
        double recall = 0;
        // F1分数
        double F1 = 0;

        // 初始化各种统计辅助工具
        for (String fileClass : fileClasses) {
            // 统计训练集中所有文档数量
            BayesTools.sumOfTrainSetFiles += Integer.valueOf(BayesTools.fileClassToSumOfFiles.get(fileClass));
            int[][] matrix = new int[][]{{0, 0}, {0, 0}};// 定义时初始化数组
            classEvaluationMatrix.put(fileClass, matrix);// 初始化各类别评价矩阵
            classToSumOfTestSetFiles.put(fileClass, 0);// 初始化各类别文档数量
        }

        // 对测试集中所有文档进行遍历
        // 对文件夹中的每个文件都进行分类，分类的同时填充每个类别的评价矩阵
        for (FileStatus fileStatus : fileStatuses) {
            Path filePath = fileStatus.getPath();// 获取文件路径
            String fileName = filePath.getName();// 获取文件名（文件名中包含原始类别戳）
            String fileInitialClass = fileName.split("-")[0];// 获取文件原始类别
            String fileJudgedClass = BayesTools.Classifier(filePath, configuration);// 获取文件判定类别
            // 每次处理一个文档则对对应的类别文档总数+1
            int x = classToSumOfTestSetFiles.get(fileInitialClass);
            classToSumOfTestSetFiles.put(fileInitialClass, ++x);
            // 接下来对原始类别和判定类别的评价矩阵对应位置进行+1
            if (fileInitialClass.equals(fileJudgedClass)) {
                classEvaluationMatrix.get(fileInitialClass)[0][0]++;
            } else {
                classEvaluationMatrix.get(fileJudgedClass)[1][0]++;
            }
        }
        // 至此统计完了测试集中各个类别文档总数，统计完了被分类到各个类别中的文档数
        // 上面只完成了评价矩阵的一半，下面计算另一半
        for (String fileClass : fileClasses) {
            int[][] matrix = classEvaluationMatrix.get(fileClass);
            int fileClassTruthYes = classToSumOfTestSetFiles.get(fileClass);// 获取测试集中此类真实文档数量
            int fileClassTruthNo = sumOfTestSetFiles - fileClassTruthYes;// 计算测试集中非此类文档的真实数量
            matrix[0][1] = fileClassTruthYes - matrix[0][0];
            matrix[1][1] = fileClassTruthNo - matrix[1][0];
        }
        // 至此各个类别的评价矩阵就建立完毕了

        // 下面开始统计各项评价指标
        // 求宏平均评价矩阵
        for (String fileClass : fileClasses) {
            // 将每个类别对应的评价矩阵各项求和
            int[][] matix = classEvaluationMatrix.get(fileClass);
            MacroAverage[0][0] += matix[0][0];
            MacroAverage[0][1] += matix[0][1];
            MacroAverage[1][0] += matix[1][0];
            MacroAverage[1][1] += matix[1][1];
        }
        // 计算评价指标TP=[0][0],FP=[1][0],FN=[0][1],TN=[1][1]
        // 计算准确率accuracy = (TP+TN)/(TP+TN+FP+FN)
        accuracy = (double) ((MacroAverage[0][0] + MacroAverage[1][1]) /
                (MacroAverage[0][0] + MacroAverage[1][0] + MacroAverage[0][1] + MacroAverage[1][1]));
        // 计算精确率precious = TP/(TP+FP)
        precision = (double) (MacroAverage[0][0]) / (MacroAverage[0][0] + MacroAverage[1][0]);
        // 计算召回率recall = TP/(TP+FN)
        recall = (double) (MacroAverage[0][0]) / (MacroAverage[0][0] + MacroAverage[0][1]);
        // 计算F1分数F1 = 2*precision*recall/(precision+recall)
        F1 = 2 * precision * recall / (precision + recall);

        // 输出评价指标
        System.out.println("accuracy: " + accuracy);
        System.out.println("precision: " + precision);
        System.out.println("recall: " + recall);
        System.out.println("F1: " + F1);

    }

    // 本方法用于判断某单个文件属于哪个类别
    public static String Classifier(Path filePath, Configuration configuration) throws IOException {
        String fileJudgedClass = null;// 判定类别
        double prob = Double.MIN_VALUE; // 相对概率
        Set<String> fileClasses = BayesTools.fileClassToSumOfFiles.keySet();// 获取所有类别
        for (String fileClass : fileClasses) {
            double temp = getRelativePosibility(filePath, fileClass, configuration);// 获取本文档在某个类别中出现的概率
            if (temp > prob) {// 取最大相对概率对应类别作为判定类别
                prob = temp;
                fileJudgedClass = fileClass;
            }
        }
        return fileJudgedClass;
    }

    // 本方法用于计算某个文档在某个类别中出现的相对概率
    public static double getRelativePosibility(Path filePath, String fileClass, Configuration configuration) throws IOException {

        double prob = 0; // 当前文档在当前类别中出现的条件概率
        FileSystem fileSystem = FileSystem.get(configuration); // 获取文件系统
        FileStatus fileStatus = fileSystem.getFileStatus(filePath); // 获取文件状态信息
        int fileLength = (int) fileStatus.getLen(); // 获取文件大小
        InputStream inputStream = null; // 创建输入流
        byte[] buffer = new byte[fileLength]; // 创建缓冲区
        String text = null;// 存放文件全部内容

        try {
            // 打开输入流
            inputStream = fileSystem.open(filePath);
            // 将输入流中的数据全都拷贝到buffer中
            IOUtils.readFully(inputStream, buffer, 0, fileLength);
            // 将缓冲区字节转换成字符串
            text = new String(buffer);

        } finally {
            IOUtils.closeStream(inputStream); // 关闭输入流
        }
        // 获取完字符串后，其中内容进行分词，并针对当前类别计算其概率
        String[] words = text.split("\r\n|\n|\r|\u0000+");
        // 默认条件概率为1/当前类别单词总数
        double defaultProbability = 1 / Double.valueOf(BayesTools.fileClassToSumOfWords.get(fileClass));
        for (String word : words) {
            // 获取对应单词在当前类别中出现的条件概率，取log然后相加，注意log之后是负值
            // 之所以去log，是因为浮点数直接相乘的话会有很大精度丢失
            /*prob += Math.log(Double.valueOf(BayesTools.
                    fileClassAndWordToPossibility.getOrDefault(fileClass + "-" + word, null)));*/
            String temp = BayesTools.fileClassAndWordToPossibility.getOrDefault(fileClass + "-" + word, null);
            // 如果没有这个单词则使用默认条件概率
            // 如果存在这个单词，则直接取相应条件概率
            prob += temp == null ? Math.log(defaultProbability) : Math.log(Double.valueOf(temp));
        }
        // 还要算上各类别文档的先验概率
        int numOfClassTrainFile = Integer.valueOf(BayesTools.fileClassToSumOfFiles.get(fileClass));// 当前类别训练集文档数量
        prob += Math.log((double) (numOfClassTrainFile / BayesTools.sumOfTrainSetFiles));// 算上当前类别文档的先验概率
        return prob;
    }
    // 还是那个问题，工具包应该和调用者尽可能的低耦合，所以这个程序还是不应该写在这个工具包中
    // 但是其他的程序确实也可以使用这个分类器，因为本来就是配合Bayes分类进行使用的
    // 最终结论还是放在这个工具类中进行使用，多写一个类实在是显得另类，没必要
}
