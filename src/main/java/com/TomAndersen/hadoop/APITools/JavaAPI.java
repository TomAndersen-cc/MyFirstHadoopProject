package com.TomAndersen.hadoop.APITools;


import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

/**
 * @Author Tom Andersen
 * @Version 0.1
 * @Date 2019/10/8
 */
public class JavaAPI {
    //需要支持接收参数，并根据参数调用对应的方法，可以利用Class反射机制

    //初始化处理工具类(默认为)JavaAPIProcessor，存储对应的Class信息
    private static final Class DEFAULTPROCESSOR = JavaAPIProcessor.class;
    //使用私有静态类型Map存储参数名到方法名的映射
    private static Map<String, String> argsToMethod = new HashMap<>();
    //使用私有静态类型Map存储参数名到对应处理器类的映射
    private static Map<String, Class> argsToClass = new HashMap<>();

    //初始化Map映射，默认将默认处理器JavaAPIProcessor中的方法和Class信息加入到Map中
    static {
        String[] DefaultArgs = {"GET", "PUT", "MKDIR", "RMDIR"};
        String[] DefaultMethod = {"Get", "Put", "Mkdirs", "Rmdirs"};
        for (int i = 0; i < DefaultArgs.length; i++) {
            argsToClass.put(DefaultArgs[i], DEFAULTPROCESSOR);
            argsToMethod.put(DefaultArgs[i], DefaultMethod[i]);
        }
    }

    //提供静态的register注册器注册方法，便于后期拓展方法列表 scalability
    public static boolean funcRegister(String arg, String funcName, Class cls) {

        argsToClass.put(arg, cls);
        argsToMethod.put(arg, funcName);
        //如果注册成功则返回True
        return true;
    }

    public static void main(String[] args) throws Exception {
        //---------------------------

        //手动设置参数进行测试：
        args = new String[]{"put", "null", "null"};

        //---------------------------
        if (argsToMethod.containsKey(args[0].toUpperCase()) &&
                argsToClass.containsKey(args[0].toUpperCase())) {
            Class<?> Classinfo = argsToClass.get(args[0].toUpperCase());
            String Methods = argsToMethod.get(args[0].toUpperCase());
            String[] Args = new String[args.length - 1];
            System.arraycopy(args, 1, Args, 0, args.length - 1);
            Method method = Classinfo.getMethod(Methods, Args.getClass());
            //因为是反射调用静态方法，所以invoke第一个参数为null，否则应该获取类实例然后再传给第一个参数
            //调用对应的方法，保存任务执行状态 new Object[]{Args}
            boolean bool = (boolean) method.invoke(null, new Object[]{Args});//注意使用已有数组创建Object数组的方式
            //此处invoke第二参数还可以写成 Args、(Object[])Args等等
            if (!bool) System.out.println("执行失败！");
            else System.out.println("执行成功！");
        } else {
            System.out.println("参数错误！");
        }
    }
}
