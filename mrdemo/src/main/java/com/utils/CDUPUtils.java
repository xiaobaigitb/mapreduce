package com.utils;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;

public class CDUPUtils {
    //删除已经存在在hdfs上面的文件文件
    public static void deleteFileName(String path) throws IOException {
        //将要删除的文件
        Path fileName = new Path(path);
        Configuration entries = new Configuration();
        //解析core-site-master2.xml文件
        entries.addResource(Resources.getResource("core-site-master2.xml"));
        //coreSiteConf.set(,);--在这里可以添加配置文件
        FileSystem fileSystem = FileSystem.get(entries);
        if (fileSystem.exists(fileName)){
            System.out.println(fileName+"已经存在，正在删除它...");
            boolean flag = fileSystem.delete(fileName, true);
            if (flag){
                System.out.println(fileName+"删除成功");
            }else {
                System.out.println(fileName+"删除失败!");
                return;
            }
        }
        //关闭资源
        fileSystem.close();
    }

    //读取文件内容
    public static void readContent(String path) throws IOException {
        //将要读取的文件路径
        Path fileName = new Path(path);
        ArrayList<String> returnValue = new ArrayList<String>();
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端系统文件
        FileSystem fileSystem = FileSystem.get(configuration);
        //open打开文件--获取文件的输入流用于读取数据
        FSDataInputStream inputStream = fileSystem.open(fileName);
//        byte[] buffer = new byte[1024];
//        IOUtils.readFully(inputStream,buffer,0,inputStream.available());
//        System.out.println(new String(buffer));
        InputStreamReader inputStreamReader = new InputStreamReader(inputStream);
        //一行一行的读取数据
        LineNumberReader lineNumberReader = new LineNumberReader(inputStreamReader);
        //定义一个字符串变量用于接收每一行的数据
        String str = null;
        //判断何时没有数据
        while ((str=lineNumberReader.readLine())!=null){
            returnValue.add(str);
        }
        //打印数据到控制台
        System.out.println("MapReduce算法操作的文件内容加载如下：");
        for (String read :
                returnValue) {
            System.out.println(read);
        }
        //关闭资源
        lineNumberReader.close();
        inputStream.close();
        inputStreamReader.close();
    }
}
