package com.day01;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.*;
import java.util.ArrayList;

/**
 * 对hdfs的增删改查
 */
public class HdfsCURD {
    public static void main(String[] args) throws IOException {
        //设置hadoop的环境变量
        System.setProperty("hadoop.home.dir", "D:\\soft\\hadoop\\hadoop-2.7.5");

        //测试上传文件
        //testCreate();
        //测试创建文件夹
        //testMkdirs();
        //测试创建文件
        //testMktxt();
        //测试读取文件
        //testRead();
        //测试将本地文件上传到hdfs
        //testCopyFromLocalFile();
        //测试下载hdfs文件到本地
        //testCopyToLocalFile();
        //测试删除文件
        //testDelete();
        //测试向文件中追加数据
        //testAppend();
    }

    private static void testAppend() throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端系统文件
        FileSystem fileSystem = FileSystem.get(configuration);
        //获取输出流，用于写入数据
        FSDataOutputStream outputStream = fileSystem.append(new Path("/firstDir/firstTest.txt"));
        outputStream.write("how old are you".getBytes());
        //关闭资源
        fileSystem.close();
        outputStream.close();
    }

    private static void testDelete() throws IOException {
        Configuration entries = new Configuration();
        //解析core-site-master2.xml文件
        entries.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端文件系统
        FileSystem fileSystem = FileSystem.get(entries);
        boolean flag = fileSystem.delete(new Path("/wcout"), true);
        System.out.println(flag);
        //关闭资源
        fileSystem.close();
    }

    private static void testCopyToLocalFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端系统文件
        FileSystem fileSystem = FileSystem.get(configuration);
        //将hdfs文件下载到本地
        //前面表示hdfs的文件路径,后面地址代表本地路径
        fileSystem.copyToLocalFile(new Path("/firstDir/rootDir"),new Path("D:/test"));
        //关闭资源
        fileSystem.close();
    }

    private static void testCopyFromLocalFile() throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端系统文件
        FileSystem fileSystem = FileSystem.get(configuration);
        File file = new File("D:/test");
        //获取文件夹中的所有文件的名字
        String[] test = file.list();
        for (int i = 0; i < test.length; i++) {
            System.out.println(test[i]);
            fileSystem.copyFromLocalFile(new Path("D:/test/"+test[i]),new Path("/sub_key1"));
        }
        //关闭资源
        fileSystem.close();
    }

    private static void testRead() throws IOException {
        ArrayList<String> returnValue = new ArrayList<String>();
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端系统文件
        FileSystem fileSystem = FileSystem.get(configuration);
        //open打开文件--获取文件的输入流用于读取数据
        FSDataInputStream inputStream = fileSystem.open(new Path("/firstDir/firstTest.txt"));
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
        for (String read :
                returnValue) {
            System.out.println(read);
        }
        //关闭资源
        lineNumberReader.close();
        inputStream.close();
        inputStreamReader.close();
    }

    private static void testMktxt() throws IOException {
        Configuration entries = new Configuration();
        entries.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端系统文件
        FileSystem fileSystem = FileSystem.get(entries);
        //向服务端创建文件
        FSDataOutputStream dataOutputStream = fileSystem.create(new Path("/firstDir/firstTest.txt"));
        //向文件中写入数据
        dataOutputStream.write("hello,firstTest.txt".getBytes());
        //关闭资源
        fileSystem.close();
        //关闭流
        dataOutputStream.close();
    }

    private static void testMkdirs() throws IOException {
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端的文件系统
        FileSystem fileSystem = FileSystem.get(configuration);
        //客户端向服务端创建文件夹
        boolean flag = fileSystem.mkdirs(new Path("/firstDir"));
        System.out.println(false);
        //关闭资源
        fileSystem.close();
        configuration.clear();
    }

    private static void testCreate() throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site.xml"));
        //获取客户端文件系统
        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        //客户端向服务端（hdfs）写元数据
        FSDataOutputStream dataOutputStream = fileSystem.create(new Path("/firstHdfs"));
        //将数据写入DateNode
        dataOutputStream.write("hello,hadoop".getBytes());
        //关闭资源
        dataOutputStream.close();
        fileSystem.close();
    }
}
