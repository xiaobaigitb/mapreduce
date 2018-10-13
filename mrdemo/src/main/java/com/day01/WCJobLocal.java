package com.day01;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.ArrayList;
//mapred是hadoop的1.X的包，mapreduce是2.X的API

/**
 * 测试-设定任务的运行
 * 输入与输出的路径
 */
public class WCJobLocal {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //设置一个任务,后面是job的名称
        Job job = Job.getInstance(coreSiteConf, "wc");
        //设置job的运行类，就是此类
        //job.setJarByClass(WCJobLocal.class);
        //将打的jar包自动上传临时目录，运行之后就删除了--自己看不到
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //mrdemo/target/jar/wcreducer/mrdemo-1.0-SNAPSHOT.jar
        job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(WCMapper.class);
        job.setCombinerClass(WCCombiner.class);
        job.setReducerClass(WCReducer.class);
        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/wc"));
        //设置任务的输出路径--保存结果(这个目录必须是不存在的目录)
        //删除存在的文件
        deleteFileName("/wcout");

        FileOutputFormat.setOutputPath(job, new Path("/wcout"));
        //运行任务 true：表示打印详情
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println(flag);
            readContent("/wcout/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }
    }

    //删除已经存在在hdfs上面的文件文件
    private static void deleteFileName(String path) throws IOException {
        //将要删除的文件
        Path fileName = new Path(path);
        Configuration entries = new Configuration();
        //解析core-site-master2.xml文件
        entries.addResource(Resources.getResource("core-site-master2.xml"));
        //coreSiteConf.set(,);--在这里可以添加配置文件
        //获取客户端文件系统
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
    private static void readContent(String path) throws IOException {
        //将要读取的文件路径
        Path fileName = new Path(path);
        ArrayList<String> returnValue = new ArrayList<String>();
        Configuration configuration = new Configuration();
        configuration.addResource(Resources.getResource("core-site-master2.xml"));
        //获取客户端系统文件
        FileSystem fileSystem = FileSystem.get(configuration);
        //open打开文件--获取文件的输入流用于读取数据
        FSDataInputStream inputStream = fileSystem.open(fileName);
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
        System.out.println("文件内容如下：");
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
