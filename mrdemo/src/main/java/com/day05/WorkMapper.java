package com.day05;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * java基本数据类型步支持序列化--存放在内存中不在磁盘中（不能被持久化）
 * 用来处理map任务：映射
 *
 1.扫描hdfs上的文本文件
 2.小文件的判定标准是小于2MB
 3,合并小文件成一个大文件。
 合并的方式是每个小文件的内容为一行。如果小文件为多行文件，则将小文件拼成字符串。
 一行由key:filename,value：content组成。
 如：
 a.txt  的内容：
 hello henan
 hello world
 b.txt  的内容：
 hello henan
 hello zhengzhou
 */
public class WorkMapper extends Mapper<LongWritable,Text,Text,Text> {
    private Text filenameKey;
    private boolean flag=false;

//    @Override
//    protected void setup(Context context) throws IOException, InterruptedException {
//        InputSplit split = context.getInputSplit();
//        Path path = ((FileSplit) split).getPath();
//
//
//        //使用hdfs的API---用Configuration
//        Configuration configuration = context.getConfiguration();
//        //客户端文件系统
//        FileSystem fileSystem = FileSystem.get(configuration);
//
//        if (fileSystem.getFileLinkStatus(path).isDirectory()){
//            flag=true;
//            return;
//        }
//
//        //获取文件的状态
//        long len = fileSystem.getFileLinkStatus(path).getLen();
//        //关闭资源
//        //fileSystem.close();
//        if (len>2*1024*1024||!path.getName().contains(".txt")){
//            //如果大于2M则不运行
//            //系统推出，jvm终止--推出，不执行map
//            //System.exit(0);
//            flag=true;
//        }
//        //toString：全路径
//        //getName：文件名
//        filenameKey = new Text(path.getName());
//    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//        if (!flag){
////            context.write(filenameKey, value);
////        }
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        context.write(new Text(inputSplit.getPath().getName()),value);

    }
}
