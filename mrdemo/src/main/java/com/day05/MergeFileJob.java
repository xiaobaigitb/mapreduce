package com.day05;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 将小文件（小于2M）合并成大文件
 */
public class MergeFileJob {

    public static class MergeFileMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            context.write(new Text(inputSplit.getPath().getName()),value);

        }
    }

    public static class MergeFileReduce extends Reducer<Text,Text,Text,Text>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //key：表示文件名
            //values：表示的是一个文件的所有内容
            //将迭代器中的文件拼接
            Iterator<Text> iterator = values.iterator();
            StringBuffer content = new StringBuffer();
            while (iterator.hasNext()){
                Text line = iterator.next();
                content.append(line).append(",");
            }
            //打印
            context.write(key,new Text(content.toString()));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        //coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "merge");
        //设置job的运行类
        //job.setJarByClass(MergeFileJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT-jar-with-dependencies.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(MergeFileMapper.class);
        job.setReducerClass(MergeFileReduce.class);

        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            if (!fileStatus.isDirectory()) {
                //判断大小 及格式
                if (fileStatus.getLen() < 2 * 1014 * 1024 && fileStatus.getPath().getName().contains(".txt")) {

                    FileInputFormat.addInputPath(job, fileStatus.getPath());
                }
            }
        }

        //创建输出目录
        //Path outPath = new Path("/mergeout");
        //删除存在目录
        CDUPUtils.deleteFileName("/mergeout");

        //设置任务的输入路径

        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job, new Path("/mergeout"));
        //运行任务
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("读取文件内容：");
            CDUPUtils.readContent("/mergeout/part-r-00000");
        }else {
            System.out.println("文件读取失败");
        }

    }
}
