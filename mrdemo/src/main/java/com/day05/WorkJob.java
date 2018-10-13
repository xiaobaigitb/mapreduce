package com.day05;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WorkJob {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://hadoop02:9000");
        conf.addResource(Resources.getResource("core-site-master2.xml"));
        //System.setProperty("HADOOP_USER_NAME", "hadoop");
        Job job = Job.getInstance(conf, "combine small files to bigfile");

        //job.setJarByClass(WorkJob.class);
        job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT-jar-with-dependencies.jar");
        //job.setNumReduceTasks(2);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);

        job.setMapperClass(WorkMapper.class);
        job.setReducerClass(WorkReduce.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // job.setOutputFormatClass(SequenceFileOutputFormat.class);


        FileSystem ff = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> files = ff.listFiles(new Path("/"), true);
        while (files.hasNext()){
            LocatedFileStatus fileStatus = files.next();
            if (!fileStatus.isDirectory()){
                //判断大小及格式
                if (fileStatus.getLen()<2*1024*1024&&fileStatus.getPath().getName().contains(".txt")){
                    FileInputFormat.setInputPaths(job, fileStatus.getPath());
                }
            }
        }


        //Path input = new Path("/smallfiles");
        Path output = new Path("/bigfile");

        //FileInputFormat.setInputPaths(job, input);
        FileSystem fs = FileSystem.get(conf);
        if (fs.exists(output)) {
            System.out.println(output+"文件存在，正在删除");
            fs.delete(output, true);
            System.out.println(output+"删除成功");
        }
        FileOutputFormat.setOutputPath(job, output);
        boolean flag = job.waitForCompletion(true);

        if (flag){
            System.out.println(flag+",合并的大文件内容");
//            for (int i = 0; i < 2; i++) {
//                CDUPUtils.readContent(output+"/part-r-0000"+i);
//            }
            CDUPUtils.readContent(output+"/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }
    }
}
