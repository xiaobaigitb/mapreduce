package com.day07;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class ScoreJob {
    public static class ScoreMapper extends Mapper<LongWritable,Text,ScoreWritable,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            String[] scores = value.toString().split(",");
            ScoreWritable score = new ScoreWritable(Integer.parseInt(scores[0]), Integer.parseInt(scores[1]), Integer.parseInt(scores[2]));
            context.write(score,NullWritable.get());
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "score");
        //设置job的运行类
        job.setJarByClass(ScoreJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(ScoreMapper.class);

        //map输出类型
        job.setMapOutputKeyClass(ScoreWritable.class);
        job.setMapOutputValueClass(NullWritable.class);


        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        //创建输出目录
        //Path outPath = new Path("/mergeout");
        //删除存在目录
        CDUPUtils.deleteFileName("/scoreout");

        //设置任务的输入路径

        FileInputFormat.addInputPath(job, new Path("/score/"));

        FileOutputFormat.setOutputPath(job, new Path("/scoreout"));
        //运行任务
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("读取文件内容：");
            CDUPUtils.readContent("/scoreout/part-r-00000");
        }else {
            System.out.println("文件加载失败");
        }
    }
}
