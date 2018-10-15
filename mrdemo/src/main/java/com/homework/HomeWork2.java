package com.homework;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

public class HomeWork2 {

    public static class HomeWork2Mapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] strings = line.split(",");
            String keyOut=strings[0];
            String valueOut=strings[2];
            context.write(new Text(keyOut),new Text(valueOut));
        }
    }
    public static class HomeWork2Reducer extends Reducer<Text,Text,Text,IntWritable>{
        int max=-1;
        Text maxRecord=null;
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            /*int max=-1;
            Text maxRecord=null;*/
            while (iterator.hasNext()){
                Text sale = iterator.next();
                if (Integer.parseInt(String.valueOf(sale))>max){
                    max=Integer.parseInt(String.valueOf(sale));
                    maxRecord=key;
                }else if (Integer.parseInt(String.valueOf(sale))==max){
                    maxRecord=new Text(maxRecord.toString()+","+key);
                }
            }
            System.out.println(maxRecord);
            context.write(maxRecord,new IntWritable(max));
        }

        /*@Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(maxRecord,new IntWritable(max));
        }*/
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "inverseindex");
        //设置job的运行类
        job.setJarByClass(HomeWork2.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(HomeWork2Mapper.class);
        job.setReducerClass(HomeWork2Reducer.class);

        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/join1/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        CDUPUtils.deleteFileName("/join1out");
        FileOutputFormat.setOutputPath(job, new Path("/join1out"));
        //运行任务
        boolean flag = job.waitForCompletion(true);
        if (flag){
            CDUPUtils.readContent("/join1out/part-r-00000");
        }else {
            System.out.println(flag);
        }

    }

}
