package com.homework;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * henan,zhengzhou,1000
 * henan,xinxiang,2000
 * hebei,shijiazhuang,500
 * hebei,handan,1200
 *
 * 用mr最大销售额的城市
 */
public class MarketApp {
    public static class MarketMapper extends Mapper<LongWritable,Text,Text,NullWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //key：表示文件---value：表示一行数据
            context.write(value,NullWritable.get());
        }
    }

    public static class MarketReduce extends Reducer<Text,NullWritable,Text,IntWritable> {
        private int max = 0;
        Text maxRecord = null;
        @Override
        protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            String[] strings = key.toString().split(",");
            int parseInt = Integer.parseInt(strings[2]);
            if (parseInt>max){
                max=parseInt;
                maxRecord=new Text(strings[1]);
            }else if (parseInt==max){
                maxRecord=new Text(maxRecord.toString()+","+strings[1]);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            context.write(maxRecord,new IntWritable(max));
        }
    }
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务,后面是job的名称
        Job job = Job.getInstance(coreSiteConf, "Reverse");
        //设置job的运行类，就是此类
        job.setJarByClass(MarketApp.class);

        //将打的jar包自动上传临时目录，运行之后就删除了--自己看不到
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");

        //设置Map和Reduce处理类
        job.setMapperClass(MarketMapper.class);
        job.setReducerClass(MarketReduce.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/work"));
        //设置任务的输出路径--保存结果(这个目录必须是不存在的目录)
        //删除存在的文件
        CDUPUtils.deleteFileName("/markout");

        FileOutputFormat.setOutputPath(job, new Path("/markout"));
        //运行任务 true：表示打印详情
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("使用mapreduce就每个省中最值的结果...");
            CDUPUtils.readContent("/markout/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }
    }

}
