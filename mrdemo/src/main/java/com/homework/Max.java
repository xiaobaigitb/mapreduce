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

/**
 * henan,zhengzhou,1000
 * henan,xinxiang,2000
 * hebei,shijiazhuang,500
 * hebei,handan,1200
 *
 *
 * 用mr求每个省的最大销售额
 */
public class Max {
    public static class MaxMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        //定义一个最大值
        private int max = 0;
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //key：表示文件---value：表示一行数据
            String[] strings = value.toString().split(",");
            int parseInt = Integer.parseInt(strings[2]);
            //int valueMax = Math.max(parseInt,max);
            if (parseInt> max)
                max =parseInt;
            context.write(new Text("Max:"),new IntWritable(max));
        }
    }

    public static class MaxReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        private int max = 0;
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            for (IntWritable val : values) {
                //int ValueMax = Math.max(val.get(), maxNum);
                if ( val.get() > max) {
                    max = val.get();
                }
            }
            context.write(key,new IntWritable(max));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //设置一个任务,后面是job的名称
        Job job = Job.getInstance(coreSiteConf, "Reverse");
        //将打的jar包自动上传临时目录，运行之后就删除了--自己看不到
        job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");

        //设置Map和Reduce处理类
        job.setMapperClass(MaxMapper.class);
        job.setReducerClass(MaxReduce.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/work"));
        //设置任务的输出路径--保存结果(这个目录必须是不存在的目录)
        //删除存在的文件
        CDUPUtils.deleteFileName("/maxout");

        FileOutputFormat.setOutputPath(job, new Path("/maxout"));
        //运行任务 true：表示打印详情
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("使用mapreduce就每个省中最值的结果...");
            CDUPUtils.readContent("/maxout/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }
    }
}
