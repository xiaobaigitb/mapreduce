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

/**
 * henan,zhengzhou,1000
 * henan,xinxiang,2000
 * hebei,shijiazhuang,500
 * hebei,handan,1200
 *
 * 用mr求每个省的平均销售额
 */
public class AVE{

    public static class AVEMapper extends Mapper<LongWritable,Text,Text,IntWritable>{

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] strings = value.toString().split(",");
            String keyout = strings[0];
            String valueout = strings[2];
            //将字符串转化为int
            int valueInt = Integer.parseInt(valueout);

            context.write(new Text(keyout),new IntWritable(valueInt));
        }
    }

    public static class AVEReduce extends Reducer<Text,IntWritable,Text,IntWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            Iterator<IntWritable> iterator = values.iterator();
            int count = 0;
            int sum = 0;
            while (iterator.hasNext()){
                sum+=iterator.next().get();
                count++;
            }
            int ave = sum/count;

            context.write(key,new IntWritable(ave));
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
        job.setMapperClass(AVEMapper.class);
        job.setReducerClass(AVEReduce.class);

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
        CDUPUtils.deleteFileName("/aveout");

        FileOutputFormat.setOutputPath(job, new Path("/aveout"));
        //运行任务 true：表示打印详情
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("使用mapreduce就每个省的平均值的结果...");
            CDUPUtils.readContent("/aveout/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }
    }

}
