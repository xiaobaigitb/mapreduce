package com.day03;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;

public class StudentApp {
    public static class StudentMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //获取文件名
            Path path = ((FileSplit) context.getInputSplit()).getPath();
            String fileName = path.toString();
            String valueString = value.toString();
            String[] strings = valueString.split(",");
            if (fileName.contains("class")){
                String keyout = strings[0];
                String valueout = strings[1];
                context.write(new Text(keyout),new Text(valueout));
            }else {
                String keyout = strings[2];
                String valueout = strings[0] + "," + strings[1];
                context.write(new Text(keyout),new Text(valueout));
            }
        }
    }

    public static class StudentReduce extends Reducer<Text,Text,Text,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            Iterator<Text> iterator = values.iterator();
            //定义一个集合用来存放key相同的value集合
            //[haman 1,zhangsan 2,lisi]
            ArrayList<String> list = new ArrayList<String>();
            while (iterator.hasNext()){
                list.add(iterator.next().toString());
            }
            //遍历集合
            //定义一个变量用来接收班级
            String className = null;
            for (int i = 0; i <list.size() ; i++) {
                if (!list.get(i).contains(",")){
                    className=list.get(i);
                    break;
                }
            }
            for (int i = 0; i <list.size() ; i++) {
                if (list.get(i).contains(",")){
                    //打印
                    context.write(new Text(list.get(i)+","+className),NullWritable.get());
                }
            }
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
        job.setMapperClass(StudentMapper.class);
        job.setReducerClass(StudentReduce.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/join"));
        //设置任务的输出路径--保存结果(这个目录必须是不存在的目录)
        //删除存在的文件
        CDUPUtils.deleteFileName("/joinout");

        FileOutputFormat.setOutputPath(job, new Path("/joinout"));
        //运行任务 true：表示打印详情
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("使用mapreduce来两张表连接操作的结果...");
            CDUPUtils.readContent("/joinout/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }
    }
}
