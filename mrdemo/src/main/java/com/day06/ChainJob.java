package com.day06;

import com.day01.*;
import com.day04.TeacherJob;
import com.day05.MergeFileJob;
import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

import static com.utils.CDUPUtils.deleteFileName;

public class ChainJob {
    public static void main(String[] args) throws IOException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        Job job1 = setJob1();
        //Job job2 = setJob2();
        Job job3 = setJob3();

        ControlledJob controlledJob1 = new ControlledJob(coreSiteConf);
        controlledJob1.setJob(job1);

//        ControlledJob controlledJob2 = new ControlledJob(coreSiteConf);
//        controlledJob2.setJob(job2);

        ControlledJob controlledJob3 = new ControlledJob(coreSiteConf);
        controlledJob3.setJob(job3);

        //controlledJob2.addDependingJob(controlledJob1);
        controlledJob3.addDependingJob(controlledJob1);

        JobControl jobControl = new JobControl("demo");
        jobControl.addJob(controlledJob1);
        //jobControl.addJob(controlledJob2);
        jobControl.addJob(controlledJob3);

        new Thread(jobControl).start();

        while (true){
            List<ControlledJob> jobList = jobControl.getRunningJobList();
            System.out.println(jobList);
            Thread.sleep(5000);
        }

    }

    private static Job setJob3() throws IOException {
        Configuration coreSiteConf = new Configuration();
        //加载配置文件
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //coreSiteConf.set(,);--在这里可以添加配置文件
        //设置一个任务（配置项）,后面是job的名称
        Job job3 = Job.getInstance(coreSiteConf, "wc");
        //设置job的运行类，就是此类
        //job3.setJarByClass(WCJob.class);
        job3.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置reduce的 个数
        job3.setNumReduceTasks(2);
        job3.setPartitionerClass(WCPartitioner.class);
        //设置Map和Reduce处理类
        job3.setMapperClass(WCMapper.class);
        job3.setCombinerClass(WCCombiner.class);
        job3.setReducerClass(WCReducer.class);
        //设置map输出类型
        job3.setMapOutputKeyClass(Text.class);
        job3.setMapOutputValueClass(IntWritable.class);
        //设置job/reduce输出类型
        job3.setOutputKeyClass(Text.class);
        job3.setOutputValueClass(IntWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job3, new Path("/mergeout"));
        //设置任务的输出路径--保存结果(这个目录必须是不存在的目录)
        //删除存在的文件
        CDUPUtils.deleteFileName("/wcout");

        FileOutputFormat.setOutputPath(job3, new Path("/wcout"));
        //运行任务 true：表示打印详情
        return job3;
    }

    private static Job setJob2() throws IOException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //设置一个任务
        Job job2 = Job.getInstance(coreSiteConf, "maxsale");
        //设置job的运行类
        //job2.setJarByClass(TeacherJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        job2.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job2.setMapperClass(TeacherJob.LineCountMapper.class);
        // job.setReducerClass(MaxSaleReducer.class);*/

        //map输出类型
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(NullWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job2, new Path("/mergeout/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        //删除存在的文件
        deleteFileName("/teachout");

        FileOutputFormat.setOutputPath(job2, new Path("/teachout/"));
        //读取文件
        //CDUPUtils.readContent("/teachout/part-r-00000");
        return job2;
    }

    //文件合并
    private static Job setJob1() throws IOException {
        Configuration coreSiteConf = new Configuration();

        //coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //设置一个任务
        Job job1 = Job.getInstance(coreSiteConf, "merge");
        //设置job的运行类
        //job.setJarByClass(MergeFileJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        job1.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT-jar-with-dependencies.jar");
        //设置Map和Reduce处理类
        job1.setMapperClass(MergeFileJob.MergeFileMapper.class);
        job1.setReducerClass(MergeFileJob.MergeFileReduce.class);

        //map输出类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);


        FileSystem fileSystem = FileSystem.get(coreSiteConf);

        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            if (!fileStatus.isDirectory()) {
                //判断大小及格式
                if (fileStatus.getLen() < 2 * 1014 * 1024 && fileStatus.getPath().getName().contains(".txt")) {

                    FileInputFormat.addInputPath(job1, fileStatus.getPath());
                }
            }
        }

        //创建输出目录
        //Path outPath = new Path("/mergeout");
        //删除存在目录
        deleteFileName("/mergeout");

        //设置任务的输入路径

        // FileInputFormat.addInputPath(job, new Path("/wc/"));

        FileOutputFormat.setOutputPath(job1, new Path("/mergeout"));
        return job1;

    }
}

