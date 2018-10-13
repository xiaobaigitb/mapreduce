package cn.itxiaobai;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.List;

public class WcDMergeChainJob {
    public static void main(String[] args) throws IOException, InterruptedException {
        //创建配置
        Configuration configuration = new Configuration();
        //加载配置文件
        configuration.addResource(Resources.getResource("core-site-local.xml"));
        //合并小文件
        Job job1 = setJob1();
        //单词计数
        Job job2 = setJob2();

        ControlledJob controlledJob1 = new ControlledJob(configuration);
        controlledJob1.setJob(job1);

        ControlledJob controlledJob2 = new ControlledJob(configuration);
        controlledJob2.setJob(job2);

        //设置依赖关系
        controlledJob2.addDependingJob(controlledJob1);

        JobControl jobControl = new JobControl("word count depend merge small file");

        jobControl.addJob(controlledJob1);
        jobControl.addJob(controlledJob2);
        //开启线程执行jobControl
        new Thread(jobControl).start();

        //打印正在执行线程的详情
        while (true){
            List<ControlledJob> jobList = jobControl.getRunningJobList();
            System.out.println(jobList);
            Thread.sleep(5000);
        }
    }

    private static Job setJob2() throws IOException {
        //创建配置
        Configuration configuration = new Configuration();
        //加载配置文件
        configuration.addResource(Resources.getResource("core-site-local.xml"));
        //设定一个任务
        Job job2 = Job.getInstance(configuration, "my word count local");
        //设置执行的job主类
        job2.setJarByClass(WCJob.WCJober.class);
        //设置Map的加载类以及输出类型
        job2.setMapperClass(WCJob.WCMapper.class);
        job2.setMapOutputKeyClass(Text.class);
        job2.setMapOutputValueClass(IntWritable.class);
        //设置Reduce的加载类以及job/reduce的输出类型
        job2.setReducerClass(WCJob.WCReducer.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);

        //创建任务输入与输出路径
        FileInputFormat.addInputPath(job2,new Path("/mymergeout"));
        //在输出路径之前判断一下是否存在，如果存在删除
        CDUPUtils.deleteFileName("/mywcout");
        FileOutputFormat.setOutputPath(job2,new Path("/mywcout"));
        return job2;
    }

    private static Job setJob1() throws IOException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job job1 = Job.getInstance(coreSiteConf, "my small merge big file");
        //设置job的运行类
        job1.setJarByClass(MergeSmallFileJob.MyJob.class);

        //设置Map和Reduce处理类
        job1.setMapperClass(MergeSmallFileJob.MergeSmallFileMapper.class);
        job1.setReducerClass(MergeSmallFileJob.MergeSmallFileReduce.class);

        //map输出类型
        job1.setMapOutputKeyClass(Text.class);
        job1.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);

        FileSystem fileSystem = FileSystem.get(coreSiteConf);
        //listFiles:可以迭代便利文件
        RemoteIterator<LocatedFileStatus> listFiles = fileSystem.listFiles(new Path("/"), true);
        while (listFiles.hasNext()) {
            LocatedFileStatus fileStatus = listFiles.next();
            Path filesPath = fileStatus.getPath();
            if (!fileStatus.isDirectory()) {
                //判断大小 及格式
                if (fileStatus.getLen() < 2 * 1014 * 1024 && filesPath.getName().contains(".txt")) {
                    //文件输入路径
                    FileInputFormat.addInputPath(job1,filesPath);
                }
            }
        }

        //删除存在目录
        CDUPUtils.deleteFileName("/mymergeout");

        FileOutputFormat.setOutputPath(job1, new Path("/mymergeout"));
        return job1;
    }
}
