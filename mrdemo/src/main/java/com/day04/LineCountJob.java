package com.day04;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
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
import java.sql.*;
import java.util.ArrayList;
import java.util.Iterator;

public class LineCountJob {
    private static final Log LOG = LogFactory.getLog(Job.class);
    public static class LineCountMapper extends Mapper<LongWritable,Text,Text,NullWritable>{

        Connection conn;
        PreparedStatement statement;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {

            try {

                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://192.168.203.72:3306/test", "root", "123");
                statement = conn.prepareStatement("update line_num set `count`=`count`+1 where id=1");
            }catch (ClassNotFoundException e){
                e.printStackTrace();
                LOG.info(e);
                //System.exit(-1);
            }catch (SQLException e){
                LOG.info(e);
                e.printStackTrace();
                //System.exit(-1);
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                conn.close();
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            try {
                statement.executeUpdate();
                System.out.println("count++");
                context.write(new Text("line"),NullWritable.get());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.addResource(Resources.getResource("core-site-master2.xml"));
        //设置一个任务,后面是job的名称
        Job job = Job.getInstance(coreSiteConf, "Reverse");
        //job.setJarByClass(TeacherJob.class);
        //将打的jar包自动上传临时目录，运行之后就删除了--自己看不到
        job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT-jar-with-dependencies.jar");

        //设置Map和Reduce处理类
        job.setMapperClass(LineCountMapper.class);
        //job.setReducerClass(StudentReduce.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        //设置job/reduce输出类型
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(NullWritable.class);
        //设置任务的输入路径

        FileInputFormat.addInputPath(job, new Path("/work"));
        //设置任务的输出路径--保存结果(这个目录必须是不存在的目录)
        //删除存在的文件
        CDUPUtils.deleteFileName("/lineout");

        FileOutputFormat.setOutputPath(job, new Path("/lineout"));
        //运行任务 true：表示打印详情
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("使用mapreduce来计算行数...");
            CDUPUtils.readContent("/lineout/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }
    }
}
