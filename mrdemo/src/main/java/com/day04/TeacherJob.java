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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/*
*@ClassName:DistinctJob
 @Description:TODO
 @Author:
 @Date:2018/10/10 14:04
 @Version:v1.0
*/
//在每个mapper 将行数保存在myql数据库

public class TeacherJob {
    private static final Log LOG = LogFactory.getLog(LineCountJob.class);

    public static class LineCountMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

        Connection conn;
        PreparedStatement statement ;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            try {
                Class.forName("com.mysql.jdbc.Driver");
                conn = DriverManager.getConnection("jdbc:mysql://192.168.203.72:3306/test", "root", "123");
                statement = conn.prepareStatement("update line_num set `count`=`count`+1 where id=1");
            } catch (ClassNotFoundException e) {
                LOG.info(e);
                //System.exit(-1);
                e.printStackTrace();
            } catch (SQLException e) {
                LOG.info(e);
                //System.exit(-1);
                e.printStackTrace();
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            try {
                statement.close();
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            try {
                statement.executeUpdate();
                System.out.println("count");
                context.write(new Text("teach"),NullWritable.get());
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

    }



    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();

        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务
        Job job = Job.getInstance(coreSiteConf, "maxsale");
        //设置job的运行类
        job.setJarByClass(TeacherJob.class);
        //mrdemo/target/mrdemo-1.0-SNAPSHOT.jar
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");
        //设置Map和Reduce处理类
        job.setMapperClass(LineCountMapper.class);
        // job.setReducerClass(MaxSaleReducer.class);*/

        //map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);
        //设置job/reduce输出类型
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(NullWritable.class);

        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/work/"));
        // FileInputFormat.addInputPath(job, new Path("/wc/"));
        //删除存在的文件
        CDUPUtils.deleteFileName("/teachout");

        FileOutputFormat.setOutputPath(job, new Path("/teachout/"));
        //运行任务
        boolean flag = job.waitForCompletion(true);
        if (flag){
            System.out.println("使用mapreduce来计算行数...");
            CDUPUtils.readContent("/teachout/part-r-00000");
        }else {
            System.out.println(flag+",读取文件失败");
        }

    }


}
