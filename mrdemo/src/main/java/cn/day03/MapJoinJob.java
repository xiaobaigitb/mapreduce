package cn.day03;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
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
import java.util.HashMap;
import java.util.Iterator;

public class MapJoinJob {
    public static class MapJoinMapper extends Mapper<LongWritable,Text,Text,Text>{
        private HashMap<String,String> smallTable = new HashMap<String, String>();

        @Override
        protected void setup(Context context) throws IOException {
            Configuration configuration = context.getConfiguration();
            String path = configuration.get("smallTable.Path");
            System.out.println(path);
            FileSystem fileSystem = FileSystem.get(configuration);
            FSDataInputStream dataInputStream = fileSystem.open(new Path(path));
            String line = null;
            while ((line=dataInputStream.readLine())!=null){
                String[] split = line.split(",");
                smallTable.put(split[1],line);
            }
            dataInputStream.close();
            //fileSystem.close();

        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            String[] split = value.toString().split(",");
            String smallTables = smallTable.get(split[0]);
                context.write(new Text(smallTables+","+value),new Text());


//            //获取文件名
//            Path path = ((FileSplit) context.getInputSplit()).getPath();
//            String fileName = path.toString();
//            String valueString = value.toString();
//            String[] strings = valueString.split(",");
//            if (fileName.contains("class")){
//                String keyout = strings[0];
//                String valueout = strings[1];
//                context.write(new Text(keyout),new Text(valueout));
//            }else {
//                String keyout = strings[2];
//                String valueout = strings[0] + "," + strings[1];
//                context.write(new Text(keyout),new Text(valueout));
//            }
//        }
    }
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration coreSiteConf = new Configuration();
        coreSiteConf.set("smallTable.Path","/join/class.txt");

        coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
        //设置一个任务,后面是job的名称
        Job job = Job.getInstance(coreSiteConf, "Reverse");
        job.setJarByClass(MapJoinJob.class);
        //将打的jar包自动上传临时目录，运行之后就删除了--自己看不到
        //job.setJar("mrdemo/target/mrdemo-1.0-SNAPSHOT.jar");

        //设置Map和Reduce处理类
        job.setMapperClass(MapJoinMapper.class);
        //job.setReducerClass(StudentReduce.class);

        //设置map输出类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        //设置job/reduce输出类型
        //job.setOutputKeyClass(Text.class);
        //job.setOutputValueClass(NullWritable.class);
        //设置任务的输入路径
        FileInputFormat.addInputPath(job, new Path("/join/student.txt"));
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
}
