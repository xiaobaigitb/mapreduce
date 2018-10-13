package cn.itxiaobai;

import com.day05.MergeFileJob;
import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 合并小文件的任务（2M一下属于小文件）
 */
public class MergeSmallFileJob {

    public static class MergeSmallFileMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
           //将文件名作为key，内容作为value输出
           //1.获取文件名
            FileSplit inputSplit = (FileSplit) context.getInputSplit();
            String fileName = inputSplit.getPath().getName();
            //打印文件名以及与之对应的内容
            context.write(new Text(fileName),value);
        }
    }

    public static class MergeSmallFileReduce extends Reducer<Text,Text,Text,Text>{
        /**
         *
         * @param key:文件名
         * @param values：一个文件的所有内容
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //将迭代器中的内容拼接
            Iterator<Text> iterator = values.iterator();
            //使用StringBuffer
            StringBuffer stringBuffer = new StringBuffer();
            while (iterator.hasNext()){
                stringBuffer.append(iterator.next()).append(",");
            }
            //打印
            context.write(key,new Text(stringBuffer.toString()));
        }
    }

    public static class MyJob{
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            Configuration coreSiteConf = new Configuration();
            coreSiteConf.addResource(Resources.getResource("core-site-local.xml"));
            //设置一个任务
            Job job = Job.getInstance(coreSiteConf, "my small merge big file");
            //设置job的运行类
            job.setJarByClass(MyJob.class);

            //设置Map和Reduce处理类
            job.setMapperClass(MergeSmallFileMapper.class);
            job.setReducerClass(MergeSmallFileReduce.class);

            //map输出类型
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            //设置job/reduce输出类型
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);

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
                        FileInputFormat.addInputPath(job,filesPath);
                    }
                }
            }

            //删除存在目录
            CDUPUtils.deleteFileName("/mymergeout");

            FileOutputFormat.setOutputPath(job, new Path("/mymergeout"));
            //运行任务
            boolean flag = job.waitForCompletion(true);
            if (flag){
                System.out.println("文件读取内容如下：");
                CDUPUtils.readContent("/mymergeout/part-r-00000");
            }else {
                System.out.println("文件加载失败....");
            }

        }
    }
}

