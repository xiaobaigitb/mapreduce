package cn.itxiaobai;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Iterator;

/**
 * 单词计数任务：wordcount
 */
public class WCJob {
    public static class WCMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
        /**
         *
         * @param key:读取一个文本文件
         * @param value：一行数据
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //将一行数据分割
            String[] words = value.toString().split(",");
            for (int i = 0; i < words.length; i++) {
                String word = words[i];

                context.write(new Text(word),new IntWritable(1));
            }
        }
    }

    public static class WCReducer extends Reducer<Text,IntWritable,Text,IntWritable>{
        /**
         *
         * @param key
         * @param values:迭代器中存放的是相同key的value值
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            Iterator<IntWritable> iterator = values.iterator();
            while (iterator.hasNext()){
                IntWritable one = iterator.next();
                count+=one.get();
            }
            context.write(key,new IntWritable(count));
        }
    }

    /**
     * 本地运行，不上传jar包
     */
    public static class WCJober{
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //创建配置
            Configuration configuration = new Configuration();
            //加载配置文件
            configuration.addResource(Resources.getResource("core-site-local.xml"));
            //设定一个任务
            Job job = Job.getInstance(configuration, "my word count local");
            //设置执行的job主类
            job.setJarByClass(WCJober.class);
            //设置Map的加载类以及输出类型
            job.setMapperClass(WCMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            //设置Reduce的加载类以及job/reduce的输出类型
            job.setReducerClass(WCReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);

            //创建任务输入与输出路径
            FileInputFormat.addInputPath(job,new Path("/wc"));
            //在输出路径之前判断一下是否存在，如果存在删除
            CDUPUtils.deleteFileName("/mywcout");
            FileOutputFormat.setOutputPath(job,new Path("/mywcout"));
            //执行任务，打印详情
            boolean flag = job.waitForCompletion(true);
            if (flag){
                System.out.println("文件读取内容如下：");
                CDUPUtils.readContent("/mywcout/part-r-00000");
            }else {
                System.out.println(flag+"文件加载失败....");
            }
        }
    }

}
