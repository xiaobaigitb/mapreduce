package cn.day01;

import com.google.common.io.Resources;
import com.utils.CDUPUtils;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 使用mapreduce对文本数据做预处理（转化为结构化表格）
 */
public class Dedup {
    public static void main(String[] args) {
        String line = "110.75.173.48 - - [28/April/2016:23:59:58 +0800] \"GET /thread-36410-1-9.html HTTP/1.1\" 200 68628";
        //使用正则表达
        //String reg = "(\\d+\\.\\d+\\.\\d+\\.\\d+).*(\\d+\\/\\S+).*(\\\".*\\\")\\s(\\S+).*";
        String reg = "(\\d+\\.\\d+\\.\\d+\\.\\d+).*(\\d+\\/\\S+).*(\\\".*\\\")\\s(\\S+).*";
        // 编译正则表达式
        Pattern pattern = Pattern.compile(reg);
        // 忽略大小写的写法
        // Pattern pat = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
        Matcher matcher = pattern.matcher(line);
        // 检查所有的结果
        boolean b = matcher.find();
        if (b){
            System.out.println(matcher.group(1));
            System.out.println(matcher.group(2));
            System.out.println(matcher.group(3));
            System.out.println(matcher.group(4));
        }
        System.out.println(b);
    }


    public static class DedupMapper extends Mapper<LongWritable,Text,Text,NullWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //super.map(key, value, context);
            String line = value.toString();
            //使用正则表达
            String reg = "(\\d+\\.\\d+\\.\\d+\\.\\d+).*(\\d+\\/\\S+).*(\\\".*\\\")\\s(\\S+).*";
            // 编译正则表达式
            Pattern pattern = Pattern.compile(reg);
            // 忽略大小写的写法
            // Pattern pat = Pattern.compile(regEx, Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(line);
            // 检查所有的结果
            boolean flag = matcher.find();
            if (flag&&matcher.groupCount()==4){
                String ip = matcher.group(1);
                String date = matcher.group(2);
                String path = matcher.group(3);
                String status  = matcher.group(4);
                //对路径进行拆分
                String[] strings = path.split(" ");
                if (strings!=null&&strings.length>=2){
                    String method = strings[0].substring(1);
                    String url = strings[1];
                    context.write(new Text(ip+","+date+","+method+","+url+","+status),NullWritable.get());
                }
            }

        }
    }

    public static class DedupJob{
        public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
            //创建配置
            Configuration configuration = new Configuration();
            //加载配置文件
            configuration.addResource(Resources.getResource("core-site-local.xml"));
            //设定一个任务
            Job job = Job.getInstance(configuration, "my word count local");
            //设置执行的job主类
            job.setJarByClass(DedupJob.class);
            //设置Map的加载类以及输出类型
            job.setMapperClass(DedupMapper.class);
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(NullWritable.class);
            //创建任务输入与输出路径
            FileInputFormat.addInputPath(job,new Path("/log"));
            //在输出路径之前判断一下是否存在，如果存在删除
            CDUPUtils.deleteFileName("/olgout");
            FileOutputFormat.setOutputPath(job,new Path("/olgout"));
            //执行任务，打印详情
            boolean flag = job.waitForCompletion(true);
            if (flag){
                System.out.println("文件读取内容如下：");
                CDUPUtils.readContent("/olgout/part-r-00000");
            }else {
                System.out.println(flag+"文件加载失败....");
            }
        }
    }
}

