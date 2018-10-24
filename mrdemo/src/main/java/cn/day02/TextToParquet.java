package cn.day02;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class TextToParquet {
    public static void main(String[] args) throws IOException {
        MessageType schema = MessageTypeParser.parseMessageType("message Pair {\n" +
                " required binary ip (UTF8);\n" +
                " required binary date (UTF8);\n" +
                " required binary method (UTF8);\n" +
                " required binary url (UTF8);\n" +
                " required int32 status;\n" +
                "}");


        GroupFactory factory = new SimpleGroupFactory(schema);

        Path path = new Path("d:\\test\\access_2016_04_28.parquet");

        Configuration configuration = new Configuration();

        GroupWriteSupport writeSupport = new GroupWriteSupport();

        writeSupport.setSchema(schema, configuration);



        ParquetWriter<Group> writer = new ParquetWriter<Group>(path, configuration, writeSupport);
//把本地文件读取进去，用来生成parquet格式文件
        BufferedReader br = new BufferedReader(new FileReader(new File("d:\\test\\access_2016_04_28.log")));
        String line = "";



        while ((line=br.readLine())!=null){
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
                String paths = matcher.group(3);
                String status  = matcher.group(4);
                //对路径进行拆分
                String[] strings = paths.split(" ");
                if (strings!=null&&strings.length>=2){
                    String method = strings[0].substring(1);
                    String url = strings[1];
                    //context.write(new Text(ip+","+date+","+method+","+url+","+status),NullWritable.get());

                    Group group = factory.newGroup()
                            .append("ip",ip)
                            .append("date",date)
                            .append("method",method)
                            .append("url",url)
                            .append("status",Integer.parseInt(status));
                    writer.write(group);
                }
            }
        }
        System.out.println("write end");
        writer.close();
    }

}
