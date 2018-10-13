package com.day02;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * 用来处理reduce任务：合并（数据去重）
 * 在reduce端框架会将相同的key的value放在一个集合（迭代器）
 */
public class DRReducer extends Reducer<Text,NullWritable,Text,NullWritable>{
    //每次处理一个key,会被循环调用，有多少个key就会调用几次
    //获取map处理的数据 hello 集合（用于存储key相同的value--1）

    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        //将数据按照key进行排序
        /**
         * 怎样将去重的数据排序
         * todo
         */
        //1.将key的text类型转化为String
        String keyString = key.toString();
        //将字符串转化成int类型
        int keyInt = Integer.parseInt(keyString);

        //打印
        context.write(key,NullWritable.get());
    }

    //排序
    public static Map<String, String> sortMapByKey(Map<String, String> oriMap, final boolean isRise) {
        if (oriMap == null || oriMap.isEmpty()) {
            return null;
        }

        Map<String, String> sortMap = new TreeMap<String, String>(new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
                if (isRise) {
                    // 升序排序
                    return o1.compareTo(o2);
                } else {
                    // 降序排序
                    return o2.compareTo(o1);
                }
            }
        });
        sortMap.putAll(oriMap);
        return sortMap;
    }
}
