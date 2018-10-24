package com.zhiyou100;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * 1.继承UDF
 * 2.实现evaluate方法
 * 3.打包编译，上传
 * add jar /home/hadoop/lib/vincent-1.0-SNAPSHOT.jar;
 * list jars;
 * create temporary function uuid as 'com.zhiyou100.UUID';
 */
public class UUID extends UDF {
    //调用UUID类相当于调用了evaluate方法
    public String evaluate(){
        return java.util.UUID.randomUUID().toString();
    }
}
