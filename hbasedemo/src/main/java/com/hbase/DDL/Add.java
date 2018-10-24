package com.hbase.DDL;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class Add {
    public static void main(String[] args) throws IOException {
        Configuration conf = null;
        conf = HBaseConfiguration.create();
        conf.addResource(Resources.getResource("hbase-site.xml"));
        HBaseAdmin admin = new HBaseAdmin(conf);
        HTableDescriptor htd = new HTableDescriptor(TableName.valueOf("people1"));
        HColumnDescriptor hcdInfo = new HColumnDescriptor("info");
        HColumnDescriptor hcdData = new HColumnDescriptor("data");
        hcdInfo.setMaxVersions(3);
        htd.addFamily(hcdInfo);
        htd.addFamily(hcdData);
        admin.createTable(htd);
        System.out.println("success");
        admin.close();
    }
}
