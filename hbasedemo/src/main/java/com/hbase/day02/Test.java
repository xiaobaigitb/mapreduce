package com.hbase.day02;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * 查看建表结果：执行 scan 'hbase:meta'
 * 以上我们只是显示了部分region的信息，可以看到region的start-end key 还是比较随机散列的。同样可以查看hdfs的目录结构,的确和预期的38个预分区一致
 * 就已经按hash方式，预建好了分区，以后在插入数据的时候，也要按照此rowkeyGenerator的方式生成rowkey,有兴趣的话，也可以做些试验，插入些数据，看看数据的分布。
 * 方法没完成
 * @author mc
 *
 */
public class Test {
    public static void main(String[] args) throws Exception {
        testHashAndCreateTable();
    }


    public static void testHashAndCreateTable() throws Exception{
        HashChoreWoker worker = new HashChoreWoker(1000000,10);
        byte [][] splitKeys = worker.calcSplitKeys();

        HBaseAdmin admin = new HBaseAdmin(HBaseConfiguration.create());
        TableName tableName = TableName.valueOf("hash_split_table");

        if (admin.tableExists(tableName)) {
            try {
                admin.disableTable(tableName);
            } catch (Exception e) {
            }
            admin.deleteTable(tableName);
        }

        HTableDescriptor tableDesc = new HTableDescriptor(tableName);
        HColumnDescriptor columnDesc = new HColumnDescriptor(Bytes.toBytes("info"));
        columnDesc.setMaxVersions(1);
        tableDesc.addFamily(columnDesc);

        admin.createTable(tableDesc ,splitKeys);
        System.out.println(tableName+":createSuccess");

        admin.close();
    }
}
