package com.hbase.DDL;

import com.google.common.io.Resources;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.ArrayList;
import java.util.NavigableMap;

public class HBaseDDL {
    private static Configuration configuration;
    private static Connection connection;
    private static Admin admin;
    static {
        //1.获得Configuration实例并进行相关设置
        configuration = HBaseConfiguration.create();
        configuration.addResource(Resources.getResource("hbase-site.xml"));
        //2.获得Connection实例
        try {
            connection = ConnectionFactory.createConnection(configuration);
            //3.1获得Admin接口
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void main(String[] args) throws IOException {
        String  familyNames[]={"nature","social"};
        byte [][] splitKeys ={{'f'},{'l'},{'o'}};//f-----l-----o(临界值)
        //createTable("test_hbase",familyNames,splitKeys);
        //insert("test_hbase","luban","nature","height","250");
        //insert("test_hbase","libai","nature","height","1250");
        //dropTable("test_hbase");
        //delete("test_hbase", "libai");
        //testPutAll("test_hbase");
        //testGet("test_hbase","luban","nature","height");
        //testScan("test_hbase","libai","luban","nature","height");
        //testScanWithCacheAndBatch(5,4,"test_hbase","libai","luban","nature","height");
    }

    /**
     * 组合使用扫描器缓存和批量大小
     * @param caching
     * @param batch
     * @throws IOException
     */
    private static void testScanWithCacheAndBatch(int caching, int batch,String tableName,
                                                  String rowKey0,String rowKey1,
                                                  String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(rowKey0), Bytes.toBytes(rowKey1));
        //!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
        scan.setCaching(caching);
        scan.setBatch(batch);
        //!!!!!!!!!!!!!!!!!!!!!!!!!!
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner) {
            String r = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
            System.out.println(r);
        }
        table.close();
    }

    /**
     * 查看某个rowkey范围的数据，按字典顺序排序
     */
    private static void testScan(String tableName,String rowKey0,String rowKey1,String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Scan scan = new Scan(Bytes.toBytes(rowKey0), Bytes.toBytes(rowKey1));
        ResultScanner scanner = table.getScanner(scan);
        for(Result result : scanner) {
            String r = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
            System.out.println(r);
        }
        table.close();
    }

    /**
     * 查看某个cell的值
     */
    private static void testGet(String tableName,String rowKey,String family, String column) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        Result resut = table.get(get);
        String cell = Bytes.toString(resut.getValue(Bytes.toBytes(family), Bytes.toBytes(column)));
        System.out.println(cell);
        table.close();
    }

    /**
     * 批量插入数据
     */
    private static void testPutAll(String tableName) throws IOException {
        HTable table = new HTable(configuration, tableName);
        ArrayList<Put> puts = new ArrayList<Put>(1000);
        for (int i = 0; i < 10000; i++) {
            Put put = new Put(Bytes.toBytes(i));
            put.addColumn(Bytes.toBytes("nature"), Bytes.toBytes("height"), Bytes.toBytes("250"));
            put.addColumn(Bytes.toBytes("nature"), Bytes.toBytes("age"), Bytes.toBytes("25"));
            put.addColumn(Bytes.toBytes("nature"), Bytes.toBytes("money"), Bytes.toBytes("1250"));
            puts.add(put);
            //每个1000提交一次，提交提高性能
            if(i % 1000 == 0) {
                table.put(puts);
                System.out.println("已提交"+i+"条数据");
                puts = new ArrayList<Put>(1000);
            }
        }
        table.put(puts);
        table.close();
    }
    /**
     * 创建表
     * @param tableName 表名
     * @param familyNames 列族名
     * */
    public static void createTable(String tableName, String familyNames[],byte [][] splitKeys) throws IOException {
        //如果表存在退出
        if (admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println("Tableexists!");
            return;
        }
        //通过HTableDescriptor类来描述一个表，HColumnDescriptor描述一个列族
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));
        for (String familyName : familyNames) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(familyName);
            //设置版本数量
            hColumnDescriptor.setMaxVersions(3);
            tableDescriptor.addFamily(hColumnDescriptor);
        }
        //tableDescriptor.addFamily(new HColumnDescriptor(familyName));
        admin.createTable(tableDescriptor,splitKeys);
        //System.out.println(tableName+"创建成功");
        System.out.println("createtable success!");
    }

    /**
     * 删除表
     * @param tableName 表名
     * */
    public static void dropTable(String tableName) throws IOException {
        //如果表不存在报异常
        if (!admin.tableExists(TableName.valueOf(tableName))) {
            System.out.println(tableName+"不存在");
            return;
        }

        //删除之前要将表disable
        if (!admin.isTableDisabled(TableName.valueOf(tableName))) {
            admin.disableTable(TableName.valueOf(tableName));
        }
        admin.deleteTable(TableName.valueOf(tableName));
        //System.out.println(tableName+"删除成功");
        System.out.println("deletetable " + tableName + "-----ok.");
    }

    /**
     * 指定行/列中插入数据
     * @param tableName 表名
     * @param rowKey 主键rowkey
     * @param family 列族
     * @param column 列
     * @param value 值
     * TODO: 批量PUT
     */
    public static void insert(String tableName, String rowKey, String family, String column, String value) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table =connection.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        System.out.println("insertrecored " + rowKey + " totable " + tableName + "ok.");
        //System.out.println("数据导入成功");

        // 查询数据
        Get get = new Get(rowKey.getBytes());
        //get.addFamily(COLUMN_FAMILY_NAME.getBytes());                         // 获取特定列族的数据
        //get.addColumn(COLUMN_FAMILY_NAME.getBytes(), COLUMNS[1].getBytes());  // 获取特定列的数据
        Result result = table.get(get);

        NavigableMap<byte[], byte[]> familyMap = result.getFamilyMap(family.getBytes());
        for (byte[] key :
                familyMap.keySet()) {
            System.out.println(new String(key)+"---"+new String(familyMap.get(key)));
        }


    }

    /**
     * 删除表中的指定行
     * @param tableName 表名
     * @param rowKey rowkey
     * TODO: 批量删除
     */
    public static void delete(String tableName, String rowKey) throws IOException {
        //3.2获得Table接口,需要传入表名
        Table table = connection.getTable(TableName.valueOf(tableName));
        Delete delete = new Delete(Bytes.toBytes(rowKey));
        table.delete(delete);
    }
}
