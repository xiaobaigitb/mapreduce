package com.phoenix.day01;

import org.apache.zookeeper.server.quorum.CommitProcessor;

import java.sql.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Phoenix_Test {
    private static Connection conn=null;
    private static Statement statement = null;
    static {
        try {
            //phoenix4.14.0用下面的驱动对应hbase1.4.+
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            //这里配置zookeeper的地址，可单个，也可多个。可以是域名或者ip
            String url = "jdbc:phoenix:master2:2181/hbase";
            //String url = "jdbc:phoenix:41.byzoro.com,42.byzoro.com,43.byzoro.com:2181";
            conn = DriverManager.getConnection(url);
            statement = conn.createStatement();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    /**
     * 测试类
     * @param args
     * @throws SQLException
     */
    public static void main(String[] args) throws SQLException, ExecutionException, InterruptedException {
        //testRead();
        //testReadAll();
        testupsertAll(1000000);
        //testUpsert();
        //testCreateTable("student");
        //testDelete("student");
        //testSelect("student");
    }


    /**
     * 扫描全表
     * @param tableName
     * @throws SQLException
     */
    private static void testSelect(String tableName) throws SQLException {
        String sql="select * from "+tableName+"";
        statement.executeUpdate(sql);
        conn.commit();

        conn.close();
        statement.close();
    }

    /**
     * 单挑添加数据
     * @throws SQLException
     */
    private static void testUpsert() throws SQLException {
        String sql1="upsert into test_phoenix_api values(1,'test1')";
        //String sql2="upsert into test_phoenix_api values(2,'test2')";
        //String sql3="upsert into test_phoenix_api values(3,'test3')";
        statement.executeUpdate(sql1);
        //statement.executeUpdate(sql2);
        //statement.executeUpdate(sql3);
        conn.commit();
        System.out.println("数据已插入");

        statement.close();
        conn.close();
    }

    /**
     * 删除数据
     * @param tableName
     * @throws SQLException
     */
    private static void testDelete(String tableName) throws SQLException {
        String sql="delete from "+tableName+" where mykey = 1";
        statement.executeUpdate(sql);
        conn.commit();

        conn.close();
        statement.close();
    }

    /**
     * c创建表
     * @param tableName
     * @throws SQLException
     */
    private static void testCreateTable(String tableName) throws SQLException {
        String sql="create table "+tableName+"(mykey integer not null primary key ,mycolumn varchar )";
        statement.executeUpdate(sql);
        conn.commit();

        conn.close();
        statement.close();
    }

    /**
     * 批量插入
     * @param count
     * @throws SQLException
     */
    private static void testupsertAll(int count) throws SQLException, ExecutionException, InterruptedException {
        ExecutorService pool = Executors.newFixedThreadPool(5);
        Future<?> submit = pool.submit(new Runnable() {
            @Override
            public void run() {

                System.out.println(Thread.currentThread().getName()+"-------------------");

                    try {
                        for (int i = 0; i < 1000000; i++) {
                            String sql ="upsert into STUDENT_CON values("+i+",'lisi',12)";
                            statement.executeUpdate(sql);
                                conn.commit();
                        }
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
            }
        });
        submit.get();
//        for (int i = 0; i < count; i++) {
//            String sql ="upsert into STUDENT_CON values("+i+",'lisi',12)";
//            statement.executeUpdate(sql);
//            conn.commit();
//        }
        System.out.println(count+"条数据已插入完毕");
//        conn.close();
//        statement.close();

    }

    /**
     * 使用phoenix提供的api操作hbase中读取数据
     */
    private static void testReadAll() throws SQLException {
//String sql = "select count(1) as num from web_stat";
        String sql = "select *  from web_stat where core = 35";
        long time = System.currentTimeMillis();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            //获取core字段值
            int core = rs.getInt("core");
            //获取core字段值
            String host = rs.getString("host");
            //获取domain字段值
            String domain = rs.getString("domain");
            //获取feature字段值
            String feature = rs.getString("feature");
            //获取date字段值,数据库中字段为Date类型，这里代码会自动转化为string类型
            String date = rs.getString("date");
            //获取db字段值
            String db = rs.getString("db");
            System.out.println("host:"+host+"\tdomain:"+domain+"\tfeature:"+feature+"\tdate:"+date+"\tcore:" + core+"\tdb:"+db);
        }
        long timeUsed = System.currentTimeMillis() - time;
        System.out.println("time " + timeUsed + "mm");
        //关闭连接
        rs.close();
        statement.close();
        conn.close();
    }

    /**
     * 使用phoenix提供的api操作hbase读取数据
     */
    private static void testRead() throws SQLException {
        //Statement statement = conn.createStatement();
        String sql = "select count(1) as num from web_stat";
        long time = System.currentTimeMillis();
        ResultSet rs = statement.executeQuery(sql);
        while (rs.next()) {
            int count = rs.getInt("num");
            System.out.println("row count is " + count);
        }
        long timeUsed = System.currentTimeMillis() - time;
        System.out.println("time " + timeUsed + "mm");
        //关闭连接
        rs.close();
        statement.close();
        conn.close();
    }
}

