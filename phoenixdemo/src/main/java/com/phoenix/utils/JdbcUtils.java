package com.phoenix.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Vector;
import java.util.ArrayList;

public class JdbcUtils {
          static String driver = "com.mysql.jdbc.Driver";
          static Vector<Connection> pools = new Vector<Connection>();
          private static String url_unic = "?useUnicode=true&characterEncoding=utf8";

          public static Connection getDBConnection(String url,String dataBase,String user,String pwd){
    try {
      //1.加载驱动
      Class.forName(driver);
      //2.取得数据库连接
      String url_total = url+"/"+dataBase+url_unic;
      System.out.println(url_total);
      Connection conn = DriverManager.getConnection(url_total, user, pwd);
      return conn;
    } catch (SQLException e) {
      e.printStackTrace();
    } catch (ClassNotFoundException e) {
      e.printStackTrace();
    }
    return null;
  }

            //关闭数据库连接
              public static void closeCon(Connection con){
    if(con != null){
    	try{
      con.close();
    	  }catch(Exception e){
    	  	e.printStackTrace();
      	}
    }
  }

          public static void insertIntoTable(String url,String user,String pwd,String table,List<String> list,Connection conn,PreparedStatement pstmt,int start,int end){
    try {
            String sql = "insert into " + table + " (" + "inner_account_id,account_id,custom_id,custom_cname,account_type,is_internal,"
				   + "first_login_date,last_date,account_mode,is_special_line,account_status,custom_type_id,iwind_type,has_auxiliary"
				   + ")" + " values " + "(?,?,?,?,?,?,?,?,?,?,?,?,?,?)";
            pstmt = conn.prepareStatement(sql);
            for(String str:list.subList(start, end)){
			  String[] s = str.split("\t");
                pstmt.setInt(1,Integer.parseInt(s[0]));
                pstmt.setString(2,s[1]);
                pstmt.setString(3,s[2]);
                pstmt.setString(4,s[3]);
                pstmt.setString(5,s[4]);
                pstmt.setString(6,s[5]);
                pstmt.setString(7,s[6]);
                pstmt.setString(8,s[7]);
                pstmt.setString(9,s[8]);
                pstmt.setString(10,s[9]);
                pstmt.setString(11,s[10]);
                pstmt.setString(12,s[11]);
                pstmt.setString(13,s[12]);
                pstmt.setString(14,s[13]);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
            conn.commit();
            System.out.println("insert data success!");
	  } catch(SQLException e){
	  	 e.printStackTrace();
	  	 System.out.println("insert data fail!");
	  }
  }
    //删除表中数据
    public static void dropData(String dataBase,String url,String user,String pwd,String table){
        Connection conn = getDBConnection(url,dataBase,user,pwd);
        try{
            String sql = "delete from "+ table;
            //System.out.println(sql);
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.executeUpdate();
            System.out.println("delete data success!");
        } catch(SQLException e){
            e.printStackTrace();
            System.out.println("delete data fail!");
        }finally{
            closeCon(conn);
        }
    }

    //读取文本文件
    public static void writeToDat(String dataBase,String path,String url,String user,String pwd,String table,int start,int end,int flag,int ch){
        File file = new File(path);
        List<String> list = new ArrayList<String>();
        Connection conn = null;
        PreparedStatement pstmt = null;
        try {
            InputStreamReader isr = new InputStreamReader(new FileInputStream(file), "utf8");
            BufferedReader bw = new BufferedReader(isr);
            String line = null;
            conn = getDBConnection(url,dataBase,user,pwd);
            conn.setAutoCommit(false);
            int count = 0;
            while((line = bw.readLine()) != null){
                //System.out.println(line);
                list.add(line);


                if(list.size() == 50000){
                    insertIntoTable(url,user,pwd,table,list,conn,pstmt,start,end);
                    list.clear();
                    System.out.println("已成功插入： "+ (++count)*50000 + "条数据" );
                }
            }
            if(list.size() < 50000){
		    int total = list.size();
		    int every = (int)total/10;
		    int[] s ={0,0,0,0,0,0,0,0,0,(total%10)};
                insertIntoTable(url,user,pwd,table,list,conn,pstmt,(flag)*every,((flag+1)*every+s[ch]));
                System.out.println(flag);
                System.out.println(s[ch]);
                //list.clear();
                System.out.println("OK,已成功插入全部数据!");
            }
            bw.close();
		 }catch(FileNotFoundException e){
			 e.printStackTrace();
		 }catch(SQLException e){
			 e.printStackTrace();
		 }catch(IOException e){
			 e.printStackTrace();
		 } finally{
            if (pstmt != null){
                try{
                    pstmt.close();
                }catch(SQLException e){
                    e.printStackTrace();
                }
            }
            if(conn != null){
                closeCon(conn);
            }
        }
    }
}
