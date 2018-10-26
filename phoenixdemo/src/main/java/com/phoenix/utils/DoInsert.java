package com.phoenix.utils;

import java.util.concurrent.CountDownLatch;
        //import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;

public class DoInsert {
          public static void main(String[] args) {
    long startTimes = System.currentTimeMillis();
    int threadCount = 10;
    int total = 50000;
    int every = total/threadCount;
    int flag = 0;
    int ch = 0;
    final CountDownLatch latch = new CountDownLatch(threadCount);
    //传入参数
    String url = args[0];     //传入数据库url
    String dataBase = args[1];  //传入数据库名
    String user = args[2];    //传入用户名
    String pwd = args[3];     //传入用户密码
    String table = args[4];    //传入表名
    String path = args[5];    //传入本地文件地址
    String deleteFlag = args[6]; //是否在插入前删除表中的数据(true or false)
    System.out.println("deleteFlag is: "+deleteFlag);
    if(deleteFlag.equals("true") | deleteFlag.equals("True")){
        JdbcUtils.dropData(dataBase,url,user,pwd,table);
    }
    for(int i=0;i<threadCount;i++){
      Thread thread = new Thread(new Worker(dataBase,latch,i*every,(i+1)*every,flag++,ch++,path,user,pwd,table,url));
      thread.start();
    }
    try {
      latch.await();
      long endTimes = System.currentTimeMillis();
      System.out.println("所有线程执行完毕,用时为 :" + (endTimes - startTimes)/1000 + "s");
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }



}



class Worker implements Runnable{

          int start = 0;
          int end = 0;
          int flag;
  int ch;
  String dataBase;
  String path;
  String user;
  String pwd;
  String table;
  String url;

          CountDownLatch latch;
              public Worker(String dataBase,CountDownLatch latch, int start,int end,int flag,int ch,String path,String user,String pwd,String table,String url){
    this.start = start;
    this.end = end;
    this.latch = latch;
    this.flag = flag;
    this.ch = ch;
    this.dataBase = dataBase;
    this.path = path;
    this.user = user;
    this.pwd = pwd;
    this.table = table;
    this.url = url;
  }

              @Override
              public synchronized void run() {
    System.out.println("线程" + Thread.currentThread().getName()+ "正在执行。。");
    JdbcUtils.writeToDat(dataBase,path,url,user,pwd,table,start,end,flag,ch);
    latch.countDown();
  }

}

