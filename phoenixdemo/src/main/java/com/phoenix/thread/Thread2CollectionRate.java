//package com.phoenix.thread;
//
//import java.util.List;
//import java.util.Map;
//
///**
// * @author
// * TODO:收款率线程
// */
//public class Thread2CollectionRate implements Runnable{
//
// private String sql;
//
// private List<Map<String, Object>> listMap ;
//
// @Override
// public void run() {
// //此处线程中获取DAO实例是通过spring注入，各位可以根据自己的方式注入
// BaseService service = (BaseService)SpringContextUtil.getService("service");
// List<Map<String, Object>> list = service.getList(sql);
//
// System.out.println("run tread ok ..................................." + list.size());
// //添加数据到MAP中去
// if(listMap != null) {
// listMap.addAll(list);
//        }
//
//    }
//
//
//    public String getSql() {
//        return sql;
//    }
//
//    public void setSql(String sql) {
//        this.sql = sql;
//    }
//
//    /**
//     * @return the listMap
//     */
//    public List<Map<String, Object>> getListMap() {
//        return listMap;
//    }
//
//
//
//
//    /**
//     * @param listMap the listMap to set
//     */
//    public void setListMap(List<Map<String, Object>> listMap) {
//        this.listMap = listMap;
//    }
//
//
//
//}
