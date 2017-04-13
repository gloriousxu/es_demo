package com.crm.es;

import java.util.*;

/**
 * @author xuchao
 * @create 2017/4/12.
 */
//
public class EsHandler {
    static Dao dao = new Dao();
    public static void main(String[] args) {

        /*索引学员文档*/
//        System.out.println("开始索引学员表……");
//        indexStudentstab();
//        System.out.println("索引学员成功！");

        /*索引工作表*/
//        System.out.println("开始索引工作表……");
//        indexJobs();
//        System.out.println("索引工作成功！");
//
//        /*索引订单表*/
//        System.out.println("开始索引订单表……");
//        indexorders();
//        System.out.println("索引订单成功！");

        /*查询学员数据*/
        SearchStudent search = new SearchStudent();
        search.searchStudents(search.dummySearchCondition());

    }

    //索引学员表
    static void indexStudentstab(){
        EsSerice es = new EsSerice();
        es.initClient();
        try {
            List stus = dao.findStudents();
            if (stus != null && stus.size() != 0) {
                es.bulkIndexStudent("crm","studentstab",stus);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            /*关闭连接*/
            es.close();
        }
    }

    //索引工作表
    static void indexJobs(){
        EsSerice es = new EsSerice();
        es.initClient();
        try {
            List jobs = dao.findJobs();

            if (jobs != null && jobs.size() != 0) {
                es.bulkIndexStudent("crm","jobs",jobs);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            /*关闭连接*/
            es.close();
        }
    }

    //索引订单表
    static void indexorders(){
        EsSerice es = new EsSerice();
        es.initClient();
        try {
            List orders = dao.findOrders();
            if (orders != null && orders.size() != 0) {
                es.bulkIndexStudent("crm","orders",orders);
            }
        } catch (Exception e) {
            throw e;
        } finally {
            /*关闭连接*/
            es.close();
        }

    }
}
