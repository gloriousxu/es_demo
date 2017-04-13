package com.xu;

import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.text.ParseException;
import java.text.SimpleDateFormat;
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
//
//        /*索引工作表*/
//        System.out.println("开始索引工作表……");
//        indexJobs();
//        System.out.println("索引工作成功！");
//
//        /*索引订单表*/
//        System.out.println("开始索引订单表……");
//        indexorders();
//        System.out.println("索引订单成功！");

        /*查询学员数据*/
        searchStudents();

    }

    //索引学员表
    static void indexStudentstab(){
        EsSerice es = new EsSerice();
        es.initClient();
        List stus = dao.findStudents();
        if (stus != null && stus.size() != 0) {
            es.bulkIndexStudent("crm","studentstab",stus);
        }
        /*关闭连接*/
        es.close();
    }

    //索引工作表
    static void indexJobs(){
        EsSerice es = new EsSerice();
        es.initClient();
        List jobs = dao.findJobs();

        if (jobs != null && jobs.size() != 0) {
            es.bulkIndexStudent("crm","jobs",jobs);
        }

        /*关闭连接*/
        es.close();
    }

    //索引订单表
    static void indexorders(){
        EsSerice es = new EsSerice();
        es.initClient();
        List orders = dao.findOrders();

        if (orders != null && orders.size() != 0) {
            es.bulkIndexStudent("crm","orders",orders);
        }

        /*关闭连接*/
        es.close();
    }

    //搜索学员表
    static void searchStudents(){
        EsSerice es = new EsSerice();
        es.initClient();
        /*构造BUILDER*/
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        Date startdate = null;
        Date enddate = null;

        try {
            startdate = sdf.parse("2017-01-02");
            enddate = sdf.parse("2017-01-03");
        } catch (ParseException e) {
            e.printStackTrace();
        }

        FilterBuilder fb =  FilterBuilders.boolFilter().must(FilterBuilders.hasChildFilter("jobs",FilterBuilders.termFilter("status",2)))
                //.must(FilterBuilders.termsFilter("phone","13929719686","13955176704"))
                .must(FilterBuilders.hasChildFilter("orders",FilterBuilders.rangeFilter("validincome").lte(10000).gte(1000)));

        System.out.println(fb);
        /*执行Builder*/
        SearchHits hits = es.searchDocumentByFilter(fb,"crm","studentstab",0,20);

        System.out.println(hits);

        /*展示结果*/
        for (SearchHit hit:hits) {
            System.out.println(hit.getId());
        }

        /*关闭连接*/
        es.close();
    }
}
