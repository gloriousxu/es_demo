package com.crm.es;

import org.elasticsearch.common.collect.Maps;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author xuchao
 * @create 2017/4/12.
 */
//
public class Dao {

    public static void main(String[] args) throws SQLException{
        System.out.println(new Dao().findOrders().get(0));
    }

    /*学员列表*/
    public List findStudents(){
        ResultSet res = null;
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = DBManager.getConnection();
            stmt = conn.createStatement();
            String sql = "select * from studentstab where registertime between '2017-01-02' AND  '2017-01-05'";
            System.out.println("mysql connect success!");
            res = stmt.executeQuery(sql);
            List list = new ArrayList();
            while (res.next()) {
                Map m = Maps.newHashMap();
                m.put("id",res.getInt("studid"));
                m.put("username",res.getString("username"));
                m.put("realname",res.getString("realname"));
                m.put("phone",res.getString("phone"));
                m.put("phonebackone",res.getString("phonebackone"));
                m.put("phonebacktwo",res.getString("phonebacktwo"));
                m.put("phonebackthree",res.getString("phonebackthree"));
                m.put("phonebackfour",res.getString("phonebackfour"));
                m.put("oldid",res.getInt("oldid"));
                m.put("isphone",res.getInt("isphone"));
                m.put("isorder",res.getInt("isorder"));
                m.put("registertime",res.getString("registertime"));
                list.add(m);
            }
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            DBManager.free(res,stmt,conn);
        }
        return null;
    }

    /*工作列表*/
    public List findJobs(){
        ResultSet res = null;
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = DBManager.getConnection();
            stmt = conn.createStatement();
            String sql = "select * from jobs where createtime BETWEEN '2017-01-01' AND '2017-01-05'";
            System.out.println("mysql connect success!");
            res = stmt.executeQuery(sql);
            List list = new ArrayList();
            while (res.next()) {
                Map m = Maps.newHashMap();
                m.put("id",res.getInt("id"));
                m.put("parent",res.getInt("userid"));
                m.put("status",res.getInt("status"));
                m.put("worktype",res.getInt("worktype"));
                m.put("prochildtype",res.getInt("prochildtype"));
                m.put("protype",res.getInt("protype"));
                m.put("consultcotent",res.getString("consultcotent"));
                m.put("coursename",res.getString("coursename"));
                m.put("createtime",res.getString("createtime"));
                m.put("finishedtime",res.getString("finishedtime"));
                m.put("createdate",res.getDate("createtime"));
                m.put("finisheddate",res.getDate("finishedtime"));
                m.put("dealer",res.getInt("dealer"));
                m.put("userphone",res.getString("userphone"));
                m.put("worksource",res.getString("worksource"));
                list.add(m);
            }
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            DBManager.free(res,stmt,conn);
        }
        return null;
    }

    /*订单列表*/
    public List findOrders(){
        ResultSet res = null;
        Statement stmt = null;
        Connection conn = null;
        try {
            conn = DBManager.getConnection();
            stmt = conn.createStatement();
            String sql = "select a.*,b.studid from shark_order_for_search a join studentstab b ON a.userid = b.oldid where ordertime BETWEEN '2017-01-01' AND '2017-01-05'";
            System.out.println("mysql connect success!");
            res = stmt.executeQuery(sql);
            List list = new ArrayList();
            while (res.next()) {
                Map m = Maps.newHashMap();
                m.put("id",res.getInt("id"));
                m.put("parent",res.getInt("studid"));
                m.put("orderid",res.getInt("orderid"));
                m.put("orderno",res.getString("orderno"));
                m.put("productid",res.getInt("productid"));
                m.put("finance_category_id",res.getInt("finance_category_id"));
                m.put("productname",res.getString("productname"));
                m.put("orderstatusid",res.getInt("orderstatusid"));
                m.put("ordertime",res.getString("ordertime"));
                m.put("paytime",res.getString("paytime"));
                m.put("orderdate",res.getDate("ordertime"));
                m.put("paydate",res.getDate("paytime"));
                m.put("validincomepercent",res.getDouble("validincomepercent"));
                m.put("validincome",res.getDouble("validincome"));
                m.put("productstatus",res.getInt("productstatus"));
                m.put("courseprocess",res.getDouble("courseprocess"));
                list.add(m);
            }
            return list;
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            DBManager.free(res,stmt,conn);
        }
        return null;
    }
}
