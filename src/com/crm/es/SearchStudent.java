package com.crm.es;

import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.Map;

/**
 * @author xuchao
 * @create 2017/4/13.
 */
//高级查询
public class SearchStudent {
    /*
    *构造虚拟搜索条件
    *搜索条件含有值和操作符两个属性
    */
    Map<String,Map> dummySearchCondition(){
        Map<String,Map> fv = Maps.newHashMap();

        Map<String,Object> m = Maps.newHashMap();
        m.put("value","2017-01-02~2017-01-05");
        m.put("operator",OperatorEnum.BETWEEN);
        fv.put("paydate",m);

        Map<String,Object> m1 = Maps.newHashMap();
        m1.put("value","2000");
        m1.put("operator",OperatorEnum.GTE);
        fv.put("validincome",m1);

        Map<String,Object> m2 = Maps.newHashMap();
        m2.put("value","2");
        m2.put("operator",OperatorEnum.EQ);
        fv.put("status",m2);

        Map<String,Object> m3 = Maps.newHashMap();
        m3.put("value","考研");
        m3.put("operator",OperatorEnum.DEFAULT);
//        fv.put("productname",m3);

        Map<String,Object> m4 = Maps.newHashMap();
        m4.put("value","649");
        m4.put("operator",OperatorEnum.DEFAULT);
        fv.put("phone",m4);
        return fv;
    }

    /*
    * 缓存所有搜索条件的属性映射
    * TYPE:字段在索引中的类型
    * BUILDER：字段用到的过滤器
    */
    Map<String,Map> generateFieldsMap(){
        Map<String,Map> sf = Maps.newHashMap();
        Map c = Maps.newHashMap();
        c.put(FieldEnum.TYPE,"studentstab");
        c.put(FieldEnum.BUILDER, FilterEnum.LIKE);
        sf.put("phone",c);//电话
        Map c1 = Maps.newHashMap();
        c1.put(FieldEnum.TYPE,"studentstab");
        c1.put(FieldEnum.BUILDER, FilterEnum.RANGE);
        sf.put("registertime",c1);//注册时间
        Map c2 = Maps.newHashMap();
        c2.put(FieldEnum.TYPE,"studentstab");
        c2.put(FieldEnum.BUILDER, FilterEnum.TERM);
        sf.put("isphone",c2);

        Map c3 = Maps.newHashMap();
        c3.put(FieldEnum.TYPE,"jobs");
        c3.put(FieldEnum.BUILDER, FilterEnum.TERM);
        sf.put("status",c3);
        Map c4 = Maps.newHashMap();
        c4.put(FieldEnum.TYPE,"jobs");
        c4.put(FieldEnum.BUILDER, FilterEnum.RANGE);
        sf.put("createdate",c4);
        Map c5 = Maps.newHashMap();
        c5.put(FieldEnum.TYPE,"jobs");
        c5.put(FieldEnum.BUILDER, FilterEnum.RANGE);
        sf.put("finisheddate",c5);

        Map c6 = Maps.newHashMap();
        c6.put(FieldEnum.TYPE,"orders");
        c6.put(FieldEnum.BUILDER, FilterEnum.TERM);
        sf.put("orderno",c6);
        Map c7 = Maps.newHashMap();
        c7.put(FieldEnum.TYPE,"orders");
        c7.put(FieldEnum.BUILDER, FilterEnum.RANGE);
        sf.put("validincome",c7);
        Map c8 = Maps.newHashMap();
        c8.put(FieldEnum.TYPE,"orders");
        c8.put(FieldEnum.BUILDER, FilterEnum.RANGE);
        sf.put("paydate",c8);
        Map c9 = Maps.newHashMap();
        c9.put(FieldEnum.TYPE,"orders");
        c9.put(FieldEnum.BUILDER, FilterEnum.TERM);
        sf.put("productid",c9);
        Map c10 = Maps.newHashMap();
        c10.put(FieldEnum.TYPE,"orders");
        c10.put(FieldEnum.BUILDER, FilterEnum.MATCH);
        sf.put("productname",c10);
        return sf;
    }

    /*构造查询过滤器*/
    FilterBuilder generateBuilder(Map<String,Map> searchCondition, Map<String,Map> fm){

        BoolFilterBuilder fb = FilterBuilders.boolFilter();

        for (String field:searchCondition.keySet()) {
            FilterEnum builderType = (FilterEnum)fm.get(field).get(FieldEnum.BUILDER);
            String type = (String)fm.get(field).get(FieldEnum.TYPE);

            FilterBuilder b = null;
            String fieldValue = (String)searchCondition.get(field).get("value");
            OperatorEnum fieldOperator = (OperatorEnum)searchCondition.get(field).get("operator");

            switch (builderType){
                case TERM:
                    b=FilterBuilders.termFilter(field,fieldValue);
                    break;
                case RANGE:

                    RangeFilterBuilder rb=FilterBuilders.rangeFilter(field);
                    switch (fieldOperator){
                        case GTE:
                            rb.gte(fieldValue);
                            break;
                        case LTE:
                            rb.lte(fieldValue);
                            break;
                        case GT:
                            rb.gt(fieldValue);
                            break;
                        case LT:
                            rb.lt(fieldValue);
                            break;
                        case BETWEEN:
                            String[] split = fieldValue.split("~");
                            if (split!=null&split.length==2){
                                rb.gte(split[0]).lte(split[1]);
                            }
                            break;
                        default:
                            break;
                    }
                    b=rb;
                    break;
                case LIKE:
                    b = FilterBuilders.regexpFilter(field,".*"+fieldValue+".*");
                    break;
                case MATCH:
                            QueryBuilder q = QueryBuilders.matchQuery(field,fieldValue);
                            b = FilterBuilders.queryFilter(q);
                    break;
            }
            if (type == "studentstab"){
                fb=fb.must(b);
            }else{
                HasChildFilterBuilder child = FilterBuilders.hasChildFilter(type,b);
                fb=fb.must(child);
            }
        }
        return fb;
    }


    //搜索学员表
    SearchHits searchStudents(Map<String,Map> searchConditon){
        EsSerice es = new EsSerice();
        es.initClient();

        SearchHits hits = null;
        try {
            Map<String, Map> fm = generateFieldsMap();

            /*生成过滤器
            FilterBuilder fb =  FilterBuilders.boolFilter().must(FilterBuilders.hasChildFilter("jobs",FilterBuilders.termFilter("status",2)))
                    .must(FilterBuilders.termsFilter("phone","13929719686","13955176704"))
                    .must(FilterBuilders.hasChildFilter("orders",FilterBuilders.rangeFilter("validincome").lte(10000).gte(1000)));
            */
            FilterBuilder fb = generateBuilder(searchConditon, fm);

            System.out.println(fb);
            /*执行Builder*/
            hits = es.searchDocumentByFilter(fb, "crm", "studentstab", 0, 20);

            /*展示结果*/
            for (SearchHit hit : hits) {
                System.out.println(hit.getId());
            }

        }catch (Exception e){
            e.printStackTrace();
        }finally {
            /*关闭连接*/
            es.close();
        }
        return hits;
    }
}
