package com;


import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsFilter;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.*;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * @author xuchao
 * @create 2017/4/7.
 */
//
public class EsDemo {
    public static final String IP = "10.155.38.101";
    public static final int PORT = 9300;
    public static final String CLUSTERNAME = "crm_es";

    public static void main(String[] args) {
        //连接ES集群
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", CLUSTERNAME).build();
        Client client = new TransportClient(settings).addTransportAddress(
                new InetSocketTransportAddress(IP,PORT));
        System.out.println(client);

        /*构造BUILDER*/
        FilterBuilder fb =  FilterBuilders.boolFilter().must(FilterBuilders.hasChildFilter("jobs",FilterBuilders.termFilter("status",2)))
                .must(FilterBuilders.termsFilter("phone","13632112322","18514472211"))
                .must(FilterBuilders.hasChildFilter("orders",FilterBuilders.rangeFilter("paytime").lte(new Date())));

        System.out.println(fb);

        /*执行Builder*/
        SearchResponse res = client.prepareSearch("crm_students").setTypes("studentstab")
                .setPostFilter(fb)
                .setSize(100).execute().actionGet();
        System.out.println(res);
        /*展示结果*/
        for (SearchHit hit:res.getHits()) {
            System.out.println(hit.getId());
        }
        /*关闭连接*/
        client.close();
    }
}
