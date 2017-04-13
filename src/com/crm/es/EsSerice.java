package com.xu;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.collect.Maps;
import org.elasticsearch.common.lang3.StringUtils;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHits;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author xuchao
 * @create 2017/4/11.
 */
//
public class EsSerice {
    private Client client;
    private String IP;
    private int PORT;
    private String CLUSTERNAME;

    public EsSerice(Client client){
        this.client = client;
    }
    public EsSerice(){}

    public void initClient(){
        Properties properties = new Properties();

        /*读取ES配置信息*/
        try {
            properties.load(new FileInputStream(new File("src/es.properties")));
            IP = properties.getProperty("es_ip");
            PORT = Integer.parseInt(properties.getProperty("es_port"));
            CLUSTERNAME= properties.getProperty("es_clustername");
        } catch (IOException e) {
            e.printStackTrace();
        }

        /*Transport连接ES集群*/
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", CLUSTERNAME).build();

        if (client == null) {
            client = new TransportClient(settings).addTransportAddress(
                    new InetSocketTransportAddress(IP, PORT));
        }
    }

    /*索引单个文档*/
    public IndexResponse indexDocument(String index,String type,String id,Map m){
        IndexResponse response = client.prepareIndex(index, type, id)
                .setSource(m).execute().actionGet();
        return response;
    }

    /*批量索引*/
    public BulkResponse bulkIndexStudent(String index, String type, List<Map> docs){
        BulkResponse response = null;
        int start = 0;
        int count = 100;
        int loop = 0;
        while (loop <= docs.size()/count){
            int end = docs.size();
            if (loop < docs.size()/count) {
                end = start + count;
            }

            BulkRequestBuilder rb = client.prepareBulk();
            for (int i = start; i < end ; i++) {
                Map doc = docs.get(i);
                String id = String.valueOf(doc.get("id"));
                String parent = String.valueOf(doc.get("parent"));
                IndexRequestBuilder resBuilder = client.prepareIndex(index,type,id);

                /*当map中含有parent键时，才会有父子关系。*/
                if (doc.containsKey("parent")){
                    resBuilder.setParent(parent);
                }
                resBuilder.setSource(doc);

                IndexRequest req = resBuilder.request();
                rb = rb.add(req);
                response = rb.execute().actionGet();
            }
            System.out.println("已索引 "+end+" 条文档……");
            start = start + count;
            loop++;
        }
        return response;

    }

    /*删除文档*/
    public DeleteResponse deleteDocument(String index,String type,String id){
        DeleteResponse response = client.prepareDelete(index, type, id)
                .execute().actionGet();
        return response;
    }

    /*更新文档*/
    public void updateDocument(String index,String type,String id){
        Map<String, Object> params = Maps.newHashMap();
        params.put("ntitle", "ElasticSearch Server Book");
        UpdateResponse response = client.prepareUpdate(index,type,id)
                .setScript("ctx._source.title = ntitle", ScriptService.ScriptType.INLINE)
                .setScriptParams(params)
                .execute().actionGet();
    }

    /*检索文档并分页*/
    public SearchHits searchDocumentByFilter(FilterBuilder fb, String index, String type,int pageFrom,int pageCount){
        SearchResponse res = client.prepareSearch(index).setTypes(type)
                .setPostFilter(fb)
                .setFrom(pageFrom).setSize(pageCount).execute().actionGet();
        return res.getHits();
    }

    public void close(){
        try {
            if (client!=null) {
                client.close();
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            client = null;
        }


    }
}
