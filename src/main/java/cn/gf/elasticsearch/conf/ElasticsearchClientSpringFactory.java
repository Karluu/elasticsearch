package cn.gf.elasticsearch.conf;

import cn.gf.elasticsearch.autoconfigure.ElasticsearchProperties;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2020/11/18
 * Modified By :
 */
@Slf4j
public class ElasticsearchClientSpringFactory {

    private RestClientBuilder builder;
    private RestClient restClient;
    private RestHighLevelClient restHighLevelClient;

    private static ElasticsearchProperties PROPERTIES;
    private static HttpHost[] HTTP_HOST;

    private static ElasticsearchClientSpringFactory esClientSpringFactory = new ElasticsearchClientSpringFactory();

    private ElasticsearchClientSpringFactory() {
    }

    public static ElasticsearchClientSpringFactory build(ElasticsearchProperties properties, HttpHost[] httpHost) {
        PROPERTIES = properties;
        HTTP_HOST = httpHost;
        return esClientSpringFactory;
    }

    public void init() {
        builder = RestClient.builder(HTTP_HOST);
        setRequestConfigCallback();
        setHttpClientConfigCallback();
        restClient = builder.build();
        restHighLevelClient = new RestHighLevelClient(builder);
        log.info("init host:{} elasticsearch factory", Arrays.toString(HTTP_HOST));
    }

    /**
     * 配置连接时间延时
     */
    private void setRequestConfigCallback() {
        builder.setRequestConfigCallback(requestConfigBuilder -> {
            requestConfigBuilder.setConnectTimeout(PROPERTIES.getConnectTimeoutMillis());
            requestConfigBuilder.setSocketTimeout(PROPERTIES.getSocketTimeoutMillis());
            requestConfigBuilder.setConnectionRequestTimeout(PROPERTIES.getConnectionRequestTimeoutMillis());
            return requestConfigBuilder;
        });
    }

    /**
     * 使用异步httpclient时设置并发连接数
     */
    private void setHttpClientConfigCallback() {
        HttpAsyncClientBuilder httpClientBuilder = HttpAsyncClientBuilder.create();
        httpClientBuilder.setMaxConnTotal(PROPERTIES.getMaxConnTotal());
        httpClientBuilder.setMaxConnPerRoute(PROPERTIES.getMaxConnPerRoute());
        if (PROPERTIES.isAuth()) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(PROPERTIES.getUsername(), PROPERTIES.getPassword()));
            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
        }
        builder.setHttpClientConfigCallback(httpAsyncClientBuilder -> httpClientBuilder);
    }

    public RestClient getClient() {
        return restClient;
    }

    public RestHighLevelClient getRhlClient() {
        return restHighLevelClient;
    }

    public void close() {
        if (restClient != null) {
            try {
                restClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (null != restHighLevelClient) {
            try {
                restHighLevelClient.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        log.info("close host:{} elasticsearch client", Arrays.toString(HTTP_HOST));
    }
}

