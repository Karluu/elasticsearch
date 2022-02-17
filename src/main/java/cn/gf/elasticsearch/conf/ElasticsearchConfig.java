package cn.gf.elasticsearch.conf;

import cn.gf.elasticsearch.autoconfigure.ElasticsearchProperties;
import cn.gf.elasticsearch.msg.StandardConstants;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2020/11/18
 * Modified By :
 */
@Configuration
@ComponentScan(basePackageClasses = ElasticsearchClientSpringFactory.class)
public class ElasticsearchConfig {

    private ElasticsearchProperties elasticsearchProperties;

    @Autowired
    public ElasticsearchConfig(ElasticsearchProperties elasticsearchProperties) {
        this.elasticsearchProperties = elasticsearchProperties;
    }

    @Bean
    public HttpHost[] httpHost() {
        String[] configHostArr = elasticsearchProperties.getHost().split(StandardConstants.SEPARATOR_COMMA);
        HttpHost[] hostArr = new HttpHost[configHostArr.length];
        for (int i = 0; i < hostArr.length; i++) {
            String[] ipPort = configHostArr[i].split(StandardConstants.SEPARATOR_COLON);
            hostArr[i] = new HttpHost(ipPort[0], Integer.valueOf(ipPort[1]), "http");
        }
        return hostArr;
    }

    @Bean(initMethod = "init", destroyMethod = "close")
    public ElasticsearchClientSpringFactory getFactory() {
        return ElasticsearchClientSpringFactory.build(elasticsearchProperties, httpHost());
    }

    @Bean
    @Scope("singleton")
    public RestClient restClient() {
        return getFactory().getClient();
    }

    @Bean
    @Scope("singleton")
    public RestHighLevelClient restHighLevelClient() {
        return getFactory().getRhlClient();
    }

}

