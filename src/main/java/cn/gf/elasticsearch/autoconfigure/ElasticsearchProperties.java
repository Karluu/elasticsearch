package cn.gf.elasticsearch.autoconfigure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2021/12/16
 * Modified By :
 */
@Configuration
@ConfigurationProperties(prefix = "elasticsearch", ignoreInvalidFields = true)
public class ElasticsearchProperties {

    private String host = "127.0.0.1:9200";
    private int connectNum = 10;
    private int connectPerRoute = 50;
    private int connectTimeoutMillis = 1000;
    private int socketTimeoutMillis = 30000;
    private int connectionRequestTimeoutMillis = 500;
    private int maxConnPerRoute = 10;
    private int maxConnTotal = 30;
    private boolean log = true;
    private boolean auth = false;
    private String username;
    private String password;
    private String preTag = "「「「";
    private String postTag = "」」」";

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getConnectNum() {
        return connectNum;
    }

    public void setConnectNum(int connectNum) {
        this.connectNum = connectNum;
    }

    public int getConnectPerRoute() {
        return connectPerRoute;
    }

    public void setConnectPerRoute(int connectPerRoute) {
        this.connectPerRoute = connectPerRoute;
    }

    public int getConnectTimeoutMillis() {
        return connectTimeoutMillis;
    }

    public void setConnectTimeoutMillis(int connectTimeoutMillis) {
        this.connectTimeoutMillis = connectTimeoutMillis;
    }

    public int getSocketTimeoutMillis() {
        return socketTimeoutMillis;
    }

    public void setSocketTimeoutMillis(int socketTimeoutMillis) {
        this.socketTimeoutMillis = socketTimeoutMillis;
    }

    public int getConnectionRequestTimeoutMillis() {
        return connectionRequestTimeoutMillis;
    }

    public void setConnectionRequestTimeoutMillis(int connectionRequestTimeoutMillis) {
        this.connectionRequestTimeoutMillis = connectionRequestTimeoutMillis;
    }

    public int getMaxConnPerRoute() {
        return maxConnPerRoute;
    }

    public void setMaxConnPerRoute(int maxConnPerRoute) {
        this.maxConnPerRoute = maxConnPerRoute;
    }

    public int getMaxConnTotal() {
        return maxConnTotal;
    }

    public void setMaxConnTotal(int maxConnTotal) {
        this.maxConnTotal = maxConnTotal;
    }

    public boolean isLog() {
        return log;
    }

    public void setLog(boolean log) {
        this.log = log;
    }

    public boolean isAuth() {
        return auth;
    }

    public void setAuth(boolean auth) {
        this.auth = auth;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getPreTag() {
        return preTag;
    }

    public void setPreTag(String preTag) {
        this.preTag = preTag;
    }

    public String getPostTag() {
        return postTag;
    }

    public void setPostTag(String postTag) {
        this.postTag = postTag;
    }
}
