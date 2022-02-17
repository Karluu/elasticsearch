package cn.gf.elasticsearch.autoconfigure;

import cn.gf.elasticsearch.enums.TimeUnit;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2022/1/12
 * Modified By :
 */
@Configuration
@ConfigurationProperties(prefix = "elasticsearch.bulk-processor")
public class BulkProcessorProperties {

    private int batchSize = 10000;
    private Long size = 8L;
    private ByteSizeUnit byteSizeUnit = ByteSizeUnit.MB;
    private int concurrentRequests = 8;
    private int maxNumberOfRetries = 3;
    private long freshTime = 10L;
    private TimeUnit freshTimeUnit = TimeUnit.S;
    private long retryTime = 3L;
    private TimeUnit retryTimeUnit = TimeUnit.S;

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public Long getSize() {
        return size;
    }

    public void setSize(Long size) {
        this.size = size;
    }

    public ByteSizeUnit getByteSizeUnit() {
        return byteSizeUnit;
    }

    public void setByteSizeUnit(ByteSizeUnit byteSizeUnit) {
        this.byteSizeUnit = byteSizeUnit;
    }

    public int getConcurrentRequests() {
        return concurrentRequests;
    }

    public void setConcurrentRequests(int concurrentRequests) {
        this.concurrentRequests = concurrentRequests;
    }

    public int getMaxNumberOfRetries() {
        return maxNumberOfRetries;
    }

    public void setMaxNumberOfRetries(int maxNumberOfRetries) {
        this.maxNumberOfRetries = maxNumberOfRetries;
    }

    public long getFreshTime() {
        return freshTime;
    }

    public void setFreshTime(long freshTime) {
        this.freshTime = freshTime;
    }

    public TimeUnit getFreshTimeUnit() {
        return freshTimeUnit;
    }

    public void setFreshTimeUnit(TimeUnit freshTimeUnit) {
        this.freshTimeUnit = freshTimeUnit;
    }

    public long getRetryTime() {
        return retryTime;
    }

    public void setRetryTime(long retryTime) {
        this.retryTime = retryTime;
    }

    public TimeUnit getRetryTimeUnit() {
        return retryTimeUnit;
    }

    public void setRetryTimeUnit(TimeUnit retryTimeUnit) {
        this.retryTimeUnit = retryTimeUnit;
    }
}
