package cn.gf.elasticsearch.util;

import cn.gf.elasticsearch.autoconfigure.BulkProcessorProperties;
import cn.gf.elasticsearch.autoconfigure.ElasticsearchProperties;
import cn.gf.elasticsearch.dto.DataDTO;
import cn.gf.elasticsearch.dto.IndexDTO;
import cn.gf.elasticsearch.dto.MappingsDTO;
import cn.gf.elasticsearch.dto.VersionDTO;
import cn.gf.elasticsearch.enums.*;
import cn.gf.elasticsearch.msg.StandardConstants;
import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.ObjectUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.http.HttpUtil;
import cn.hutool.json.JSONArray;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.get.GetIndexRequest;
import org.elasticsearch.action.bulk.*;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.*;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.HttpAsyncResponseConsumerFactory;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.*;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Created by GuoFeng
 * Describe :
 * Created on 2021/12/24
 * Modified By :
 */
@Component
@Slf4j
public class ElasticsearchUtils {

    private RestHighLevelClient restHighLevelClient;
    private ElasticsearchProperties elasticsearchProperties;
    private BulkProcessorProperties bulkProcessorProperties;

    @Autowired
    public ElasticsearchUtils(RestHighLevelClient restHighLevelClient, ElasticsearchProperties elasticsearchProperties, BulkProcessorProperties bulkProcessorProperties) {
        this.restHighLevelClient = restHighLevelClient;
        this.elasticsearchProperties = elasticsearchProperties;
        this.bulkProcessorProperties = bulkProcessorProperties;
    }

    public SearchResponse query(QueryBuilder queryBuilder, String... indices) {
        try {
            List<String> existIndex = existBatch(indices);
            if (existIndex.size() > 0) {
                SearchRequest searchRequest = new SearchRequest(existIndex.toArray(new String[0]));
                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
                searchSourceBuilder.query(queryBuilder);
                searchRequest.source(searchSourceBuilder);
                return restHighLevelClient.search(searchRequest, buildRequestOptions(false));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public SearchResponse query(QueryBuilder queryBuilder, SearchSourceBuilder searchSourceBuilder, String... indices) {
        try {
            List<String> existIndex = existBatch(indices);
            if (existIndex.size() > 0) {
                SearchRequest searchRequest = new SearchRequest(existIndex.toArray(new String[0]));
                searchSourceBuilder.query(queryBuilder);
                searchRequest.source(searchSourceBuilder);
                return restHighLevelClient.search(searchRequest, buildRequestOptions(false));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    private RequestOptions buildRequestOptions(boolean more) {
        RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        if (more) {
            builder.setHttpAsyncResponseConsumerFactory(new HttpAsyncResponseConsumerFactory.HeapBufferedResponseConsumerFactory(500 * 1024 * 1024));
        }
        return builder.build();
    }

    public VersionDTO version() {
        String hostConfig = elasticsearchProperties.getHost();
        if (StrUtil.isEmpty(hostConfig)) {
            log.error("host config error");
            return VersionDTO.builder().build();
        }
        String[] hosts = hostConfig.split(StandardConstants.SEPARATOR_COMMA);
        for (String host : hosts) {
            try {
                String response = HttpUtil.get(String.format("http://%s", host), 5000);
                JSONObject responseData = JSONUtil.parseObj(response);
                String nodeName = responseData.getStr("name");
                JSONObject versionData = responseData.getJSONObject("version");
                if (JSONUtil.isNull(versionData)) {
                    continue;
                }
                String version = versionData.getStr("number");
                return VersionDTO.builder().nodeName(nodeName).version(version).build();
            } catch (Exception e) {
                log.error("this host [{}] error,continue get from next", host);
            }
        }
        return VersionDTO.builder().build();
    }

    public List<IndexDTO> scanIndex() {
        String hostConfig = elasticsearchProperties.getHost();
        if (StrUtil.isEmpty(hostConfig)) {
            log.error("host config error");
            return new LinkedList<>();
        }
        String[] hosts = hostConfig.split(StandardConstants.SEPARATOR_COMMA);
        for (String host : hosts) {
            try {
                String response = HttpUtil.get(String.format("http://%s/_cat/indices?h=index,status&format=json", host), 5000);
                JSONArray object = JSONUtil.parseArray(response);
                return object.toList(IndexDTO.class);
            } catch (Exception e) {
                log.error("this host [{}] error,continue get from next", host);
            }
        }
        return new LinkedList<>();
    }

    public JSONObject scanSettings(String index) {
        String hostConfig = elasticsearchProperties.getHost();
        if (StrUtil.isEmpty(hostConfig)) {
            log.error("host config error");
            return new JSONObject();
        }
        String[] hosts = hostConfig.split(StandardConstants.SEPARATOR_COMMA);
        for (String host : hosts) {
            try {
                String response = HttpUtil.get(String.format("http://%s/%s/_settings", host, index), 5000);
                return JSONUtil.parseObj(response);
            } catch (Exception e) {
                log.error("this host [{}] error,continue get from next", host);
            }
        }
        return new JSONObject();
    }

    public List<MappingsDTO> scanMappings(String index) {
        String hostConfig = elasticsearchProperties.getHost();
        if (StrUtil.isEmpty(hostConfig)) {
            log.error("host config error");
            return new LinkedList<>();
        }
        String[] hosts = hostConfig.split(StandardConstants.SEPARATOR_COMMA);
        for (String host : hosts) {
            try {
                String response = HttpUtil.get(String.format("http://%s/%s/_mappings", host, index), 5000);
                return mappings(JSONUtil.parseObj(response), index);
            } catch (Exception e) {
                log.error("this host [{}] error,continue get from next", host);
            }
        }
        return new LinkedList<>();
    }

    public void columns(SearchSourceBuilder searchSourceBuilder, String... columns) {
        searchSourceBuilder.fetchSource(columns, null);
    }

    public void terms(String field, String[] words, BoolQueryBuilder queryBuilder) {
        queryBuilder.should(QueryBuilders.termsQuery(field, words));
    }

    public void word(String field, String word, BoolQueryBuilder queryBuilder) {
        queryBuilder.must(QueryBuilders.matchPhraseQuery(field, word));
    }

    public void word(List<String> fields, String word, BoolQueryBuilder queryBuilder) {
        for (String field : fields) {
            word(field, word, queryBuilder);
        }
    }

    public void wordTerm(String field, String word, BoolQueryBuilder queryBuilder) {
        queryBuilder.must(QueryBuilders.termQuery(field, word));
    }

    public void wordTerm(List<String> fields, String word, BoolQueryBuilder queryBuilder) {
        for (String field : fields) {
            wordTerm(field, word, queryBuilder);
        }
    }

    public void wildCardQuery(String field, String word, BoolQueryBuilder queryBuilder) {
        queryBuilder.must(QueryBuilders.wildcardQuery(field, String.format("*%s*", word)));
    }

    public void wildCardQuery(List<String> fields, String word, BoolQueryBuilder queryBuilder) {
        for (String field : fields) {
            wildCardQuery(field, word, queryBuilder);
        }
    }

    public void prefixQuery(String field, String word, BoolQueryBuilder queryBuilder) {
        queryBuilder.must(QueryBuilders.prefixQuery(field, word));
    }

    public void prefixQuery(List<String> fields, String word, BoolQueryBuilder queryBuilder) {
        for (String field : fields) {
            prefixQuery(field, word, queryBuilder);
        }
    }

    public void suffixQuery(String field, String word, BoolQueryBuilder queryBuilder) {
        queryBuilder.must(QueryBuilders.wildcardQuery(field, String.format("*%s", word)));
    }

    public void suffixQuery(List<String> fields, String word, BoolQueryBuilder queryBuilder) {
        for (String field : fields) {
            suffixQuery(field, word, queryBuilder);
        }
    }

    public void filter(JSONObject filters, BoolQueryBuilder queryBuilder, String filterMethod) {
        if (null == filters) {
            return;
        }
        FilterEnum filterEnum = FilterEnum.getFilterEnum(filterMethod);
        for (JSONObject.Entry<String, Object> filter : filters.entrySet()) {
            Object value = filter.getValue();
            if (ObjectUtil.isEmpty(value)) {
                continue;
            }
            switch (filterEnum) {
                case MUST:
                    queryBuilder.filter(QueryBuilders.termsQuery(filter.getKey(), String.valueOf(value).split(StandardConstants.SEPARATOR_COMMA)));
                    break;
                case SHOULD:
                    queryBuilder.should(QueryBuilders.termsQuery(filter.getKey(), String.valueOf(value).split(StandardConstants.SEPARATOR_COMMA)));
                    break;
                case MUST_NOT:
                    queryBuilder.mustNot(QueryBuilders.termsQuery(filter.getKey(), String.valueOf(value).split(StandardConstants.SEPARATOR_COMMA)));
                    break;
                default:
                    queryBuilder.filter(QueryBuilders.termsQuery(filter.getKey(), String.valueOf(value).split(StandardConstants.SEPARATOR_COMMA)));
            }
        }
    }

    public void range(String field, String start, String end, String leftCompare, String rightCompare, BoolQueryBuilder queryBuilder) {
        if (StrUtil.isEmpty(field)) {
            throw new IllegalArgumentException(StandardConstants.ARGUMENT_DATE_FIELD);
        }
        RangeQueryBuilder rangeQueryBuilder = QueryBuilders.rangeQuery(field);
        CompareEnum leftCompareEnum = CompareEnum.getCompareEnum(leftCompare);
        if (null != leftCompareEnum) {
            rangeSet(leftCompareEnum, rangeQueryBuilder, start);
        }
        CompareEnum rightCompareEnum = CompareEnum.getCompareEnum(rightCompare);
        if (null != rightCompareEnum) {
            rangeSet(rightCompareEnum, rangeQueryBuilder, end);
        }
        if (StrUtil.isEmpty(start) && StrUtil.isEmpty(end)) {
            throw new IllegalArgumentException(StandardConstants.ARGUMENT_DATE_FIELD);
        }
        queryBuilder.filter(rangeQueryBuilder);
    }

    public void date(String dateField, String start, String end, BoolQueryBuilder queryBuilder) {
        if (StrUtil.isEmpty(dateField)) {
            throw new IllegalArgumentException(StandardConstants.ARGUMENT_DATE_FIELD);
        }
        RangeQueryBuilder dateBuilder = QueryBuilders.rangeQuery(dateField);
        if (StrUtil.isNotEmpty(start)) {
            dateBuilder.gte(start);
        }
        if (StrUtil.isNotEmpty(end)) {
            dateBuilder.lte(end);
        }
        if (StrUtil.isEmpty(start) && StrUtil.isEmpty(end)) {
            dateBuilder.gte(DateUtil.lastWeek()).lte(DateUtil.now());
        }
        queryBuilder.filter(dateBuilder);
    }

    public void page(Integer from, Integer size, SearchSourceBuilder searchSourceBuilder) {
        searchSourceBuilder.from(from).size(size);
    }

    public void sort(String orderColumn, String order, SearchSourceBuilder searchSourceBuilder) {
        switch (SortEnum.getSortEnum(order)) {
            case ASC:
                searchSourceBuilder.sort(orderColumn, SortOrder.ASC);
                break;
            case DESC:
                searchSourceBuilder.sort(orderColumn, SortOrder.DESC);
                break;
            default:
                log.warn(StandardConstants.SORT_TYPE);
                searchSourceBuilder.sort(orderColumn, SortOrder.ASC);
        }
    }

    public void setHighlightBuilder(String fields, SearchSourceBuilder searchSourceBuilder) {
        HighlightBuilder highlightBuilder = new HighlightBuilder();
        highlightBuilder.preTags(elasticsearchProperties.getPreTag()).postTags(elasticsearchProperties.getPostTag()).requireFieldMatch(false);
        if (StrUtil.isNotEmpty(fields)) {
            String[] fieldsArr = fields.split(StandardConstants.SEPARATOR_COMMA);
            for (String field : fieldsArr) {
                highlightBuilder.field(field);
            }
        } else {
            highlightBuilder.field(StandardConstants.ELASTICSEARCH_ALL_FIELDS);
        }
        searchSourceBuilder.highlighter(highlightBuilder);
    }

    public String existSingle(String index) {
        if (!exist(index)) {
            return null;
        }
        return index;
    }

    public List<String> existBatch(String... indexes) {
        List<String> in = new LinkedList<>();
        for (String index : indexes) {
            if (null != existSingle(index)) {
                in.add(index);
            }
        }
        return in;
    }

    public List<String> existBatch(List<String> indexes) {
        List<String> in = new LinkedList<>();
        for (String index : indexes) {
            if (null != existSingle(index)) {
                in.add(index);
            }
        }
        return in;
    }

    private boolean exist(String index) {
        try {
            if (restHighLevelClient.indices().exists(new GetIndexRequest().indices(index), buildRequestOptions(false))) {
                return true;
            }
            log.warn("当前索引{}不存在,查询只针对存在索引查询", index);
            return false;
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }
    }

    private IdsQueryBuilder idQuery(String id) {
        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        idsQueryBuilder.addIds(id);
        return idsQueryBuilder;
    }

    private IdsQueryBuilder idsQuery(List<String> ids) {
        IdsQueryBuilder idsQueryBuilder = new IdsQueryBuilder();
        for (String id : ids) {
            idsQueryBuilder.addIds(id);
        }
        return idsQueryBuilder;
    }

    private ScriptQueryBuilder containsQuery(String field, List<String> values) {
        if (StrUtil.isEmpty(field)) {
            return null;
        }
        if (null == values) {
            return null;
        }
        values.replaceAll(value -> String.format("'%s'", value));
        Script script = new Script(String.format("doc['%s'].values.contains(%s)", field, CollUtil.join(values, ",")));
        return new ScriptQueryBuilder(script);
    }

    private ScriptQueryBuilder greaterThanZeroQuery(String field) {
        if (StrUtil.isEmpty(field)) {
            return null;
        }
        Script script = new Script(String.format("doc['%s'].value > 0", field));
        return new ScriptQueryBuilder(script);
    }

    private ScriptQueryBuilder sumGreaterThanLimitQuery(List<String> fields, String limit, CompareEnum compareEnum) {
        StringBuilder builder = new StringBuilder();
        for (String field : fields) {
            if (StrUtil.isEmpty(field)) {
                continue;
            }
            builder.append("doc['").append(field).append("'].value +");
        }
        builder.delete(builder.length() - 1, builder.length()).append(compareEnum.getType()).append(limit);
        Script script = new Script(builder.toString());
        return new ScriptQueryBuilder(script);
    }


    public SearchResponse scrollQuery(SearchScrollRequest scrollRequest) {
        SearchResponse response = null;
        try {
            response = restHighLevelClient.scroll(scrollRequest, buildRequestOptions(true));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest) {
        ClearScrollResponse response = null;
        try {
            response = restHighLevelClient.clearScroll(clearScrollRequest, buildRequestOptions(false));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return response;
    }

    public void print(SearchSourceBuilder searchSourceBuilder) {
        log.warn("查询条件DSL:\r\n{}", Strings.toString(searchSourceBuilder, true, true));
    }

    public BulkProcessor createBulkProcessor() {
        BulkProcessor.Listener listener = new BulkProcessor.Listener() {
            @Override
            public void beforeBulk(long executionId, BulkRequest request) {
                log.info("1. 【beforeBulk】批次[{}] 携带 {} 请求数量", executionId, request.numberOfActions());
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                if (!response.hasFailures()) {
                    log.info("2. 【afterBulk-成功】批量 [{}] 完成在 {} ms", executionId, response.getTook().getMillis());
                } else {
                    BulkItemResponse[] items = response.getItems();
                    for (BulkItemResponse item : items) {
                        if (item.isFailed()) {
                            log.info("2. 【afterBulk-失败】批量 [{}] 出现异常的原因 : {}", executionId, item.getFailureMessage());
                            break;
                        }
                    }
                }
            }

            @Override
            public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                List<DocWriteRequest<?>> requests = request.requests();
                List<String> esIds = requests.stream().map(DocWriteRequest::id).collect(Collectors.toList());
                log.error("3. 【afterBulk-failure失败】es执行bulk失败,失败的esId为：{}", esIds, failure);
            }
        };
        BulkProcessor.Builder builder = BulkProcessor.builder(((bulkRequest, bulkResponseActionListener) -> restHighLevelClient.bulkAsync(bulkRequest, buildRequestOptions(false), bulkResponseActionListener)), listener);
        //到达固定条时刷新
        builder.setBulkActions(bulkProcessorProperties.getBatchSize());
        //内存到达固定值时刷新
        builder.setBulkSize(new ByteSizeValue(bulkProcessorProperties.getSize(), bulkProcessorProperties.getByteSizeUnit()));
        //设置的刷新间隔固定值
        builder.setFlushInterval(bulkProcessorProperties.getFreshTimeUnit().getTime(bulkProcessorProperties.getFreshTime()));
        //设置允许执行的并发请求数
        builder.setConcurrentRequests(bulkProcessorProperties.getConcurrentRequests());
        //设置重试策略
        builder.setBackoffPolicy(BackoffPolicy.constantBackoff(bulkProcessorProperties.getRetryTimeUnit().getTime(bulkProcessorProperties.getRetryTime()), bulkProcessorProperties.getMaxNumberOfRetries()));
        return builder.build();
    }

    public void asyncSingleAdd(DataDTO dataDTO) {
        IndexRequest indexRequest = createIndexRequest(validate(dataDTO, true), true);
        restHighLevelClient.indexAsync(indexRequest, buildRequestOptions(false), new ActionListener<IndexResponse>() {
            @Override
            public void onResponse(IndexResponse indexResponse) {
                errorDeal(indexRequest, indexResponse);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("{}:存储es时异常，数据信息为", indexRequest.id(), e);
            }
        });
    }

    public void singleAdd(DataDTO dataDTO) {
        IndexRequest indexRequest = createIndexRequest(validate(dataDTO, true), true);
        try {
            IndexResponse indexResponse = restHighLevelClient.index(indexRequest, buildRequestOptions(false));
            if (indexResponse.status() != RestStatus.OK) {
                errorDeal(indexRequest, indexResponse);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void batchAdd(List<DataDTO> dtoList, BulkProcessor bulkProcessor) {
        List<IndexRequest> requests = new LinkedList<>();
        for (DataDTO dataDTO : dtoList) {
            IndexRequest indexRequest = createIndexRequest(validate(dataDTO, true), false);
            requests.add(indexRequest);
        }
        requests.forEach(bulkProcessor::add);
    }

    public void asyncSingleUpdate(DataDTO dataDTO) {
        UpdateRequest updateRequest = createUpdateRequest(validate(dataDTO, false), true);
        restHighLevelClient.updateAsync(updateRequest, buildRequestOptions(false), new ActionListener<UpdateResponse>() {
            @Override
            public void onResponse(UpdateResponse updateResponse) {
                errorDeal(updateRequest, updateResponse);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("{}:存储es时异常，数据信息为", updateRequest.id(), e);
            }
        });
    }

    public void singleUpdate(DataDTO dataDTO) {
        UpdateRequest updateRequest = createUpdateRequest(validate(dataDTO, false), true);
        try {
            UpdateResponse updateResponse = restHighLevelClient.update(updateRequest, buildRequestOptions(false));
            if (updateResponse.status() != RestStatus.OK) {
                errorDeal(updateRequest, updateResponse);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void batchUpdate(List<DataDTO> dtoList, BulkProcessor bulkProcessor) {
        List<UpdateRequest> requests = new LinkedList<>();
        for (DataDTO dataDTO : dtoList) {
            UpdateRequest updateRequest = createUpdateRequest(validate(dataDTO, false), false);
            requests.add(updateRequest);
        }
        requests.forEach(bulkProcessor::add);
    }

    public void asyncSingleDelete(DataDTO dataDTO) {
        DeleteRequest deleteRequest = createDeleteRequest(validate(dataDTO, false), true);
        restHighLevelClient.deleteAsync(deleteRequest, buildRequestOptions(false), new ActionListener<DeleteResponse>() {
            @Override
            public void onResponse(DeleteResponse deleteResponse) {
                errorDeal(deleteRequest, deleteResponse);
            }

            @Override
            public void onFailure(Exception e) {
                log.error("{}:存储es时异常，数据信息为", deleteRequest.id(), e);
            }
        });
    }

    public void singleDelete(DataDTO dataDTO) {
        DeleteRequest deleteRequest = createDeleteRequest(validate(dataDTO, false), true);
        try {
            DeleteResponse deleteResponse = restHighLevelClient.delete(deleteRequest, buildRequestOptions(false));
            if (deleteResponse.status() != RestStatus.OK) {
                errorDeal(deleteRequest, deleteResponse);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void batchDelete(List<DataDTO> dtoList, BulkProcessor bulkProcessor) {
        List<DeleteRequest> requests = new LinkedList<>();
        for (DataDTO dataDTO : dtoList) {
            DeleteRequest deleteRequest = createDeleteRequest(validate(dataDTO, false), false);
            requests.add(deleteRequest);
        }
        requests.forEach(bulkProcessor::add);
    }

    private IndexRequest createIndexRequest(DataDTO dataDTO, boolean signal) {
        IndexRequest indexRequest = new IndexRequest(dataDTO.getIndex(), dataDTO.getType());
        indexRequest.source(dataDTO.getData(), XContentType.JSON);
        if (signal) {
            indexRequest.timeout(TimeUnit.S.getTime(1L));
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        }
        indexRequest.opType(DocWriteRequest.OpType.CREATE);
        indexRequest.id(dataDTO.getId());
        return indexRequest;
    }

    private UpdateRequest createUpdateRequest(DataDTO dataDTO, boolean single) {
        UpdateRequest updateRequest = new UpdateRequest(dataDTO.getIndex(), dataDTO.getType(), dataDTO.getId());
        updateRequest.doc(dataDTO.getData(), XContentType.JSON);
        if (single) {
            updateRequest.timeout(TimeUnit.S.getTime(1L));
            updateRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        }
        return updateRequest;
    }

    private DeleteRequest createDeleteRequest(DataDTO dataDTO, boolean single) {
        DeleteRequest deleteRequest = new DeleteRequest(dataDTO.getIndex(), dataDTO.getType(), dataDTO.getId());
        if (single) {
            deleteRequest.timeout(TimeUnit.S.getTime(1L));
            deleteRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        }
        return deleteRequest;
    }

    private void errorDeal(IndexRequest indexRequest, IndexResponse indexResponse) {
        ReplicationResponse.ShardInfo shardInfo = indexResponse.getShardInfo();
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                log.error("将id为：{}的数据存入ES时存在失败的分片，原因为：{}", indexRequest.id(), failure.getCause());
            }
        }
    }

    private void errorDeal(UpdateRequest updateRequest, UpdateResponse updateResponse) {
        ReplicationResponse.ShardInfo shardInfo = updateResponse.getShardInfo();
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                log.error("将id为：{}的数据存入ES时存在失败的分片，原因为：{}", updateRequest.id(), failure.getCause());
            }
        }
    }

    private void errorDeal(DeleteRequest deleteRequest, DeleteResponse deleteResponse) {
        ReplicationResponse.ShardInfo shardInfo = deleteResponse.getShardInfo();
        if (shardInfo.getFailed() > 0) {
            for (ReplicationResponse.ShardInfo.Failure failure : shardInfo.getFailures()) {
                log.error("将id为：{}的数据存入ES时存在失败的分片，原因为：{}", deleteRequest.id(), failure.getCause());
            }
        }
    }

    private DataDTO validate(DataDTO dataDTO, boolean add) {
        if (StrUtil.isEmpty(dataDTO.getIndex())) {
            throw new IllegalArgumentException("DataDTO index param can not be null");
        }
        if (StrUtil.isEmpty(dataDTO.getType())) {
            throw new IllegalArgumentException("DataDTO type param can not be null");
        }
        if (StrUtil.isEmpty(dataDTO.getId()) && !add) {
            throw new IllegalArgumentException("DataDTO id param can not be null");
        }
        return dataDTO;
    }

    private void rangeSet(CompareEnum compareEnum, RangeQueryBuilder rangeQueryBuilder, String word) {
        switch (compareEnum) {
            case LT:
                rangeQueryBuilder.lt(word);
                break;
            case LTE:
                rangeQueryBuilder.lte(word);
                break;
            case GT:
                rangeQueryBuilder.gt(word);
                break;
            case GTE:
                rangeQueryBuilder.gte(word);
                break;
            default:
                log.error(StandardConstants.COMPARE_ERROR);
        }
    }

    private List<MappingsDTO> mappings(JSONObject data, String index) {
        VersionDTO version = version();
        VersionEnum versionEnum = VersionEnum.versionEnum(version.getVersion());
        if (null == versionEnum) {
            return new LinkedList<>();
        }
        switch (versionEnum) {
            case FIVE:
                return commonFiveAndSix(data, index);
            case SIX:
                return commonFiveAndSix(data, index);
            case SEVEN:
                return seven(data, index);
            default:
                throw new IllegalArgumentException("can not get cluster version from param");
        }
    }

    private List<MappingsDTO> commonFiveAndSix(JSONObject data, String index) {
        JSONObject mappings = JSONUtil.getByPath(data, String.format("$.%s.mappings", index), JSONUtil.createObj());
        Set<String> keySet = mappings.keySet();
        List<MappingsDTO> result = new LinkedList<>();
        for (String type : keySet) {
            nest(JSONUtil.getByPath(mappings, String.format("$.%s.properties", type), JSONUtil.createObj()), result, type, false);
        }
        return result;
    }

    private List<MappingsDTO> seven(JSONObject data, String index) {
        List<MappingsDTO> result = new LinkedList<>();
        JSONObject mappings = JSONUtil.getByPath(data, String.format("$.%s.mappings.properties", index), JSONUtil.createObj());
        nest(mappings, result, null, false);
        return result;
    }

    private void nest(JSONObject mappings, List<MappingsDTO> result, String type, boolean nestFlag) {
        Set<String> keys = mappings.keySet();
        for (String key : keys) {
            String fieldType = JSONUtil.getByPath(mappings, String.format("$.%s.type", key), "");
            MappingsDTO mappingsDTO = MappingsDTO.builder().type(type).field(key).fieldType(fieldType).build();
            List<MappingsDTO> nest = new LinkedList<>();
            if (nestFlag) {
                nest.add(mappingsDTO);
            }
            if ("nested".equals(type)) {
                nest(JSONUtil.getByPath(mappings, String.format("$.%s.properties", key), JSONUtil.createObj()), nest, type, true);
                mappingsDTO.setNest(nest);
            }
            result.add(mappingsDTO);
        }
    }
}
