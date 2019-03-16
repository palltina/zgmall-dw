package com.zhao.dw.publisher.server.serverImpl;

import com.atguigu.common.constant.ZmallConstant;
import com.zhao.dw.publisher.server.PublisherServer;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.core.search.aggregation.TermsAggregation;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.bucket.terms.TermsBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.springframework.beans.factory.annotation.Autowired;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PublisherServerImpl implements PublisherServer {

    @Autowired
    JestClient jestClient;

    @Override
    public int getDauTotal(String date) {
        Map dauHours = getDauHours(date);
        int sum = 0;
        for(int i=1;i <= 24;i++){
           sum += (int)dauHours.get(i);
        }
        return sum;
    }

    @Override
    public Map getDauHours(String date) {
        Map dauHourMap = new HashMap();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("logDate", date);
        boolQueryBuilder.filter(matchQueryBuilder);
        searchSourceBuilder.query(boolQueryBuilder);
        TermsBuilder termsBuilder = AggregationBuilders.terms("groupby_logHour").field("logHour.keyword").size(24);
        searchSourceBuilder.aggregation(termsBuilder);
        System.out.println(searchSourceBuilder.toString());
        Search search = new Search.Builder(searchSourceBuilder.toString()).addIndex(ZmallConstant.ES_INDEX_DAU).addType("_doc").build();
        try {
            SearchResult searchResult = jestClient.execute(search);
            List<TermsAggregation.Entry> entryList = searchResult.getAggregations().getTermsAggregation("groupby_logHour").getBuckets();
            for (TermsAggregation.Entry entry : entryList) {
                dauHourMap.put(entry.getKey(), entry.getCount());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return dauHourMap;
    }
}
