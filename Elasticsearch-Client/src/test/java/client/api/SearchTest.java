package client.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.filter.FiltersAggregator;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregationBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.sort.SortOrder;
import org.junit.Test;

import com.tistory.joyhong.elasticsearch.client.ClientApi;
import com.tistory.joyhong.elasticsearch.client.api.SearchApi;
import com.tistory.joyhong.elasticsearch.client.data.HighlightData;
import com.tistory.joyhong.elasticsearch.client.data.SortData;

public class SearchTest {

	public String ip = "localhost";
	
//	@Test
	public void search1() {
		ClientApi api = new ClientApi(ip);
		SearchApi schApi = api.getSearchApi();
		
		List<SortData> sortList = new ArrayList<SortData>();
		SortData sData = new SortData();
		sData.setField("_uid");
		sData.setType(SortOrder.DESC);
		sortList.add(sData);
		
		
		QueryBuilder qBuilder = QueryBuilders.matchQuery("title", "제목");
		SearchResponse response = schApi.search(qBuilder, sortList);
		SearchHits hits = response.getHits();
		System.out.println(hits.getTotalHits());
		SearchHit[] searchHits = hits.getHits();
		for (SearchHit hit : searchHits) {
			System.out.println(hit.getSourceAsString());
		}
		
		qBuilder = QueryBuilders.matchQuery("title", "10");
		response = schApi.search(qBuilder);
		hits = response.getHits();
		System.out.println(hits.getTotalHits());
		searchHits = hits.getHits();
		for (SearchHit hit : searchHits) {
			System.out.println(hit.getSourceAsString());
		}
		
		api.close();
	}
	
//	@Test
	public void search2() {
		ClientApi api = new ClientApi(ip);
		SearchApi schApi = api.getSearchApi();
		
		List<HighlightData> highlightList = new ArrayList<HighlightData>();
		HighlightData data = new HighlightData();
		data.setField("title");
		data.setPreTag("<a>");
		data.setPostTag("</a>");
		highlightList.add(data);
		
		
		QueryBuilder qBuilder = QueryBuilders.matchQuery("title", "제목");
		SearchResponse response = schApi.search(qBuilder,0, 10, null, null, highlightList);
		SearchHits hits = response.getHits();
		System.out.println(hits.getTotalHits());
		SearchHit[] searchHits = hits.getHits();
		for (SearchHit hit : searchHits) {
			System.out.println(hit.getSourceAsString());
			Map<String, HighlightField> highlightFields = hit.getHighlightFields();
			HighlightField highlight = highlightFields.get("title"); 
			System.out.println(highlight);
		    Text[] fragments = highlight.fragments();  
		    String fragmentString = fragments[0].string();
		    System.out.println(fragmentString);
		}
		
		api.close();
	}
	
//	@Test
	public void search3() {
		ClientApi api = new ClientApi(ip);
		SearchApi schApi = api.getSearchApi();
		
		List<HighlightData> highlightList = new ArrayList<HighlightData>();
		HighlightData data = new HighlightData();
		data.setField("title");
		highlightList.add(data);
		
		AggregationBuilder aggregation = AggregationBuilders.terms("by_date").field("code");
		aggregation.subAggregation(AggregationBuilders.count("cc").field("uid"));
		
		QueryBuilder qBuilder = QueryBuilders.matchQuery("title", "캡틴");
		SearchResponse response = schApi.search(qBuilder,0, 10, null, null, highlightList, aggregation);
		SearchHits hits = response.getHits();
		System.out.println(hits.getTotalHits());
		SearchHit[] searchHits = hits.getHits();
		for (SearchHit hit : searchHits) {
			System.out.println(hit.getSourceAsString());
			Map<String, HighlightField> highlightFields = hit.getHighlightFields();
			HighlightField highlight = highlightFields.get("title"); 
		    Text[] fragments = highlight.fragments();  
		    String fragmentString = fragments[0].string();
		    System.out.println(fragmentString);
		}
		
		
		Aggregations aggregations = response.getAggregations();
		Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
		Terms companyAggregation = (Terms) aggregationMap.get("by_date");
		System.out.println(companyAggregation);
		for (Terms.Bucket bucket : companyAggregation.getBuckets()) {
			String key = bucket.getKeyAsString();
		    long docCount = bucket.getDocCount();
		    System.out.println(key + "\t" +  docCount);
		}
		
		api.close();
	}
	
//	@Test
	public void search4() {
		ClientApi api = new ClientApi(ip);
		SearchApi schApi = api.getSearchApi();
		
		List<HighlightData> highlightList = new ArrayList<HighlightData>();
		HighlightData data = new HighlightData();
		data.setField("title");
		highlightList.add(data);
		
		
		BoolQueryBuilder boolQBuilder = QueryBuilders.boolQuery();
		boolQBuilder.must(QueryBuilders.termQuery("title", "캡틴").boost(1));
		boolQBuilder.should(QueryBuilders.termQuery("code", "2023").boost(10));
		SearchResponse response = schApi.search(boolQBuilder,0, 10,  new String[] {"test"}, null, highlightList);
		SearchHits hits = response.getHits();
		SearchHit[] searchHits = hits.getHits();
		for (SearchHit hit : searchHits) {
			System.out.println(hit.getSourceAsString());
			Map<String, HighlightField> highlightFields = hit.getHighlightFields();
			HighlightField highlight = highlightFields.get("title"); 
		    Text[] fragments = highlight.fragments();  
		    String fragmentString = fragments[0].string();
		    System.out.println(fragmentString);
		}
		
		api.close();
	}
	
//	@Test
	public void searchScroll1() {
		ClientApi api = new ClientApi(ip);
		SearchApi schApi = api.getSearchApi();
		
		
		BoolQueryBuilder boolQBuilder = QueryBuilders.boolQuery();
		boolQBuilder.must(QueryBuilders.termQuery("title", "캡틴").boost(1));
		SearchResponse response = schApi.searchScroll(boolQBuilder, 10,  new String[] {"test"});
		String scrollId = response.getScrollId();
		SearchHit[] searchHits = response.getHits().getHits();
		Arrays.stream(searchHits).forEach(x -> System.out.println(x.getSourceAsString()));

		while (searchHits != null && searchHits.length > 0) { 
		    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); 
		    scrollRequest.scroll(TimeValue.timeValueMinutes(1L));
		    try {
				response = schApi.getClient().scroll(scrollRequest, RequestOptions.DEFAULT);
			} catch (IOException e) {
				e.printStackTrace();
			}
		    scrollId = response.getScrollId();
		    searchHits = response.getHits().getHits();
		    Arrays.stream(searchHits).forEach(x -> System.out.println(x.getSourceAsString()));
		}
		
		ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); 
		clearScrollRequest.addScrollId(scrollId);
		ClearScrollResponse clearScrollResponse;
		boolean succeeded = false;
		try {
			clearScrollResponse = schApi.getClient().clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
			succeeded = clearScrollResponse.isSucceeded();
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println(succeeded);
		
		api.close();
	}
	
	
	@Test
	public void searchTemplate() {
		ClientApi api = new ClientApi(ip);
		SearchApi schApi = api.getSearchApi();
		StringBuilder script = new StringBuilder("{") ;
		script.append("  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } },");
		script.append("  \"size\" : \"{{size}}\",");
		script.append("  \"highlight\": { \"fields\" : { \"{{highlight}}\" : {} } }");
		script.append("}");
		
		Map<String, Object> params = new HashMap<>();
		params.put("field", "title");
		params.put("value", "캡틴");
		params.put("size", 10);
		params.put("highlight", "title");
		SearchResponse response = schApi.searchTemplate(script.toString(), params, "test");
		SearchHits hits = response.getHits();
		System.out.println(hits.getTotalHits());
		SearchHit[] searchHits = hits.getHits();
		for (SearchHit hit : searchHits) {
			System.out.println(hit.getSourceAsString());
			Map<String, HighlightField> highlightFields = hit.getHighlightFields();
			HighlightField highlight = highlightFields.get("title"); 
		    Text[] fragments = highlight.fragments();  
		    String fragmentString = fragments[0].string();
		    System.out.println(fragmentString);
		}
		
		api.close();
	}
	
}
