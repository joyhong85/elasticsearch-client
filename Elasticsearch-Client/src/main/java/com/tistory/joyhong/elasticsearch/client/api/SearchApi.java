package com.tistory.joyhong.elasticsearch.client.api;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;

import com.tistory.joyhong.elasticsearch.client.data.HighlightData;
import com.tistory.joyhong.elasticsearch.client.data.SortData;

/**
 * Search API 모음
 * 엘라스틱서치 자바 High Level REST Client v6.8
 * 참조 : https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.8/_search_apis.html
 * @author joyhong
 *
 */
public class SearchApi {

	Logger log = LogManager.getLogger("searchLog");
	
	
	private RestHighLevelClient client;
	
	public SearchApi(RestHighLevelClient client) {
		this.client = client;
	}
	
	public SearchResponse search(QueryBuilder qBuilder) {
		return _search(qBuilder, 0, 10, null, null, null, null, null, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder, boolean fetchSource) {
		return _search(qBuilder, 0, 10, null, null, null, fetchSource, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder, List<SortData> sortList) {
		return _search(qBuilder, 0, 10, null, sortList, null, null, null, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder, String[] indices) {
		return _search(qBuilder, 0, 10, indices, null, null, null, null, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder, String[] indices, List<SortData> sortList) {
		return _search(qBuilder, 0, 10, indices, sortList, null, null, null, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size) {
		return _search(qBuilder, startNum, size, null, null, null, null, null, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, boolean fetchSource) {
		return _search(qBuilder, startNum, size, null, null, null, fetchSource, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, AggregationBuilder aggregation) {
		return _search(qBuilder, startNum, size, null, null, null, null, null, aggregation);
	}
	
	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, List<SortData> sortList) {
		return _search(qBuilder, startNum, size, null, sortList, null, null, null, null);
	}

	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, List<SortData> sortList, AggregationBuilder aggregation) {
		return _search(qBuilder, startNum, size, null, sortList, null, null, null, aggregation);
	}

	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, String[] indices) {
		return _search(qBuilder, startNum, size, indices, null, null, null, null, null);
	}	
	
	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, String[] indices, boolean fetchSource) {
		return _search(qBuilder, startNum, size, indices, null, null, fetchSource, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, String[] indices, List<SortData> sortList) {
		return _search(qBuilder, startNum, size, indices, sortList, null, null, null, null);
	}
	
	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, String[] indices, List<SortData> sortList, boolean fetchSource) {
		return _search(qBuilder, startNum, size, indices, sortList, null, fetchSource, null);
	}

	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, String[] indices, List<SortData> sortList, List<HighlightData> highlightList) {
		return _search(qBuilder, startNum, size, indices, sortList, highlightList, null, null, null);
	}

	public SearchResponse search(QueryBuilder qBuilder,int startNum, int size, String[] indices, List<SortData> sortList, List<HighlightData> highlightList, AggregationBuilder aggregation) {
		return _search(qBuilder, startNum, size, indices, sortList, highlightList, null, null, aggregation);
	}
	
	private SearchResponse _search(QueryBuilder qBuilder, int startNum, int size, String[] indices, List<SortData> sortList, List<HighlightData> highlightList, 
			boolean fetchSource, AggregationBuilder aggregation) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.fetchSource(fetchSource);
		if(aggregation != null)
			searchSourceBuilder.aggregation(aggregation);
		return _searchExcution(searchSourceBuilder, qBuilder, startNum, size, indices, sortList, highlightList);
	}

	private SearchResponse _search(QueryBuilder qBuilder, int startNum, int size, String[] indices, List<SortData> sortList, List<HighlightData> highlightList, 
			String[] includeFields, String[] excludeFields, AggregationBuilder aggregation) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		if(includeFields != null && excludeFields!= null)
			searchSourceBuilder.fetchSource(includeFields, excludeFields);
		if(aggregation != null)
			searchSourceBuilder.aggregation(aggregation);
		return _searchExcution(searchSourceBuilder, qBuilder, startNum, size, indices, sortList, highlightList);
	}
	
	private SearchResponse _searchExcution(SearchSourceBuilder searchSourceBuilder, QueryBuilder qBuilder, int startNum, int size, String[] indices, 
			List<SortData> sortList, List<HighlightData> highlightList) {
		SearchRequest request = new SearchRequest(); 
		searchSourceBuilder.query(qBuilder); 
		searchSourceBuilder.from(startNum);
		searchSourceBuilder.size(size);
		searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
		if(highlightList != null) {
			HighlightBuilder highlightBuilder = new HighlightBuilder(); 
			for(HighlightData data : highlightList) {
				HighlightBuilder.Field highlightField = new HighlightBuilder.Field(data.getField());
				if(data.getType() != null)
					highlightField.highlighterType(data.getType());
				else
					highlightField.highlighterType("unified");
				if(data.getPreTag() != null && data.getPostTag() != null) {
					highlightField.preTags(data.getPreTag());
					highlightField.postTags(data.getPostTag());
				}
				highlightBuilder.field(highlightField);
				
				searchSourceBuilder.highlighter(highlightBuilder);
			}
		}
		if(sortList != null) {
			for(SortData data : sortList) {
				if("score".equals(data.getField()))
					searchSourceBuilder.sort(new ScoreSortBuilder().order(data.getType()));
				else if("id".equals(data.getField()))
					searchSourceBuilder.sort(new FieldSortBuilder("_uid").order(data.getType()));
				else 
					searchSourceBuilder.sort(new FieldSortBuilder(data.getField()).order(data.getType()));
			}
		}
		if(indices != null)
			request.indices(indices);
		request.source(searchSourceBuilder); 
		SearchResponse response = null;
		try {
			response = client.search(request, RequestOptions.DEFAULT);
			RestStatus status = response.status();
			TimeValue took = response.getTook();
			log.info(status+" / " + took);
		} catch (IOException e) {
			log.error(e);
		}
		return response;
	}

	public SearchResponse searchScroll(QueryBuilder qBuilder, int size, String[] indices) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		return _searchScroll(searchSourceBuilder, qBuilder, size, indices, null, null);
	}

	public SearchResponse searchScroll(QueryBuilder qBuilder, int size, String[] indices, List<SortData> sortList) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		return _searchScroll(searchSourceBuilder, qBuilder, size, indices, sortList, null);
	}

	public SearchResponse searchScroll(QueryBuilder qBuilder, int size, String[] indices, List<SortData> sortList, List<HighlightData> highlightList) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		return _searchScroll(searchSourceBuilder, qBuilder, size, indices, sortList, highlightList);
	}

	public SearchResponse searchScroll(QueryBuilder qBuilder, int size, String[] indices, boolean fetchSource) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.fetchSource(fetchSource);
		return _searchScroll(searchSourceBuilder, qBuilder, size, indices, null, null);
	}
	
	public SearchResponse searchScroll(QueryBuilder qBuilder, int size, String[] indices, boolean fetchSource, List<SortData> sortList, List<HighlightData> highlightList, AggregationBuilder aggregation) {
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); 
		searchSourceBuilder.fetchSource(fetchSource);
		if(aggregation != null)
			searchSourceBuilder.aggregation(aggregation);
		return _searchScroll(searchSourceBuilder, qBuilder, size, indices, sortList, highlightList);
	}
	
	private SearchResponse _searchScroll(SearchSourceBuilder searchSourceBuilder, QueryBuilder qBuilder, int size, String[] indices, 
			List<SortData> sortList, List<HighlightData> highlightList) {
		final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
		SearchRequest searchRequest = new SearchRequest(indices);
		searchSourceBuilder.query(qBuilder);
		searchSourceBuilder.size(size); 
		searchRequest.source(searchSourceBuilder);
		searchRequest.scroll(scroll); 
		searchSourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS));
		if(highlightList != null) {
			HighlightBuilder highlightBuilder = new HighlightBuilder(); 
			for(HighlightData data : highlightList) {
				HighlightBuilder.Field highlightField = new HighlightBuilder.Field(data.getField());
				if(data.getType() != null)
					highlightField.highlighterType(data.getType());
				else
					highlightField.highlighterType("unified");
				if(data.getPreTag() != null && data.getPostTag() != null) {
					highlightField.preTags(data.getPreTag());
					highlightField.postTags(data.getPostTag());
				}
				highlightBuilder.field(highlightField);
				
				searchSourceBuilder.highlighter(highlightBuilder);
			}
		}
		if(sortList != null) {
			for(SortData data : sortList) {
				if("score".equals(data.getField()))
					searchSourceBuilder.sort(new ScoreSortBuilder().order(data.getType()));
				else if("id".equals(data.getField()))
					searchSourceBuilder.sort(new FieldSortBuilder("_uid").order(data.getType()));
				else 
					searchSourceBuilder.sort(new FieldSortBuilder(data.getField()).order(data.getType()));
			}
		}
		
		SearchResponse response = null;
		try {
			response = client.search(searchRequest, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return response;
	}
	
	/**
	 * RestHighLevelClient 객체를 반환
	 * @return
	 */
	public RestHighLevelClient getClient() {
		return client;
	}
	
	/**
	 * Search Template으로 검색한다.
	 * @param script
	 * @param scriptParams
	 * @param indices
	 * @return
	 */
	public SearchResponse searchTemplate(String script, Map<String, Object> scriptParams, String... indices) {
		SearchTemplateRequest request = new SearchTemplateRequest();
		request.setRequest(new SearchRequest(indices)); 

		request.setScriptType(ScriptType.INLINE);
		request.setScript( script);

		request.setScriptParams(scriptParams);
		
		SearchTemplateResponse searchTemplateResponse;
		SearchResponse response = null;
		try {
			searchTemplateResponse = client.searchTemplate(request, RequestOptions.DEFAULT);
			response = searchTemplateResponse.getResponse();
		} catch (IOException e) {
			log.error(e);
		}
		return response;
	}
}
