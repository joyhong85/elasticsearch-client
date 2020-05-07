package com.tistory.joyhong.elasticsearch.client.api;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.client.indices.GetIndexRequest;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeRequest;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse;
import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.admin.indices.flush.FlushResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.RestStatus;

/**
 * 인덱스 API 모음
 * 엘라스틱서치 자바 High Level REST Client v6.8
 * 참조 : https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.8/_indices_apis.html
 * @author joyhong
 *
 */
public class IndexApi {

	Logger log = LogManager.getLogger("indexLog");
	
	private RestHighLevelClient client;
	
	public IndexApi(RestHighLevelClient client) {
		this.client = client;
	}


	/**
	 * 인덱스 생성
	 * @param indexName
	 * @return
	 */
	public Boolean createIndex(String indexName, XContentBuilder settingsBuilder, XContentBuilder mappingBuilder) {

		// 인덱스생성 요청 객체
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		
		return _createIndex(request, indexName, settingsBuilder, mappingBuilder);
	}
	
	/**
	 * 인덱스 생성 with alias
	 * @param indexName
	 * @param settingsBuilder
	 * @param mappingBuilder
	 * @param aliasName
	 * @return
	 */
	public Boolean createIndex(String indexName, XContentBuilder settingsBuilder, XContentBuilder mappingBuilder, String aliasName) {

		// 인덱스생성 요청 객체
		CreateIndexRequest request = new CreateIndexRequest(indexName);
		
		// Alias 설정
        request.alias(new Alias(aliasName));

        return _createIndex(request, indexName, settingsBuilder, mappingBuilder);
	}
	
	/**
	 * 인덱스를 생성한다.
	 * @param request
	 * @param indexName
	 * @param settingsBuilder
	 * @param mappingBuilder
	 * @return
	 */
	private Boolean _createIndex(CreateIndexRequest request, String indexName, XContentBuilder settingsBuilder, XContentBuilder mappingBuilder) {
		boolean acknowledged = false;
		// 세팅 정보
		request.settings(settingsBuilder);
		// 매핑 정보
		request.mapping(mappingBuilder);
		// 인덱스 생성
		CreateIndexResponse createIndexResponse;
		try {
			createIndexResponse = client.indices().create(request, RequestOptions.DEFAULT);
			acknowledged = createIndexResponse.isAcknowledged();
		} catch (ElasticsearchException | IOException e) {
			log.error(e);
		}
		
		if(acknowledged)
			log.info(indexName + " 인덱스가 생성되었습니다.");
		else
			log.info(indexName + " 인덱스 생성을 실패했습니다.");

		return acknowledged;
	}
	
	/**
	 * 인덱스 삭제
	 * @param indexName
	 * @return
	 */
	public Boolean deleteIndex(String indexName) {
		boolean acknowledged = false;

		try {
			// 인덱스 삭제 요청 객체
			DeleteIndexRequest request = new DeleteIndexRequest(indexName);

			AcknowledgedResponse response = client.indices().delete(request, RequestOptions.DEFAULT);
			acknowledged = response.isAcknowledged();
		} catch (ElasticsearchException | IOException e) {
			log.error(e);
		}
		if(acknowledged)
			log.info(indexName + " 인덱스가 삭제되었습니다.");
		else
			log.info(indexName + " 인덱스 삭제를 실패했습니다.");

		return acknowledged;
	}
	
	/**
	 * 인덱스 Open
	 * @param indexName
	 * @return
	 */
	public Boolean openIndex(String indexName) {
		boolean acknowledged = false;

		try {
			// 인덱스 open
			OpenIndexRequest request = new OpenIndexRequest(indexName);

			OpenIndexResponse response = client.indices().open(request, RequestOptions.DEFAULT);

			acknowledged = response.isAcknowledged();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return acknowledged;
	}

	/**
	 * 인덱스 닫기
	 * @param indexName
	 * @return
	 * 
	 * http://localhost:9200/indexName/_close
	 */
	public boolean closeIndex(String indexName) {
		boolean acknowledged = false;

		try {
			// 인덱스 close
			CloseIndexRequest request = new CloseIndexRequest(indexName);

			AcknowledgedResponse response = client.indices().close(request, RequestOptions.DEFAULT);

			acknowledged = response.isAcknowledged();
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		return acknowledged;
	}
	
	/**
	 * 인덱스 존재 여부 확인
	 * @param indexName
	 * @return
	 */
	public Boolean existIndex(String indexName) {
		boolean acknowledged = false;
		
		try {
			// 인덱스 존재 확인
			GetIndexRequest request = new GetIndexRequest(indexName);
			
			acknowledged = client.indices().exists(request, RequestOptions.DEFAULT);
			
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		
		return acknowledged;
		
	}
	
	/**
	 * 텍스트에 대한 analyze 수행
	 * @param indexName
	 * @param analyzer
	 * @param text
	 * @return
	 */
	public List<AnalyzeToken> analyze(String indexName, String analyzer, String text) {
		AnalyzeRequest request = new AnalyzeRequest();
		request.index(indexName);
		request.text(text);  
		request.analyzer(analyzer);
		List<AnalyzeResponse.AnalyzeToken> tokens = null;
		try {
			AnalyzeResponse response = client.indices().analyze(request, RequestOptions.DEFAULT);
			tokens = response.getTokens(); 
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tokens;
	}
	
	/**
	 * 단일 인덱스를 refresh 한다.
	 * @param indexName
	 * @return
	 */
	public RefreshResponse refreshIndex(String indexName) {
		RefreshRequest request = new RefreshRequest("index1"); 
		return _refreshIndex(request);
	}
	
	/**
	 * 복수개의 인덱스들을 refresh 한다.
	 * @param indexNames
	 * @return
	 */
	public RefreshResponse refreshIndices(String ... indexNames) {
		RefreshRequest request = new RefreshRequest(indexNames); 
		return _refreshIndex(request);
	}
	
	/**
	 * 모든 인덱스들을 refresh 한다.
	 * @return
	 */
	public RefreshResponse refreshIndexAll() {
		RefreshRequest request = new RefreshRequest();
		return _refreshIndex(request);
	}
	
	/**
	 * refresh 수행
	 * @param request
	 * @return
	 */
	private RefreshResponse _refreshIndex(RefreshRequest request) {
		RefreshResponse refreshResponse = null;
		try {
			refreshResponse = client.indices().refresh(request, RequestOptions.DEFAULT);
		} catch (ElasticsearchException  e) {
			if (e.status() == RestStatus.NOT_FOUND) {
				log.error("인덱스를 찾을 수 없습니다.");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return refreshResponse;
	}
	
	/**
	 * 단일 인덱스를 flush 한다.
	 * @param indexName
	 * @return
	 */
	public FlushResponse flushIndex(String indexName) {
		FlushRequest request = new FlushRequest("index1"); 
		return _flushIndex(request);
	}
	
	/**
	 * 복수개의 인덱스들을 flush 한다.
	 * @param indexNames
	 * @return
	 */
	public FlushResponse flushIndices(String ... indexNames) {
		FlushRequest request = new FlushRequest(indexNames); 
		return _flushIndex(request);
	}
	
	/**
	 * 모든 인덱스들을 flush 한다.
	 * @return
	 */
	public FlushResponse flushIndexAll() {
		FlushRequest request = new FlushRequest();
		return _flushIndex(request);
	}
	
	/**
	 * flush 수행
	 * @param request
	 * @return
	 */
	private FlushResponse _flushIndex(FlushRequest request) {
		FlushResponse flushResponse = null;
		try {
			flushResponse = client.indices().flush(request, RequestOptions.DEFAULT);
		} catch (ElasticsearchException  e) {
			if (e.status() == RestStatus.NOT_FOUND) {
				log.error("인덱스를 찾을 수 없습니다.");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return flushResponse;
	}
	
}
