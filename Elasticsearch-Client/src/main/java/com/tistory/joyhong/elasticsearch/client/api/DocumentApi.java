package com.tistory.joyhong.elasticsearch.client.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetRequest;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;

import com.tistory.joyhong.elasticsearch.client.data.BulkData;

/**
 * Document API 모음
 * 엘라스틱서치 자바 High Level REST Client v6.8
 * 참조 : https://www.elastic.co/guide/en/elasticsearch/client/java-rest/6.8/java-rest-high-supported-apis.html
 * @author joyhong
 *
 */
public class DocumentApi {

	Logger log = LogManager.getLogger("documentLog");
	
	
	private RestHighLevelClient client;
	
	public DocumentApi(RestHighLevelClient client) {
		this.client = client;
	}
	
	/**
	 * 문서를 해당 인덱스에 생성한다.
	 * 문서는 XContentBuilder 로 기술
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @param indexBuilder
	 * @return
	 */
	public IndexResponse createDocument(String indexName, String typeName, String _id, XContentBuilder indexBuilder) {
		
		// 데이터 추가
		IndexRequest request = new IndexRequest(indexName, typeName, _id);
		request.source(indexBuilder);
		
		return _createDocument(request, indexName, _id);
	}
	
	/**
	 * 문서를 해당 인덱스에 생성한다.
	 * 문서는 String 로 기술
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @param jsonString
	 * @return
	 */
	public IndexResponse createDocument(String indexName, String typeName, String _id, String jsonString) {
		
		// 데이터 추가
		IndexRequest request = new IndexRequest(indexName, typeName, _id);
		request.source(jsonString, XContentType.JSON);
		
		return _createDocument(request, indexName, _id);
	}
	
	/**
	 * 문서를 해당 인덱스에 생성한다.
	 * 문서는 Map<String, Object> 로 기술
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @param jsonMap
	 * @return
	 */
	public IndexResponse createDocument(String indexName, String typeName, String _id, Map<String, Object> jsonMap) {
		
		// 데이터 추가
		IndexRequest request = new IndexRequest(indexName, typeName, _id);
		request.source(jsonMap);
		
		return _createDocument(request, indexName, _id);
	}
	
	/**
	 * 문서를 해당 인덱스에 생성
	 * @param request
	 * @param indexName
	 * @param _id
	 * @return
	 */
	private IndexResponse _createDocument(IndexRequest request, String indexName, String _id) {
		// 결과 조회
		IndexResponse response = null;
		try {
			response = client.index(request, RequestOptions.DEFAULT);
			log.info(response.status() + " in " +indexName + " :: created id=" + _id);
		} catch (ElasticsearchException | IOException e) {
			if(((ElasticsearchException) e).status().equals(RestStatus.CONFLICT)) {
				log.error("문서 생성 실패");
			}
			log.error(e);
		}
		
		return response;
	}
	
	/**
	 * 문서를 조회하고 해당 문서의 source를 반환
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @return
	 */
	public GetResponse getDocument(String indexName, String typeName, String _id) {
		GetRequest request = new GetRequest(indexName, typeName, _id);
		return _getDocument(request);
	}
	
	/**
	 * 문서를 조회하고 해당 문서의 특정 필드만을 반환
	 * 이 필드는 매핑에서 저장하도록 설정이 되어 있어야만 가능하다.
	 * @param indexName
	 * @param typeName
	 * @param fieldName
	 * @param _id
	 * @return
	 */
	public GetResponse getDocument(String indexName, String typeName, String[] fieldName, String _id) {
		GetRequest request = new GetRequest(indexName, typeName, _id);
		//fieldName만을 결과로 반환
		request.storedFields(fieldName); 
		return _getDocument(request);
	}

	/**
	 * 문서를 조회하고 해당 문서의 source를 반환
	 * source에 포함할 필드와 포함하지 않을 필드는 인수로 넘겨 받은 값에 따라 정해진다.
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @param includeField
	 * @param excludeField
	 * @return
	 */
	public GetResponse getDocument(String indexName, String typeName, String _id, String[] includeField, String[] excludeField) {
		GetRequest request = new GetRequest(indexName, typeName, _id);
		//결과로 반환하는 source에 포함할 필드와 포함하지 않을 필드를 설명
		FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includeField, excludeField);
		request.fetchSourceContext(fetchSourceContext); 
		return _getDocument(request);
	}
	
	/**
	 * 문서를 조회한다.
	 * @param request
	 * @return
	 */
	private GetResponse _getDocument(GetRequest request) {
		GetResponse response = null;
		try {
			response = client.get(request, RequestOptions.DEFAULT);
			if(!response.isExists()) {
				log.info("문서를 찾을 수 없습니다.");
				return null;
			}
		} catch (ElasticsearchException  e) {
			if (e.status() == RestStatus.NOT_FOUND) {
				log.error("인덱스를 찾을 수 없습니다.");
			}
		} catch (IOException e) {
			log.error(e);
		}
		return response;
	}
	
	/**
	 * 복수개의 문서를 조회한다.
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @return
	 */
	public MultiGetResponse getMultiDocument(String indexName, String typeName, String... _id) {
		MultiGetRequest request = new MultiGetRequest();
		for(String id : _id) {
			request.add(new MultiGetRequest.Item(indexName, typeName, id));
		}
		MultiGetResponse response = null;
		try {
			response = client.mget(request, RequestOptions.DEFAULT);
			
		} catch (IOException e) {
			log.error(e);
		}
		return response;
	}
	
	/**
	 * 복수개의 문서를 조회하고 해당 문서의 특정 필드만을 반환
	 * 이 필드는 매핑에서 저장하도록 설정이 되어 있어야만 가능하다.
	 * @param indexName
	 * @param typeName
	 * @param fieldName
	 * @param _id
	 * @return
	 */
	public MultiGetResponse getMultiDocument(String indexName, String typeName, String[] fieldName, String... _id) {
		MultiGetRequest request = new MultiGetRequest();
		for(String id : _id) {
			request.add(new MultiGetRequest.Item(indexName, typeName, id).storedFields(fieldName));
		}
		MultiGetResponse response = null;
		try {
			response = client.mget(request, RequestOptions.DEFAULT);
			
		} catch (IOException e) {
			log.error(e);
		}
		return response;
	}

	/**
	 * 복수개의 문서를 조회하고 해당 문서의 source를 반환
	 * source에 포함할 필드와 포함하지 않을 필드는 인수로 넘겨 받은 값에 따라 정해진다.
	 * @param indexName
	 * @param typeName
	 * @param includeField
	 * @param excludeField
	 * @param _id
	 * @return
	 */
	public MultiGetResponse getMultiDocument(String indexName, String typeName, String[] includeField, String[] excludeField, String... _id) {
		MultiGetRequest request = new MultiGetRequest();
		FetchSourceContext fetchSourceContext = new FetchSourceContext(true, includeField, excludeField);
		for(String id : _id) {
			request.add(new MultiGetRequest.Item(indexName, typeName, id).fetchSourceContext(fetchSourceContext));
		}
		MultiGetResponse response = null;
		try {
			response = client.mget(request, RequestOptions.DEFAULT);
			
		} catch (IOException e) {
			log.error(e);
		}
		return response;
	}
	
	
	/**
	 * 문서가 해당 인덱스에 존재하는지 여부를 반환한다.
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @return
	 */
	public Boolean existDocument(String indexName, String typeName, String _id) {
		GetRequest request = new GetRequest(indexName, typeName, _id);
		
		request.fetchSourceContext(new FetchSourceContext(false));
		request.storedFields("_none_");
		boolean exists = false; 
		try {
			exists = client.exists(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return exists;
		
	}
	
	public UpdateResponse updateDocumentWithScript(String indexName, String typeName, String _id, String updateField, String value) {
		UpdateRequest request = new UpdateRequest(indexName, typeName, _id);
		Map<String, Object> parameters =  Collections.singletonMap(updateField, value); 

		Script inline = new Script(ScriptType.INLINE, "painless",
		        "ctx._source."+updateField+" = params."+updateField, parameters);  
		request.script(inline);
		
		return _updateDocument(request, indexName, _id);
	}
	
	/**
	 * 문서를 업데이트 한다.
	 * 업데이트 내용을 json형태의 string으로 기술한다.
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @param jsonString
	 * @return
	 */
	public UpdateResponse updateDocument(String indexName, String typeName, String _id, String jsonString) {
		UpdateRequest request = new UpdateRequest(indexName, typeName, _id);
		request.doc(jsonString, XContentType.JSON);
		
		return _updateDocument(request, indexName, _id);
	}

	/**
	 * 문서를 업데이트 한다.
	 * 업데이트 내용을 Map<String, Object>으로 기술한다.
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @param jsonMap
	 * @return
	 */
	public UpdateResponse updateDocument(String indexName, String typeName, String _id, Map<String, Object> jsonMap) {
		UpdateRequest request = new UpdateRequest(indexName, typeName, _id);
		request.doc(jsonMap, XContentType.JSON);
		
		return _updateDocument(request, indexName, _id);
	}

	/**
	 * 문서를 업데이트 한다.
	 * 업데이트 내용을 XContentBuilder으로 기술한다.
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @param builder
	 * @return
	 */
	public UpdateResponse updateDocument(String indexName, String typeName, String _id, XContentBuilder builder) {
		UpdateRequest request = new UpdateRequest(indexName, typeName, _id);
		request.doc(builder, XContentType.JSON);
		
		return _updateDocument(request, indexName, _id);
	}
	
	/**
	 * 문서의 내용을 업데이트 한다.
	 * @param request
	 * @param indexName
	 * @param _id
	 * @return
	 */
	private UpdateResponse _updateDocument(UpdateRequest request, String indexName, String _id) {
		UpdateResponse response = null;
		try {
			response = client.update(request, RequestOptions.DEFAULT);
			log.info(response.status() + " in " +indexName + " :: updated id=" + _id);
		} catch (ElasticsearchException | IOException e) {
			if(((ElasticsearchException) e).status().equals(RestStatus.CONFLICT)) {
				log.error("문서 업데이트 실패");
			}
			log.error(e);
		}
		
		return response;
	}
	
	/**
	 * 문서를 삭제한다.
	 * @param indexName
	 * @param typeName
	 * @param _id
	 * @return
	 */
	public DeleteResponse deleteDocument(String indexName, String typeName, String _id) {
		DeleteRequest request = new DeleteRequest(indexName, typeName, _id);
		DeleteResponse response = null;
		try {
			response = client.delete(request, RequestOptions.DEFAULT);
			log.info(response.status() + " in " +indexName + " :: deleted id=" + _id);
		} catch (IOException e) {
			log.error(e);
		}
		return response;
	}
	
	public Object getBulkItem() {
		BulkRequest request = new BulkRequest(); 
		Object[] item = {request, client};
		return item;
	}
	
	/**
	 * BulkProcessor를 반환한다.
	 * flush는 기본 1000개로 정해진다.
	 * @return
	 */
	public BulkProcessor getBulkProcessor() {
		return _getBulkProcessor(1000); 
	}
	
	/**
	 * ArrayList<BulkData>를 입력받아 bulk request를 수행한다.
	 * @param bulkList
	 */
	public void bulkDocument(ArrayList<BulkData> bulkList) {
		
		BulkRequest request = new BulkRequest();
		
		for(BulkData data : bulkList) {
			if(data.getActionType().equals(BulkData.Type.CREATE))
				request.add(new IndexRequest(data.getIndexName(), data.getTypeName(), data.getId()).source(data.getJsonMap()));
			else if(data.getActionType().equals(BulkData.Type.UPDATE))
				request.add(new UpdateRequest(data.getIndexName(), data.getTypeName(), data.getId()).doc(data.getJsonMap()));
			else if(data.getActionType().equals(BulkData.Type.DELETE))
				request.add(new DeleteRequest(data.getIndexName(), data.getTypeName(), data.getId()));
		}
		
		BulkResponse response = null;
		try {
			response = client.bulk(request, RequestOptions.DEFAULT);
		} catch (IOException e) {
			log.error(e);
		}
		for (BulkItemResponse bulkItemResponse : response) { 
			if (bulkItemResponse.isFailed()) { 
		        BulkItemResponse.Failure failure =bulkItemResponse.getFailure(); 
		        log.info(failure.getIndex() +" - " + failure.getType() + " - "+ failure.getId() + " / " + failure.getMessage());
		    }
		}
		log.info(response.getItems());
		log.info(response.getTook());
		
	}

	
	/**
	 * BulkProcessor를 반환한다.
	 * @param bulkActions flush 할 건수
	 * @return
	 */
	public BulkProcessor getBulkProcessor(int bulkActions) {
		return _getBulkProcessor(bulkActions); 
	}
	
	private BulkProcessor _getBulkProcessor(int bulkActions ) {
		BulkProcessor bulkProcessor = BulkProcessor.builder(
				(request, bulkListener) ->
				client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),new BulkProcessor.Listener() {
					int count = 0;
					
					@Override
					public void beforeBulk(long l, BulkRequest bulkRequest) {
						count = count + bulkRequest.numberOfActions();
						log.info("Uploaded " + count + " so far");
					}
					@Override
					public void afterBulk(long l, BulkRequest bulkRequest, BulkResponse bulkResponse) {
						if (bulkResponse.hasFailures()) {
							for (BulkItemResponse bulkItemResponse : bulkResponse) {
								if (bulkItemResponse.isFailed()) {
									BulkItemResponse.Failure failure = bulkItemResponse.getFailure();
									log.info("Error " + failure.toString());
								}
							}
						}
					}
					@Override
					public void afterBulk(long l, BulkRequest bulkRequest, Throwable throwable) {
						log.info("Errors " + throwable.toString());
					}
				})
				.setBulkActions(bulkActions).setConcurrentRequests(0)
				.setFlushInterval(TimeValue.timeValueSeconds(30L))
				.build();
		return bulkProcessor; 
	}
	
	
	public Boolean bulkDocumentWithBulkProcessor(ArrayList<BulkData> bulkList) {
		return _bulkDocumentWithBulkProcessor(bulkList, 1000);
	}
	
	public Boolean bulkDocumentWithBulkProcessor(ArrayList<BulkData> bulkList, int bulkActions) {
		return _bulkDocumentWithBulkProcessor(bulkList, bulkActions);
	}
	
	private Boolean _bulkDocumentWithBulkProcessor(ArrayList<BulkData> bulkList, int bulkActions) {
		boolean terminated = false ;
		BulkProcessor bulkProcessor = _getBulkProcessor(bulkActions);
		
		for(BulkData data : bulkList) {
			if(data.getActionType().equals(BulkData.Type.CREATE))
				bulkProcessor.add(new IndexRequest(data.getIndexName(), data.getTypeName(), data.getId()).source(data.getJsonMap()));
			else if(data.getActionType().equals(BulkData.Type.UPDATE))
				bulkProcessor.add(new UpdateRequest(data.getIndexName(), data.getTypeName(), data.getId()).doc(data.getJsonMap()));
			else if(data.getActionType().equals(BulkData.Type.DELETE))
				bulkProcessor.add(new DeleteRequest(data.getIndexName(), data.getTypeName(), data.getId()));
		}
		try {
			terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
			log.info("BulkProcessor Finished...." + terminated);
			bulkProcessor.close();
		} catch (InterruptedException e) {
			log.error(e);
		} 
		return terminated;
	}
	
}
