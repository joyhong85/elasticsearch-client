package client.api;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.get.MultiGetItemResponse;
import org.elasticsearch.action.get.MultiGetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Test;

import com.tistory.joyhong.elasticsearch.client.ClientApi;
import com.tistory.joyhong.elasticsearch.client.api.DocumentApi;
import com.tistory.joyhong.elasticsearch.client.data.BulkData;
import com.tistory.joyhong.elasticsearch.client.data.BulkData.Type;
import com.tistory.joyhong.elasticsearch.client.db.ConnectionDB;

public class DocumentTest {

	public String ip = "localhost";
	
//	@Test
	public void createDocument1() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		XContentBuilder indexBuilder = XContentFactory.jsonBuilder()
			.startObject()
				.field("code", "2021")
				.field("title", "나니아 연대기")
				.field("date", new Date())
			.endObject();
		docApi.createDocument("test", "_doc", "1", indexBuilder);
		
		indexBuilder = XContentFactory.jsonBuilder()
			.startObject()
				.field("code", "2022")
				.field("title", "Walk to Remember")
				.field("date", new Date())
			.endObject();
		docApi.createDocument("test", "_doc", "2", indexBuilder);
		
		indexBuilder = XContentFactory.jsonBuilder()
			.startObject()
				.field("code", "2023")
				.field("title", "캡틴 아메리카")
				.field("date", new Date())
			.endObject();
		docApi.createDocument("test", "_doc", "3", indexBuilder);
		
		indexBuilder = XContentFactory.jsonBuilder()
			.startObject()
				.field("code", "2024")
				.field("title", "우리 정말 사랑했을까?")
				.field("date", new Date())
			.endObject();
		docApi.createDocument("test", "_doc", "4", indexBuilder);
		
		api.close();
	}
	
//	@Test
	public void createDocument2() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.KOREAN);
		String jsonString = "{" +
		        "\"code\":\"2025\"," +
		        "\"title\":\"퍼스트 어벤져\"," +
		        "\"date\":\"" + format.format(new Date()) +"\"" +
		        "}";
		IndexResponse result = docApi.createDocument("test", "_doc", "5", jsonString);
		api.close();

		System.out.println(result.getResult().toString());
	}
	
//	@Test
	public void createDocument3() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		Map<String, Object> jsonMap = new HashMap<>();
		jsonMap.put("code", "2026");
		jsonMap.put("title", "나의 캡틴 그대");
		jsonMap.put("date", new Date());
		
		IndexResponse result = docApi.createDocument("test", "_doc", "9", jsonMap);
		api.close();
		
		System.out.println(result.getResult().toString());
	}
	
//	@Test
	public void getDocument1() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		GetResponse result = docApi.getDocument("test", "_doc", "6");
		api.close();
		
		if(result != null) {
			System.out.println(result.getIndex() + "\t" + result.getType() + "\t" + result.getId());
			System.out.println(result.getSourceAsString());
			Map<String, Object> sourceMap = result.getSourceAsMap(); 
			Iterator<String> iter = sourceMap.keySet().iterator();
			String key;
			while(iter.hasNext()) {
				key = iter.next();
				System.out.println(key + "\t" + sourceMap.get(key));
			}
			
		}
	}
	
//	@Test
	public void getDocument2() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		String[] fileds = {"title"};
		GetResponse result = docApi.getDocument("test", "_doc", fileds, "6");
		api.close();
		
		if(result != null) {
			System.out.println(result.getIndex() + "\t" + result.getType() + "\t" + result.getId());
			System.out.println(result.getField("title").getValue().toString());
		}
	}
	
//	@Test
	public void getDocument3() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		String[] includeField= {"code", "title"};
		String[] excludeField= {"date"};
		GetResponse result = docApi.getDocument("test", "_doc", "6", includeField, excludeField);
		api.close();
		
		if(result != null) {
			System.out.println(result.getIndex() + "\t" + result.getType() + "\t" + result.getId());
			System.out.println(result.getSourceAsString());
		}
	}
	
//	@Test
	public void getDocument4() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		boolean result = docApi.existDocument("test", "_doc", "7");
		api.close();
		
		System.out.println(result);
	}
	
//	@Test
	public void getMultiDocument1() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		MultiGetResponse result = docApi.getMultiDocument("test", "_doc", "6", "7", "1");
		api.close();
		
		if(result != null) {
			Iterator<MultiGetItemResponse> iter = result.iterator();
			MultiGetItemResponse itemResponse;
			while(iter.hasNext()){
				itemResponse = iter.next();
				GetResponse response = itemResponse.getResponse();  
				if(response.isExists()) {
					System.out.println(response.getSourceAsString());
				}else {
					System.out.println(response);
				}
			}
			
		}
	}
	
//	@Test
	public void getMultiDocument2() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		String[] fileds = {"title", "code"};
		MultiGetResponse result = docApi.getMultiDocument("test", "_doc", fileds, "6", "7", "1");
		api.close();
		
		if(result != null) {
			Iterator<MultiGetItemResponse> iter = result.iterator();
			MultiGetItemResponse itemResponse;
			while(iter.hasNext()){
				itemResponse = iter.next();
				GetResponse response = itemResponse.getResponse();  
				if(response.isExists()) {
					System.out.println(response.getFields());
				}else {
					System.out.println(response);
				}
			}
			
		}
	}
	
//	@Test
	public void getMultiDocument3() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		String[] includeField= {"code", "title"};
		String[] excludeField= {"date"};
		MultiGetResponse result = docApi.getMultiDocument("test", "_doc", includeField, excludeField, "6", "7", "1");
		api.close();
		
		if(result != null) {
			Iterator<MultiGetItemResponse> iter = result.iterator();
			MultiGetItemResponse itemResponse;
			while(iter.hasNext()){
				itemResponse = iter.next();
				GetResponse response = itemResponse.getResponse();  
				if(response.isExists()) {
					System.out.println(response.getSourceAsString());
				}else {
					System.out.println(response);
				}
			}
			
		}
	}
	
//	@Test
	public void updateDocument1() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		String[] includeField= {"code", "title"};
		String[] excludeField= {"date"};
		UpdateResponse result = docApi.updateDocumentWithScript("test", "_doc", "1", "code", "1999");
		api.close();
		
		if(result != null) {
			System.out.println(result.getResult().toString());
			
		}
	}
//	@Test
	public void updateDocument2() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		Map<String, Object> jsonMap = new HashMap<String, Object>();
		jsonMap.put("code", "111");
		jsonMap.put("title", "타이틀");
		jsonMap.put("date", new Date());
		UpdateResponse result = docApi.updateDocument("test", "_doc", "1", jsonMap);
		api.close();
		
		if(result != null) {
			System.out.println(result.getResult().toString());
			
		}
	}
	
//	@Test
	public void bulk() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		Object[] item = (Object[]) docApi.getBulkItem();
		BulkRequest request = (BulkRequest) item[0];
		RestHighLevelClient client = (RestHighLevelClient) item[1];
		
		request.add(new IndexRequest("test", "_doc", "1").source(XContentType.JSON,"code", "100", "title","넘버원","date",new Date()));
		request.add(new UpdateRequest("test", "_doc", "2").doc(XContentType.JSON,"code", "101", "title","넘버투","date",new Date()));
		request.add(new DeleteRequest("test", "_doc", "3"));
		request.add(new IndexRequest("test", "_doc", "4").source(XContentType.JSON,"code", "103", "title","넘버포","date",new Date()));
		
		BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
		for (BulkItemResponse bulkItemResponse : bulkResponse) { 
		    DocWriteResponse itemResponse = bulkItemResponse.getResponse(); 

		    switch (bulkItemResponse.getOpType()) {
				case INDEX:    
				case CREATE:
				    IndexResponse indexResponse = (IndexResponse) itemResponse;
				    break;
				case UPDATE:   
				    UpdateResponse updateResponse = (UpdateResponse) itemResponse;
				    break;
				case DELETE:   
				    DeleteResponse deleteResponse = (DeleteResponse) itemResponse;
		    }
		    System.out.println(itemResponse);
		}
		api.close();
	}
	
	@Test
	public void bulkWithBulkData() throws IOException {
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		ArrayList<BulkData> bulkList = new ArrayList<BulkData>();
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", Locale.KOREAN);
		for(int i=1;i<101;i++) {
			BulkData data = new BulkData();
			data.setActionType(Type.CREATE);
			data.setIndexName("test");
			data.setTypeName("_doc");
			data.setId(String.valueOf(i));
			Map<String, Object> jsonMap = new HashMap<String, Object>();
			jsonMap.put("code", String.valueOf(i));
			jsonMap.put("title", "제목 "+ String.valueOf(i));
			jsonMap.put("date", format.format(new Date()));
			data.setJsonMap(jsonMap);
			bulkList.add(data);
		}
		
		BulkResponse response = docApi.bulkDocument(bulkList);
		
		System.out.println(response.status() + " / " + response.getTook());
		api.close();
	}
	
//	@Test
	public void bulkProcessor1() throws IOException {
		
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		BulkProcessor bulkProcessor = docApi.getBulkProcessor();
		
		XContentBuilder indexBuilder = null;
		for(int i=101;i<10001;i++) {
			try {
				indexBuilder = XContentFactory.jsonBuilder()
						.startObject()
						.field("code", String.valueOf(100+i))
						.field("title", "제목"+ String.valueOf(i))
						.field("date", new Date())
					.endObject();
			} catch (IOException e) {
				e.printStackTrace();
			}
			bulkProcessor.add(new IndexRequest("test", "_doc", String.valueOf(i)).source(indexBuilder));
		}
		try {
			boolean terminated = bulkProcessor.awaitClose(30L, TimeUnit.SECONDS);
			System.out.println(terminated);
			bulkProcessor.close();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
		api.close();
	}
	
//	@Test
	public void bulkProcessor2() throws IOException {
		
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		ArrayList<BulkData> bulkList = new ArrayList<BulkData>();
		for(int i=1;i<10001;i++) {
			BulkData data = new BulkData();
			data.setActionType(Type.CREATE);
			data.setIndexName("test");
			data.setTypeName("_doc");
			data.setId(String.valueOf(i));
			Map<String, Object> jsonMap = new HashMap<String, Object>();
			jsonMap.put("code", String.valueOf(i));
			jsonMap.put("title", "제목 "+ String.valueOf(i));
			jsonMap.put("date", new Date());
			data.setJsonMap(jsonMap);
			bulkList.add(data);
		}
		boolean result = docApi.bulkDocumentWithBulkProcessor(bulkList);
		System.out.println(result);
		
		api.close();
	}
	
//	@Test
	public void bulkProcessor3() throws IOException {
		
		ClientApi api = new ClientApi(ip);
		DocumentApi docApi = api.getDocumentApi();
		
		ConnectionDB con = new ConnectionDB();
		Connection conn = con.DB("com.mysql.cj.jdbc.Driver", "url", "id", "password");
		try {
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		ArrayList<BulkData> bulkList = new ArrayList<BulkData>();

		Statement stmt = null;  
		ResultSet rs = null;
		String SQL = "SELECT no, code, title FROM SD_BASE"; 
		try {
			stmt = conn.createStatement();
			rs = stmt.executeQuery(SQL); 
			int count = 0;
			BulkData data;
			Map<String, Object> jsonMap;
			while (rs.next()) { 
				String code = rs.getString("code"); 
				String title = rs.getString("title"); 
				
				data = new BulkData();
				data.setActionType(Type.CREATE);
				data.setIndexName("test");
				data.setTypeName("_doc");
				data.setId(String.valueOf(rs.getInt("no")));
				jsonMap = new HashMap<String, Object>();
				jsonMap.put("code", code);
				jsonMap.put("title", title);
				data.setJsonMap(jsonMap);
				bulkList.add(data);
				
				count++;
				if(count%1000 ==0) {
					boolean result = docApi.bulkDocumentWithBulkProcessor(bulkList);
					System.out.println(result +" :: " + count);
					bulkList.clear();
				}
			} 
			
			boolean result = docApi.bulkDocumentWithBulkProcessor(bulkList);
			System.out.println(result +" :: " + count);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				rs.close();
				stmt.close();
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}

		api.close();
	}
}
