package client.api;

import java.io.IOException;
import java.util.List;

import org.elasticsearch.action.admin.indices.analyze.AnalyzeResponse.AnalyzeToken;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.Test;

import com.tistory.joyhong.elasticsearch.client.ClientApi;
import com.tistory.joyhong.elasticsearch.client.api.IndexApi;

public class IndexTest {

	public String ip = "localhost";
	
	@Test
	public void createIndex() {
		ClientApi api = new ClientApi(ip);
		IndexApi index = api.getIndexApi();
		
		// settings
		XContentBuilder settingsBuilder = null;
		try {
			settingsBuilder = XContentFactory.jsonBuilder()
				.startObject()
					.field("number_of_shards", 5)
					.field("number_of_replicas", 0)

					.startObject("analysis")
						.startObject("analyzer")

							.startObject("arirang_index")
								.field("tokenizer", "arirang_tokenizer")
								.array("filter", new String[] { "arirang_filter", "arirang_hanja_filter", "arirang_rm_punc_filter", "joyhong_snowball" })
							.endObject()
						.endObject()
					.endObject()
				.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// 매핑정보
		XContentBuilder mappingBuilder = null;
		try {
			mappingBuilder = XContentFactory.jsonBuilder()
				.startObject()
					.startObject("properties")
						.startObject("code")
							.field("type", "keyword")
							.field("store", "true")
							.field("index_options", "docs")
						.endObject()
						.startObject("title")
							.field("type", "text")
							.field("store", "true")
							.field("index_options", "docs")
						.endObject()
						.startObject("date")
							.field("type", "date")
							.field("store", "true")
							.field("index_options", "docs")
						.endObject()
					.endObject()
				.endObject();
		} catch (IOException e) {
			e.printStackTrace();
		}
					
		boolean result = index.createIndex("test", settingsBuilder, mappingBuilder);
		
		api.close();
		
		System.out.println(result);
	}

	
//	@Test
	public void deleteIndex() {
		ClientApi api = new ClientApi(ip);
		IndexApi index = api.getIndexApi();
		boolean result = index.deleteIndex("test");
		
		api.close();
		
		System.out.println(result);
	}
	
//	@Test
	public void existIndex() {
		ClientApi api = new ClientApi(ip);
		IndexApi index = api.getIndexApi();
		boolean result = index.existIndex("test");
		
		api.close();
		
		System.out.println(result);
		
	}
	
//	@Test
	public void analyze() {
		ClientApi api = new ClientApi(ip);
		IndexApi index = api.getIndexApi();
		List<AnalyzeToken> rstList = index.analyze("test", "arirang_analyzer", "엘라스틱서치 자바 클라이언트 테스트 중입니다.");
		for(AnalyzeToken token : rstList) {
			System.out.println(token.getTerm() + "\t" + token.getPosition());
		}
		
		api.close();
		
	}
}
