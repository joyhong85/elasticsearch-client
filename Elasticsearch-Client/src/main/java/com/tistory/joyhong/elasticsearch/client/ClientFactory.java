package com.tistory.joyhong.elasticsearch.client;

import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;

/**
 * RestHighLevelClient  인스턴스를 생성하여 반환한다.
 * @author joyhong
 *
 */
public class ClientFactory {

	public static RestHighLevelClient createClient() {
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost("localhost", 9200, "http")));
		return client;
	}
	
	public static RestHighLevelClient createClient(String hostname) {
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, 9200, "http")));
		return client;
	}

	public static RestHighLevelClient createClient(String hostname, int port) {
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, "http")));
		return client;
	}

	public static RestHighLevelClient createClient(String hostname, int port, String scheme) {
		RestHighLevelClient client = new RestHighLevelClient(RestClient.builder(new HttpHost(hostname, port, scheme)));
		return client;
	}
	
	
}
