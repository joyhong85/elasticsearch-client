package com.tistory.joyhong.elasticsearch.client.data;

import java.util.Map;

public class BulkData {

	public enum Type{
		CREATE, UPDATE, DELETE
	}
	
	private String indexName;
	private String typeName;
	private String id;
	private Map<String, Object> jsonMap;
	private Type actionType;
	
	public String getIndexName() {
		return indexName;
	}
	public void setIndexName(String indexName) {
		this.indexName = indexName;
	}
	public String getTypeName() {
		return typeName;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getId() {
		return id;
	}
	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}
	public Map<String, Object> getJsonMap() {
		return jsonMap;
	}
	public void setJsonMap(Map<String, Object> jsonMap) {
		this.jsonMap = jsonMap;
	}
	public Type getActionType() {
		return actionType;
	}
	public void setActionType(Type actionType) {
		this.actionType = actionType;
	}
	
}
