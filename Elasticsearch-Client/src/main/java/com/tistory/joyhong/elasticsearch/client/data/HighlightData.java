package com.tistory.joyhong.elasticsearch.client.data;

public class HighlightData {

	private String field;
	private String type;
	private String preTag;
	private String postTag;
	
	public String getField() {
		return field;
	}
	public void setField(String field) {
		this.field = field;
	}
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getPreTag() {
		return preTag;
	}
	public void setPreTag(String preTag) {
		this.preTag = preTag;
	}
	public String getPostTag() {
		return postTag;
	}
	public void setPostTag(String postTag) {
		this.postTag = postTag;
	}
	
}
