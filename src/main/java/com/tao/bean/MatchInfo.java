package com.tao.bean;

public class MatchInfo {
	public static String ROW_FAMILY = "matchInfo";
	
	private String userID;
	private String maxHeight;
	private String minHeight;
	private String maxAge;
	private String minAge;
	private String maxWeight;
	private String minWeight;
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public String getMaxHeight() {
		return maxHeight;
	}
	public void setMaxHeight(String maxHeight) {
		this.maxHeight = maxHeight;
	}
	public String getMinHeight() {
		return minHeight;
	}
	public void setMinHeight(String minHeight) {
		this.minHeight = minHeight;
	}
	public String getMaxAge() {
		return maxAge;
	}
	public void setMaxAge(String maxAge) {
		this.maxAge = maxAge;
	}
	public String getMinAge() {
		return minAge;
	}
	public void setMinAge(String minAge) {
		this.minAge = minAge;
	}
	public String getMaxWeight() {
		return maxWeight;
	}
	public void setMaxWeight(String maxWeight) {
		this.maxWeight = maxWeight;
	}
	public String getMinWeight() {
		return minWeight;
	}
	public void setMinWeight(String minWeight) {
		this.minWeight = minWeight;
	}
	@Override
	public String toString() {
		return "MatchInfo [maxHeight=" + maxHeight + ", minHeight=" + minHeight
				+ ", maxAge=" + maxAge + ", minAge=" + minAge + ", maxWeight="
				+ maxWeight + ", minWeight=" + minWeight + "]";
	}
	
}
