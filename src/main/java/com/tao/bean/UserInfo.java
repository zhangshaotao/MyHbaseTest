package com.tao.bean;

public class UserInfo {
	
	public static String ROW_FAMILY = "userInfo";
	
	private String userName;
	private String userID;
	private String brithday;
	private String gender;
	public String getUserName() {
		return userName;
	}
	public void setUserName(String userName) {
		this.userName = userName;
	}
	public String getUserID() {
		return userID;
	}
	public void setUserID(String userID) {
		this.userID = userID;
	}
	public String getBrithday() {
		return brithday;
	}
	public void setBrithday(String brithday) {
		this.brithday = brithday;
	}
	public String getGender() {
		return gender;
	}
	public void setGender(String gender) {
		this.gender = gender;
	}
	@Override
	public String toString() {
		return "User [userName=" + userName + ", userID=" + userID
				+ ", brithday=" + brithday + ", gender=" + gender
				+ ", getUserName()=" + getUserName() + ", getUserID()="
				+ getUserID() + ", getBrithday()=" + getBrithday()
				+ ", getGender()=" + getGender() + ", getClass()=" + getClass()
				+ ", hashCode()=" + hashCode() + ", toString()="
				+ super.toString() + "]";
	}
	
	
	
}
