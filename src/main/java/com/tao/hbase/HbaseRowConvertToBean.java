package com.tao.hbase;

import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;

import com.tao.bean.MatchInfo;
import com.tao.bean.UserInfo;

public class HbaseRowConvertToBean {
	
	/**
	 * ��KeyValue����ת��UserInfo
	 * 
	 * @param UserInfo
	 * @param rowKey
	 * @param kvList
	 * @param pushType
	 */
	public static UserInfo convertHbaseDataTobean(UserInfo userInfo,String rowKey, List<KeyValue> kvList) {
		
		String userId = StringUtils.reverse(rowKey);
		if(userInfo == null)
			userInfo = new UserInfo();
		
		userInfo.setUserID(userId);
		
		for (KeyValue kv : kvList)
		{
			String cf = new String(kv.getFamily());
			String fieldName = new String(kv.getQualifier());
			String fieldValue = new String(kv.getValue());
			
			//�������е�""ת����null
			if(fieldValue.equals(""))
			{
				fieldValue = null;
			}
			
			// ����userInfo����
			if (cf.equals(UserInfo.ROW_FAMILY))
			{
				//nickName
				if (fieldName.equals("userName")) 
				{
					userInfo.setUserName(fieldValue);
				}
				//gender
				if (fieldName.equals("gender")) 
				{
					userInfo.setGender(fieldValue);
				}
				//brithday
				if (fieldName.equals("brithday")) 
				{
					userInfo.setBrithday(fieldValue);
				}
			}	
		}
		
		return userInfo;
	}	
	/**
	 * ��KeyValue����ת��MatchInfo
	 * 
	 * @param MatchInfo
	 * @param rowKey
	 * @param kvList
	 * @param pushType
	 */
	public static MatchInfo convertHbaseDataTobean(MatchInfo matchInfo,String rowKey, List<KeyValue> kvList) {
		
		String userId = StringUtils.reverse(rowKey);
		if(matchInfo == null)
			matchInfo = new MatchInfo();
		
		matchInfo.setUserID(userId);
		
		for (KeyValue kv : kvList)
		{
			String cf = new String(kv.getFamily());
			String fieldName = new String(kv.getQualifier());
			String fieldValue = new String(kv.getValue());
			
			//�������е�""ת����null
			if(fieldValue.equals(""))
			{
				fieldValue = null;
			}
			
			// ����userInfo����
			if (cf.equals(MatchInfo.ROW_FAMILY))
			{
				//nickName
				if (fieldName.equals("maxHeight")) 
				{
					matchInfo.setMaxHeight(fieldValue);
				}
				//gender
				if (fieldName.equals("minHeight")) 
				{
					matchInfo.setMinHeight(fieldValue);
				}
				//brithday
				if (fieldName.equals("maxAge")) 
				{
					matchInfo.setMaxAge(fieldValue);
				}
			}	
		}
		
		return matchInfo;
	}	
}
