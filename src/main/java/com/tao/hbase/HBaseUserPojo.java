package com.tao.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.log4j.Logger;

import com.tao.bean.MatchInfo;
import com.tao.bean.UserInfo;

public class HBaseUserPojo extends HBaseOperateCommon{
	private static Logger logger = Logger.getLogger(UserInfo.class);
	private static String TABLE_NAME = "user"; //hbase中user表
	private static String IDX_TABLE_NAME = "bhdp_user_idx";
	
	private UserInfo userInfo; //hbase user表中userinfo列簇
	private MatchInfo matchInfo;//hbase user表中matchinfo列簇
	
	
	public HBaseUserPojo() {
		super(TABLE_NAME, IDX_TABLE_NAME);
	}

	/**
	 * 得到用户的服务状态详情
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String> getServiceDetailAll(String userId) throws IOException {
		userId = StringUtils.reverse(userId);

		List<String> list = new ArrayList<String>();
		Result rs = getResultMaxVersions(userId, "userInfo", "serviceDetail");
		for (KeyValue kv : rs.raw()) {
			String value = new String(kv.getValue());
			list.add(value);
		}
		return list;
	}

	/**
	 * 通过服务ID,得到用户的服务状态详情
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String> getServiceDetailByServiceId(String userId, int serviceId) throws IOException {
		userId = StringUtils.reverse(userId);

		List<String> list = new ArrayList<String>();
		Result rs = getResultMaxVersions(userId, "userInfo", "serviceDetail" + serviceId);
		for (KeyValue kv : rs.raw()) {
			String value = new String(kv.getValue());
			list.add(value);
		}
		return list;
	}

	/**
	 * 通过服务ID,得到用户的服务状态详情
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String> getServiceDetailByServiceSign(String userId, String serviceSign) throws IOException {
		userId = StringUtils.reverse(userId);

		List<String> list = new ArrayList<String>();
		Result rs = getResultMaxVersions(userId, "userInfo", "serviceDetail_" + serviceSign);
		for (KeyValue kv : rs.raw()) {
			String value = new String(kv.getValue());
			list.add(value);
		}
		return list;
	}

	/**
	 * 得到用户的索引登录详情信息
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String[]> getLoginDetail(String userId) throws IOException {
		userId = StringUtils.reverse(userId);

		List<String[]> list = new ArrayList<String[]>();
		Result rs = getResultMaxVersions(userId, "behaviourTTL", "loginDetail");
		for (KeyValue kv : rs.raw()) {

			String value = new String(kv.getValue());

			String[] strArr = new String[4];
			strArr[0] = new String(kv.getRow());
			strArr[1] = null;//DateUtil.getDateTime(kv.getTimestamp());
			strArr[2] = value.split("\\|")[0];
			strArr[3] = value.split("\\|")[1];

			list.add(strArr);
		}
		return list;
	}

	/**
	 * 得到用户的索引登录详情信息
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String[]> getLoginDetail(String userId, long minStamp, long maxStamp) throws IOException {
		userId = StringUtils.reverse(userId);

		List<String[]> list = new ArrayList<String[]>();
		Result rs = getResultMaxVersions(userId, "behaviourTTL", "loginDetail", minStamp, maxStamp);
		for (KeyValue kv : rs.raw()) {

			String value = new String(kv.getValue());

			String[] strArr = new String[4];
			strArr[0] = new String(kv.getRow());
			strArr[1] = null;//DateUtil.getDateTime(kv.getTimestamp());
			strArr[2] = value.split("\\|")[0];
			strArr[3] = value.split("\\|")[1];

			list.add(strArr);
		}
		return list;
	}

	/**
	 * 得到用户的索引登录详情信息
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String[]> getLoginDetail(String userId, String beginDateTime) throws IOException {
		userId = StringUtils.reverse(userId);

		long minStamp = 0;// DateUtil.getUnixTime(beginDateTime);
		long maxStamp = System.currentTimeMillis();
		return getLoginDetail(userId, minStamp, maxStamp);
	}

	/**
	 * 得到用户的发消息ID列表
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String> getMsgSendDetail(String userId) throws IOException {
		userId = StringUtils.reverse(userId);

		List<String> list = new ArrayList<String>();
		Result rs = getResultMaxVersions(userId, "behaviourTTL", "msgSendDetail");
		for (KeyValue kv : rs.raw()) {
			list.add(new String(kv.getValue()));
		}
		return list;
	}

	/**
	 * 得到用户的收消息ID列表
	 * 
	 * @return
	 * @throws IOException
	 */
	public List<String> getMsgReceiveDetail(String userId) throws IOException {
		userId = StringUtils.reverse(userId);

		List<String> list = new ArrayList<String>();
		Result rs = getResultMaxVersions(userId, "behaviourTTL", "msgReceiveDetail");
		for (KeyValue kv : rs.raw()) {
			list.add(new String(kv.getValue()));
		}
		return list;
	}

	public UserInfo getUserInfo(String userId) throws IOException {
		userId = StringUtils.reverse(userId);

		userInfo = new UserInfo();
		Result rs = getResult(userId, UserInfo.ROW_FAMILY);
		reflectSetValue(rs, userInfo);
		return userInfo;
	}

	public UserInfo getUserInfo(String userId, Set<String> qualifierSet) throws IOException {
		userId = StringUtils.reverse(userId);

		userInfo = new UserInfo();
		Result rs = getResult(userId, UserInfo.ROW_FAMILY, qualifierSet);
		reflectSetValue(rs, userInfo);
		return userInfo;
	}

	public MatchInfo getMatchInfo(String userId) throws IOException {
		userId = StringUtils.reverse(userId);

		matchInfo = new MatchInfo();
		Result rs = getResult(userId, MatchInfo.ROW_FAMILY);
		reflectSetValue(rs, matchInfo);
		return matchInfo;
	}

	public MatchInfo getMatchInfo(String userId, Set<String> qualifierSet) throws IOException {
		userId = StringUtils.reverse(userId);

		matchInfo = new MatchInfo();
		Result rs = getResult(userId, MatchInfo.ROW_FAMILY, qualifierSet);
		reflectSetValue(rs, matchInfo);
		return matchInfo;
	}

	/**
	 * 将hbase的行对应java的字段
	 * 
	 * @param scanner
	 * @return
	 * @throws IOException
	 */
	public List<UserInfo> getMultUserInfoByKeyList(String userId, List<String> rowKeyList) throws IOException {
		userId = StringUtils.reverse(userId);

		Result[] rArray = getResultList(rowKeyList);

		List<UserInfo> list = new ArrayList<UserInfo>();

		for (Result rs : rArray) {
			UserInfo info = new UserInfo();
			reflectSetValue(rs, info);
			list.add(info);
		}
		return list;
	}

	/**
	 * 将hbase的行对应java的字段
	 * 
	 * @param scanner
	 * @return
	 * @throws IOException
	 */
	public List<UserInfo> getMultUserInfoByKeyList(String userId, List<String> rowKeyList, Set<String> qualifierSet) throws IOException {
		userId = StringUtils.reverse(userId);

		Result[] rArray = getResultList(rowKeyList, UserInfo.ROW_FAMILY, qualifierSet);

		List<UserInfo> list = new ArrayList<UserInfo>();

		for (Result rs : rArray) {
			UserInfo info = new UserInfo();
			reflectSetValue(rs, info);
			list.add(info);
		}
		return list;
	}

	/**
	 * 将hbase的行对应java的字段
	 * 
	 * @param scanner
	 * @return
	 * @throws IOException
	 */
	public List<MatchInfo> getMultMatchInfoByKeyList(String userId, List<String> rowKeyList, Set<String> qualifierSet) throws IOException {
		userId = StringUtils.reverse(userId);

		Result[] rArray = getResultList(rowKeyList, MatchInfo.ROW_FAMILY, qualifierSet);

		List<MatchInfo> list = new ArrayList<MatchInfo>();

		for (Result rs : rArray) {
			MatchInfo info = new MatchInfo();
			reflectSetValue(rs, info);
			list.add(info);
		}
		return list;
	}

	public static void main(String[] args) throws IOException {

//		BhdpUser user = new BhdpUser();

//		Set<String> qualifierSet = new HashSet<String>();
//		qualifierSet.add("userName");// 用户昵称
//		qualifierSet.add("hasMainPhoto");// 是否有形象照
//		qualifierSet.add("photoNum");// 照片数量
//		qualifierSet.add("contactData");
//
//		String userId = "116239496";
//		UserInfo userInfo = user.getUserInfo(userId);
//		List<String> serviceList = user.getServiceDetailByServiceId(userId, 36);// 服务期详情
//		List<String> serviceList2 = user.getServiceDetailByServiceSign(userId, "S_SupremeMember");// 服务期详情(新表)

//		logger.info(userInfo.getKey());
//		logger.info(userInfo.getUserName());
//		logger.info(userInfo.getHasMainPhoto());
//		logger.info(userInfo.getPhotoNum());
//		logger.info(userInfo.getContactData());
//		logger.info(serviceList.toString());
//		logger.info(serviceList2.toString());

		// logger.info("------根据索引获取多条数据------");
		// matchModifyTime registerTime allUpdateTime
		// List<String> rowKeyList = user.getKeyListByIdxTableLeft("matchModifyTime", "2013-07-28 01:19");
		// List<String> rowKeyList = user.getKeyListByIdxTableBetween("registerTime", "2013-08-05 11:00:00", "2013-08-05 12:00:00");
		// logger.info("------------rowKeyList:" + rowKeyList.size());

		// Set<String> qualifierSet = new HashSet<String>();
		// qualifierSet.add("birthday");
		// qualifierSet.add("registerTime");
		// qualifierSet.add("userName");
		// List<UserInfo> list = user.getMultUserInfoByKeyList(rowKeyList, qualifierSet);
		// for (UserInfo temp : list) {
		// logger.info(temp.getRegisterTime() + " " + temp.getKey() + " " + temp.getAge() + " " + temp.getUserName());
		// }
		// logger.info("------------list:" + list.size());
	}
}
