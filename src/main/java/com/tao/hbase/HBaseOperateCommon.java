package com.tao.hbase;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

/**
 * hbase
 * 
 * @author tao
 * 
 */
public class HBaseOperateCommon {

	private static Logger logger = Logger.getLogger(HBaseOperateCommon.class);
	private static Configuration conf = HBaseConfiguration.create();

	private HTable table;
	private String tableName;
	private String idxTableName;

	public static class HbaseKeyValueModel {
		public String rowKey;
		public String family;
		public String qualifier;
		public long ts;
		public String value;
	}

	public HBaseOperateCommon() {
	}

	public HBaseOperateCommon(String tableName) {
		this.tableName = tableName;
		try {
			table = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public HBaseOperateCommon(String tableName, Configuration conf) {
		this.tableName = tableName;
		this.conf = conf;
		try {
			table = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public HBaseOperateCommon(String tableName, String idxTableName) {
		this.tableName = tableName;
		this.idxTableName = idxTableName;
		try {
			table = new HTable(conf, tableName);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建�?���?
	 * 
	 * @throws IOException
	 */
	private void creatTable(HTableDescriptor descriptor) throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		if (admin.tableExists(tableName)) {
			logger.debug("table already exists!");
		} else {
			admin.createTable(descriptor);
			logger.debug("create table " + tableName + " ok.");
		}
	}

	/**
	 * 删除�?
	 * 
	 * @throws IOException
	 */
	private void deleteTable() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		admin.disableTable(tableName);
		admin.deleteTable(tableName);
		logger.debug("delete table " + tableName + " ok.");
	}

	/**
	 * 清空�?删除后重建表
	 * 
	 * @return
	 * @throws IOException
	 */
	public void truncateTable() throws IOException {
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor descriptor = admin.getTableDescriptor(Bytes.toBytes(tableName));
		deleteTable();
		creatTable(descriptor);
	}

	/**
	 * 通过rs反射批量设置属�?值，只能设置�?��的那个版本�?
	 * 
	 * @param rs
	 * @param obj
	 * @throws IllegalArgumentException
	 * @throws IllegalAccessException
	 */
	protected void reflectSetValue(Result rs, Object obj) {
		Map<String, String> keyVauleMap = new HashMap<String, String>();

		if (rs.getRow() == null) {
			return;
		}

		String rowKey = new String(rs.getRow());
		for (KeyValue kv : rs.raw()) {
			keyVauleMap.put(new String(kv.getQualifier()), new String(kv.getValue()));
		}

		Field[] fields = obj.getClass().getDeclaredFields();
		for (Field field : fields) {
			if (field.getType() == String.class && !Modifier.isStatic(field.getModifiers())) {
				try {
					field.setAccessible(true);
					if (field.getName().equals("key"))
						field.set(obj, rowKey);
					else if (keyVauleMap.get(field.getName()) != null) {
						field.set(obj, keyVauleMap.get(field.getName()));
					}
				} catch (IllegalArgumentException e) {
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 插入�?��记录,�?��列�?
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @throws IOException
	 */
	protected void addRecord(String rowKey, String family, String qualifier, String value) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		table.put(put);
	}

	protected void addRecord(String rowKey, String family, String qualifier, long ts, String value) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		put.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), ts, Bytes.toBytes(value));
		table.put(put);
	}

	protected void addRecord(List<HbaseKeyValueModel> list) throws IOException {
		List<Put> puts = new ArrayList<Put>();
		for (HbaseKeyValueModel model : list) {
			Put put = new Put(Bytes.toBytes(model.rowKey));
			put.add(Bytes.toBytes(model.family), Bytes.toBytes(model.qualifier), model.ts, Bytes.toBytes(model.value));
			puts.add(put);
		}
		table.put(puts);
	}

	/**
	 * 插入�?��记录,多个列�?
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param value
	 * @throws IOException
	 */
	protected void addRecord(String rowKey, String family, String[] qualifiers, String[] values) throws IOException {
		Put put = new Put(Bytes.toBytes(rowKey));
		for (int i = 0; i < qualifiers.length; i++) {
			if (values[i] != null) {
				put.add(Bytes.toBytes(family), Bytes.toBytes(String.valueOf(qualifiers[i])), Bytes.toBytes(values[i]));
			} else {
				put.add(Bytes.toBytes(family), Bytes.toBytes(String.valueOf(qualifiers[i])), null);
			}
		}
		table.put(put);
	}

	/**
	 * 删除�?��记录
	 * 
	 * @param rowKey
	 * @throws IOException
	 */
	protected void delRecord(String rowKey) throws IOException {
		Delete del = new Delete(Bytes.toBytes(rowKey));
		table.delete(del);
		logger.debug("del recored " + rowKey + " ok.");
	}

	/**
	 * 删除记录
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @throws IOException
	 */
	protected void delRecord(String rowKey, String family, String qualifier) throws IOException {
		Delete del = new Delete(Bytes.toBytes(rowKey));
		del.deleteColumns(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		table.delete(del);
	}

	/**
	 * 删除整表数据
	 * 
	 * @throws IOException
	 */
	protected void delAllRecord() throws IOException {
		Scan s = new Scan();
		ResultScanner scanner = table.getScanner(s);

		Iterator<Result> it = scanner.iterator();
		while (it.hasNext()) {
			Result result = it.next();
			Delete delete = new Delete(result.getRow());
			table.delete(delete);
			table.flushCommits();
		}
	}

	/**
	 * 查找多行行记�?返回结果�?
	 * 
	 * @param rowKeyList
	 * @return
	 * @throws IOException
	 */
	protected Result[] getResultList(List<String> rowKeyList) throws IOException {
		List<Get> gets = new ArrayList<Get>();
		for (String rowKey : rowKeyList) {
			gets.add(new Get(Bytes.toBytes(rowKey)));
		}
		return table.get(gets);
	}

	/**
	 * 查找多行行记�?返回结果�?
	 * 
	 * @param rowKeyList
	 * @param family
	 * @param qualifierSet
	 * @return
	 * @throws IOException
	 */
	protected Result[] getResultList(List<String> rowKeyList, String family, Set<String> qualifierSet) throws IOException {
		List<Get> gets = new ArrayList<Get>();
		for (String rowKey : rowKeyList) {
			Get get = new Get(Bytes.toBytes(rowKey));
			for (String qualifier : qualifierSet) {
				get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
			}
			gets.add(get);
		}
		return table.get(gets);
	}

	/**
	 * 查找�?��记录,测试�?
	 * 
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	protected void getOneRecord(String rowKey) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		Result rs = table.get(get);
		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.print(kv.getTimestamp() + " ");
			System.out.print(new String(kv.getValue()));
		}
	}

	/**
	 * 查找�?��记录,返回结果�?
	 * 
	 * @param rowKey
	 * @return
	 * @throws IOException
	 */
	protected Result getResult(String rowKey) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		return table.get(get);
	}

	/**
	 * 查找�?��记录,返回结果�?
	 * 
	 * @param rowKey
	 * @param family
	 * @return
	 * @throws IOException
	 */
	protected Result getResult(String rowKey, String family) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addFamily(Bytes.toBytes(family));
		return table.get(get);
	}

	/**
	 * 查找�?��记录,通过rowkey 和指定列,返回结果�?
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @return
	 * @throws IOException
	 */
	protected Result getResult(String rowKey, String family, String qualifier) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return table.get(get);
	}

	/**
	 * 查找�?��记录,通过rowkey 和指定列,返回结果�?
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifierSet
	 * @return
	 * @throws IOException
	 */
	protected Result getResult(String rowKey, String family, Set<String> qualifierSet) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		for (String qualifier : qualifierSet) {
			get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		}
		return table.get(get);
	}

	/**
	 * 查找�?��记录,通过rowkey 和指定列,返回结果�?
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @return
	 * @throws IOException
	 */
	protected Result getResultVersions(String rowKey, String family, String qualifier, int versions) throws
			IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setMaxVersions(versions);
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return table.get(get);
	}

	/**
	 * 查找�?��记录,通过rowkey 和指定列,返回结果�?
	 *
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @return
	 * @throws IOException
	 */
	protected Result getResultMaxVersions(String rowKey, String family, String qualifier) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setMaxVersions();
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		return table.get(get);
	}

	/**
	 * 查找�?��记录,通过rowkey 和指定列,返回结果�?
	 * 
	 * @param rowKey
	 * @param family
	 * @param qualifier
	 * @param minStamp
	 * @param maxStamp
	 * @return
	 * @throws IOException
	 */
	protected Result getResultMaxVersions(String rowKey, String family, String qualifier, long minStamp, long maxStamp) throws IOException {
		Get get = new Get(Bytes.toBytes(rowKey));
		get.setMaxVersions();
		get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
		get.setTimeRange(minStamp, maxStamp);
		return table.get(get);
	}

	/**
	 * hbase通过row key 的前�?��询记�?
	 * 
	 * @param tablename
	 * @param rowPrifix
	 * @throws IOException
	 */
	protected void scaneByPrefixFilter(String rowPrifix) throws IOException {
		Scan s = new Scan();
		s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
		ResultScanner rs = table.getScanner(s);
		for (Result r : rs) {
			KeyValue[] kv = r.raw();
			for (int i = 0; i < kv.length; i++) {
				System.out.print(new String(kv[i].getRow()) + "  ");
				System.out.print(new String(kv[i].getFamily()) + ":");
				System.out.print(new String(kv[i].getQualifier()) + "  ");
				System.out.print(kv[i].getTimestamp() + "  ");
				System.out.println(new String(kv[i].getValue()));
			}
		}
	}

	/**
	 * hbase通过row key 的前�?��询记�?
	 * 
	 * @param rowPrifix
	 * @throws IOException
	 */
	protected ResultScanner getPrefixFilter(String rowPrifix) throws IOException {
		Scan s = new Scan();
		s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
		return table.getScanner(s);
	}

	/**
	 * hbase通过row key 的前�?��询记�?
	 * 
	 * @param rowPrifix
	 * @throws IOException
	 */
	protected ResultScanner getPrefixFilterRs(String rowPrifix) throws IOException {
		Scan s = new Scan();
		// s.setFilter(new PrefixFilter(rowPrifix.getBytes()));
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);

		SingleColumnValueFilter filter2 = new SingleColumnValueFilter("SearchInfo".getBytes(), "Age".getBytes(), CompareOp.EQUAL, String.valueOf("25").getBytes());

		list.addFilter(filter2);
		list.addFilter(new PrefixFilter(rowPrifix.getBytes()));

		s.setFilter(list);

		return table.getScanner(s);
	}

	/**
	 * 根据列�?过滤器查数据，效率较�?
	 * 
	 * @throws IOException
	 */
	protected ResultScanner getRecordsByColumnFilterList(FilterList list) throws IOException {
		Scan s = new Scan();
		s.setFilter(list);
		return table.getScanner(s);
	}

	/**
	 * 根据�?��、结束key查询
	 * 
	 * @throws IOException
	 */
	protected ResultScanner getRecordsByStartStop(String startRow, String stopRow) throws IOException {
		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(startRow));
		s.setStopRow(Bytes.toBytes(stopRow));
		return table.getScanner(s);
	}

	/**
	 * 通过scan索引表得到要查询的keylist
	 * 
	 * 索引格式：createDateIdx#2013-07-12 14:26:49.0#nmsg2_77_2648979730
	 * 
	 * @param scanner
	 * @return
	 */
	protected List<String> getKeyListByScaner(ResultScanner scanner) {
		List<String> keyList = new ArrayList<String>();

		for (Result r : scanner) {
			KeyValue[] kv = r.raw();
			for (int i = 0; i < kv.length; i++) {
				keyList.add(new String(kv[i].getRow()).split("#")[2]);
			}
		}

		return keyList;
	}

	/**
	 * 根据条件得到rowkeyList( where left(registerTime)='2013-07-12 16:00:00' )
	 * 
	 * @param registerTime
	 * @return
	 * @throws IOException
	 */
	public List<String> getKeyListByIdxTableLeft(String indexColumnName, String time) throws IOException {
		if (idxTableName == null)
			return null;
		ResultScanner scanner = new HBaseOperateCommon(idxTableName).getPrefixFilter(indexColumnName + "#" + time);
		return getKeyListByScaner(scanner);
	}

	/**
	 * 
	 * 根据条件得到rowkeyList (where registerTime>="2013-07-12 16:00:00" and registerTime<"2013-07-12 16:00:05" )
	 * 
	 * @param indexColumnName
	 * @param begin
	 * @param stop
	 * @return
	 * @throws IOException
	 */
	public List<String> getKeyListByIdxTableBetween(String indexColumnName, String begin, String stop) throws IOException {
		if (idxTableName == null)
			return null;
		ResultScanner scanner = new HBaseOperateCommon(idxTableName).getRecordsByStartStop(indexColumnName + "#" + begin, indexColumnName + "#" + stop);
		return getKeyListByScaner(scanner);
	}

	public static Configuration getConf() {
		return conf;
	}

	public static void setConf(Configuration conf) {
		HBaseOperateCommon.conf = conf;
	}

	public String getTableName() {
		return tableName;
	}

	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	public HTable getTable() {
		return table;
	}

	public void setTable(HTable table) {
		this.table = table;
	}

	public static void main(String[] args) throws IOException {

		// get 'bhdp_user','53332121',{COLUMN=>'userInfo:familyDescription',VERSIONS=>100}

		// HBaseOperateCommon test = new HBaseOperateCommon("bhdp_user");
		// Result rs = test.getResultMaxVersions("53332121", "userInfo", "familyDescription");
		//
		// for (KeyValue kv : rs.raw()) {
		// System.out.print(new String(kv.getRow()) + " ");
		// System.out.print(new String(kv.getFamily()) + ":");
		// System.out.print(new String(kv.getQualifier()) + " ");
		// System.out.print(kv.getTimestamp() + " ");
		// System.out.println(new String(kv.getValue()) + "\n");
		// }

		String[] qualifiers = new String[] { "c1", "c2", "c3" };
		String[] values = new String[] { "100", "200", "300" };

		HBaseOperateCommon test = new HBaseOperateCommon("test");
		test.addRecord("r3", "userInfo", qualifiers, values);

	}
}
