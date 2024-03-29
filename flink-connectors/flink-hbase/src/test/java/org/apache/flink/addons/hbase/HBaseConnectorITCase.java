/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.addons.hbase;

import org.apache.flink.addons.hbase.util.HBaseTestBase;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.internal.TableImpl;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.TableFactoryService;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.planner.runtime.utils.BatchTableEnvUtil;
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil;
import org.apache.flink.table.runtime.utils.StreamITCase;
import org.apache.flink.table.sinks.TableSink;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.test.util.TestBaseUtils;
import org.apache.flink.types.Row;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.addons.hbase.util.PlannerType.OLD_PLANNER;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_PROPERTY_VERSION;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_VERSION;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_TYPE_VALUE_HBASE;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_VERSION_VALUE_143;
import static org.apache.flink.table.descriptors.HBaseValidator.CONNECTOR_ZK_QUORUM;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.junit.Assert.assertEquals;

/**
 * IT cases for HBase connector (including HBaseTableSource and HBaseTableSink).
 */
public class HBaseConnectorITCase extends HBaseTestBase {

	// -------------------------------------------------------------------------------------
	// HBaseTableSource tests
	// -------------------------------------------------------------------------------------

	@Test
	public void testTableSourceFullScan() throws Exception {
		TableEnvironment tEnv = createBatchTableEnv();
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		tEnv.registerTableSource("hTable", hbaseTable);

		Table table = tEnv.sqlQuery("SELECT " +
			"  h.family1.col1, " +
			"  h.family2.col1, " +
			"  h.family2.col2, " +
			"  h.family3.col1, " +
			"  h.family3.col2, " +
			"  h.family3.col3 " +
			"FROM hTable AS h");

		List<Row> results = collectBatchResult(table);
		String expected =
			"10,Hello-1,100,1.01,false,Welt-1\n" +
				"20,Hello-2,200,2.02,true,Welt-2\n" +
				"30,Hello-3,300,3.03,false,Welt-3\n" +
				"40,null,400,4.04,true,Welt-4\n" +
				"50,Hello-5,500,5.05,false,Welt-5\n" +
				"60,Hello-6,600,6.06,true,Welt-6\n" +
				"70,Hello-7,700,7.07,false,Welt-7\n" +
				"80,null,800,8.08,true,Welt-8\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceProjection() throws Exception {
		TableEnvironment tEnv = createBatchTableEnv();
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		tEnv.registerTableSource("hTable", hbaseTable);

		Table table = tEnv.sqlQuery("SELECT " +
			"  h.family1.col1, " +
			"  h.family3.col1, " +
			"  h.family3.col2, " +
			"  h.family3.col3 " +
			"FROM hTable AS h");

		List<Row> results = collectBatchResult(table);
		String expected =
			"10,1.01,false,Welt-1\n" +
				"20,2.02,true,Welt-2\n" +
				"30,3.03,false,Welt-3\n" +
				"40,4.04,true,Welt-4\n" +
				"50,5.05,false,Welt-5\n" +
				"60,6.06,true,Welt-6\n" +
				"70,7.07,false,Welt-7\n" +
				"80,8.08,true,Welt-8\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceFieldOrder() throws Exception {
		TableEnvironment tEnv = createBatchTableEnv();
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
		// shuffle order of column registration
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		tEnv.registerTableSource("hTable", hbaseTable);

		Table table = tEnv.sqlQuery("SELECT * FROM hTable AS h");

		List<Row> results = collectBatchResult(table);
		String expected =
			"Hello-1,100,1.01,false,Welt-1,10\n" +
				"Hello-2,200,2.02,true,Welt-2,20\n" +
				"Hello-3,300,3.03,false,Welt-3,30\n" +
				"null,400,4.04,true,Welt-4,40\n" +
				"Hello-5,500,5.05,false,Welt-5,50\n" +
				"Hello-6,600,6.06,true,Welt-6,60\n" +
				"Hello-7,700,7.07,false,Welt-7,70\n" +
				"null,800,8.08,true,Welt-8,80\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableSourceReadAsByteArray() throws Exception {
		TableEnvironment tEnv = createBatchTableEnv();
		// fetch row2 from the table till the end
		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_1);
		hbaseTable.addColumn(FAMILY2, F2COL1, byte[].class);
		hbaseTable.addColumn(FAMILY2, F2COL2, byte[].class);
		tEnv.registerTableSource("hTable", hbaseTable);
		tEnv.registerFunction("toUTF8", new ToUTF8());
		tEnv.registerFunction("toLong", new ToLong());

		Table table = tEnv.sqlQuery(
			"SELECT " +
				"  toUTF8(h.family2.col1), " +
				"  toLong(h.family2.col2) " +
				"FROM hTable AS h"
		);

		List<Row> results = collectBatchResult(table);
		String expected =
			"Hello-1,100\n" +
				"Hello-2,200\n" +
				"Hello-3,300\n" +
				"null,400\n" +
				"Hello-5,500\n" +
				"Hello-6,600\n" +
				"Hello-7,700\n" +
				"null,800\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}

	@Test
	public void testTableInputFormat() throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<Tuple1<Integer>> result = env
			.createInput(new InputFormatForTestTable())
			.reduce((ReduceFunction<Tuple1<Integer>>) (v1, v2) -> Tuple1.of(v1.f0 + v2.f0));

		List<Tuple1<Integer>> resultSet = result.collect();

		assertEquals(1, resultSet.size());
		assertEquals(360, (int) resultSet.get(0).f0);
	}

	// -------------------------------------------------------------------------------------
	// HBaseTableSink tests
	// -------------------------------------------------------------------------------------

	// prepare a source collection.
	private static final List<Row> testData1 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo1 = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.INT, Types.STRING, Types.LONG, Types.DOUBLE, Types.BOOLEAN, Types.STRING},
		new String[]{"rowkey", "f1c1", "f2c1", "f2c2", "f3c1", "f3c2", "f3c3"});

	static {
		testData1.add(Row.of(1, 10, "Hello-1", 100L, 1.01, false, "Welt-1"));
		testData1.add(Row.of(2, 20, "Hello-2", 200L, 2.02, true, "Welt-2"));
		testData1.add(Row.of(3, 30, "Hello-3", 300L, 3.03, false, "Welt-3"));
		testData1.add(Row.of(4, 40, null, 400L, 4.04, true, "Welt-4"));
		testData1.add(Row.of(5, 50, "Hello-5", 500L, 5.05, false, "Welt-5"));
		testData1.add(Row.of(6, 60, "Hello-6", 600L, 6.06, true, "Welt-6"));
		testData1.add(Row.of(7, 70, "Hello-7", 700L, 7.07, false, "Welt-7"));
		testData1.add(Row.of(8, 80, null, 800L, 8.08, true, "Welt-8"));
	}

	@Test
	public void testTableSink() throws Exception {
		HBaseTableSchema schema = new HBaseTableSchema();
		schema.addColumn(FAMILY1, F1COL1, Integer.class);
		schema.addColumn(FAMILY2, F2COL1, String.class);
		schema.addColumn(FAMILY2, F2COL2, Long.class);
		schema.setRowKey("rk", Integer.class);
		schema.addColumn(FAMILY3, F3COL1, Double.class);
		schema.addColumn(FAMILY3, F3COL2, Boolean.class);
		schema.addColumn(FAMILY3, F3COL3, String.class);

		Map<String, String> tableProperties = new HashMap<>();
		tableProperties.put("connector.type", "hbase");
		tableProperties.put("connector.version", "1.4.3");
		tableProperties.put("connector.property-version", "1");
		tableProperties.put("connector.table-name", TEST_TABLE_2);
		tableProperties.put("connector.zookeeper.quorum", getZookeeperQuorum());
		tableProperties.put("connector.zookeeper.znode.parent", "/hbase");
		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putTableSchema(SCHEMA, schema.convertsToTableSchema());
		descriptorProperties.putProperties(tableProperties);
		TableSink tableSink = TableFactoryService
			.find(HBaseTableFactory.class, descriptorProperties.asMap())
			.createTableSink(descriptorProperties.asMap());

		StreamExecutionEnvironment execEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, streamSettings);

		DataStream<Row> ds = execEnv.fromCollection(testData1).returns(testTypeInfo1);
		tEnv.registerDataStream("src", ds);
		tEnv.registerTableSink("hbase", tableSink);

		String query = "INSERT INTO hbase SELECT ROW(f1c1), ROW(f2c1, f2c2), rowkey, ROW(f3c1, f3c2, f3c3) FROM src";
		tEnv.sqlUpdate(query);

		// wait to finish
		tEnv.execute("HBase Job");

		// start a batch scan job to verify contents in HBase table
		// start a batch scan job to verify contents in HBase table
		TableEnvironment batchTableEnv = createBatchTableEnv();

		HBaseTableSource hbaseTable = new HBaseTableSource(getConf(), TEST_TABLE_2);
		hbaseTable.setRowKey("rowkey", Integer.class);
		hbaseTable.addColumn(FAMILY1, F1COL1, Integer.class);
		hbaseTable.addColumn(FAMILY2, F2COL1, String.class);
		hbaseTable.addColumn(FAMILY2, F2COL2, Long.class);
		hbaseTable.addColumn(FAMILY3, F3COL1, Double.class);
		hbaseTable.addColumn(FAMILY3, F3COL2, Boolean.class);
		hbaseTable.addColumn(FAMILY3, F3COL3, String.class);
		batchTableEnv.registerTableSource("hTable", hbaseTable);

		Table table = batchTableEnv.sqlQuery(
			"SELECT " +
				"  h.rowkey, " +
				"  h.family1.col1, " +
				"  h.family2.col1, " +
				"  h.family2.col2, " +
				"  h.family3.col1, " +
				"  h.family3.col2, " +
				"  h.family3.col3 " +
				"FROM hTable AS h"
		);

		List<Row> results = collectBatchResult(table);
		String expected =
			"1,10,Hello-1,100,1.01,false,Welt-1\n" +
				"2,20,Hello-2,200,2.02,true,Welt-2\n" +
				"3,30,Hello-3,300,3.03,false,Welt-3\n" +
				"4,40,,400,4.04,true,Welt-4\n" +
				"5,50,Hello-5,500,5.05,false,Welt-5\n" +
				"6,60,Hello-6,600,6.06,true,Welt-6\n" +
				"7,70,Hello-7,700,7.07,false,Welt-7\n" +
				"8,80,,800,8.08,true,Welt-8\n";

		TestBaseUtils.compareResultAsText(results, expected);
	}


	// -------------------------------------------------------------------------------------
	// HBase lookup source tests
	// -------------------------------------------------------------------------------------

	// prepare a source collection.
	private static final List<Row> testData2 = new ArrayList<>();
	private static final RowTypeInfo testTypeInfo2 = new RowTypeInfo(
		new TypeInformation[]{Types.INT, Types.LONG, Types.STRING},
		new String[]{"a", "b", "c"});

	static {
		testData2.add(Row.of(1, 1L, "Hi"));
		testData2.add(Row.of(2, 2L, "Hello"));
		testData2.add(Row.of(3, 2L, "Hello world"));
		testData2.add(Row.of(3, 3L, "Hello world!"));
	}

	@Test
	public void testHBaseLookupFunction() throws Exception {
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, streamSettings);
		StreamITCase.clear();

		// prepare a source table
		DataStream<Row> ds = streamEnv.fromCollection(testData2).returns(testTypeInfo2);
		Table in = streamTableEnv.fromDataStream(ds, "a, b, c");
		streamTableEnv.registerTable("src", in);

		Map<String, String> tableProperties = hbaseTableProperties();
		TableSource source = TableFactoryService
			.find(HBaseTableFactory.class, tableProperties)
			.createTableSource(tableProperties);

		streamTableEnv.registerFunction("hbaseLookup", ((HBaseTableSource) source).getLookupFunction(new String[]{ROWKEY}));

		// perform a temporal table join query
		String sqlQuery = "SELECT a,family1.col1, family3.col3 FROM src, LATERAL TABLE(hbaseLookup(a))";
		Table result = streamTableEnv.sqlQuery(sqlQuery);

		DataStream<Row> resultSet = streamTableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());

		streamEnv.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,10,Welt-1");
		expected.add("2,20,Welt-2");
		expected.add("3,30,Welt-3");
		expected.add("3,30,Welt-3");

		StreamITCase.compareWithList(expected);
	}

	@Test
	public void testHBaseLookupTableSource() throws Exception {
		if (OLD_PLANNER.equals(planner)) {
			// lookup table source is only supported in blink planner, skip for old planner
			return;
		}
		StreamExecutionEnvironment streamEnv = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment streamTableEnv = StreamTableEnvironment.create(streamEnv, streamSettings);
		StreamITCase.clear();

		// prepare a source table
		String srcTableName = "src";
		DataStream<Row> ds = streamEnv.fromCollection(testData2).returns(testTypeInfo2);
		Table in = streamTableEnv.fromDataStream(ds, "a, b, c, proc.proctime");
		streamTableEnv.registerTable(srcTableName, in);

		Map<String, String> tableProperties = hbaseTableProperties();
		TableSource source = TableFactoryService
			.find(HBaseTableFactory.class, tableProperties)
			.createTableSource(tableProperties);
		streamTableEnv.registerTableSource("hbaseLookup", source);
		// perform a temporal table join query
		String query = "SELECT a,family1.col1, family3.col3 FROM src " +
			"JOIN hbaseLookup FOR SYSTEM_TIME AS OF src.proc as h ON src.a = h.rk";
		Table result = streamTableEnv.sqlQuery(query);

		DataStream<Row> resultSet = streamTableEnv.toAppendStream(result, Row.class);
		resultSet.addSink(new StreamITCase.StringSink<>());

		streamEnv.execute();

		List<String> expected = new ArrayList<>();
		expected.add("1,10,Welt-1");
		expected.add("2,20,Welt-2");
		expected.add("3,30,Welt-3");
		expected.add("3,30,Welt-3");

		StreamITCase.compareWithList(expected);
	}

	private static Map<String, String> hbaseTableProperties() {
		Map<String, String> properties = new HashMap<>();
		properties.put(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_HBASE);
		properties.put(CONNECTOR_VERSION, CONNECTOR_VERSION_VALUE_143);
		properties.put(CONNECTOR_PROPERTY_VERSION, "1");
		properties.put(CONNECTOR_TABLE_NAME, TEST_TABLE_1);
		// get zk quorum from "hbase-site.xml" in classpath
		String hbaseZk = HBaseConfiguration.create().get(HConstants.ZOOKEEPER_QUORUM);
		properties.put(CONNECTOR_ZK_QUORUM, hbaseZk);
		// schema
		String[] columnNames = {FAMILY1, ROWKEY, FAMILY2, FAMILY3};
		TypeInformation<Row> f1 = Types.ROW_NAMED(new String[]{F1COL1}, Types.INT);
		TypeInformation<Row> f2 = Types.ROW_NAMED(new String[]{F2COL1, F2COL2}, Types.STRING, Types.LONG);
		TypeInformation<Row> f3 = Types.ROW_NAMED(new String[]{F3COL1, F3COL2, F3COL3}, Types.DOUBLE, Types.BOOLEAN, Types.STRING);
		TypeInformation[] columnTypes = new TypeInformation[]{f1, Types.INT, f2, f3};

		DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		TableSchema tableSchema = new TableSchema(columnNames, columnTypes);
		descriptorProperties.putTableSchema(SCHEMA, tableSchema);
		descriptorProperties.putProperties(properties);
		return descriptorProperties.asMap();
	}

	// ------------------------------- Utilities -------------------------------------------------

	/**
	 * Creates a Batch {@link TableEnvironment} depends on the {@link #planner} context.
	 */
	private TableEnvironment createBatchTableEnv() {
		if (OLD_PLANNER.equals(planner)) {
			ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
			return BatchTableEnvironment.create(env, new TableConfig());
		} else {
			return TableEnvironment.create(batchSettings);
		}
	}

	/**
	 * Collects batch result depends on the {@link #planner} context.
	 */
	private List<Row> collectBatchResult(Table table) throws Exception {
		TableImpl tableImpl = (TableImpl) table;
		if (OLD_PLANNER.equals(planner)) {
			BatchTableEnvironment batchTableEnv = (BatchTableEnvironment) tableImpl.getTableEnvironment();
			DataSet<Row> resultSet = batchTableEnv.toDataSet(table, Row.class);
			return resultSet.collect();
		} else {
			return JavaScalaConversionUtil.toJava(BatchTableEnvUtil.collect(tableImpl));
		}
	}

	/**
	 * A {@link ScalarFunction} that maps byte arrays to UTF-8 strings.
	 */
	public static class ToUTF8 extends ScalarFunction {
		private static final long serialVersionUID = 1L;

		public String eval(byte[] bytes) {
			return Bytes.toString(bytes);
		}
	}

	/**
	 * A {@link ScalarFunction} that maps byte array to longs.
	 */
	public static class ToLong extends ScalarFunction {
		private static final long serialVersionUID = 1L;

		public long eval(byte[] bytes) {
			return Bytes.toLong(bytes);
		}
	}

	/**
	 * A {@link TableInputFormat} for testing.
	 */
	public static class InputFormatForTestTable extends TableInputFormat<Tuple1<Integer>> {
		private static final long serialVersionUID = 1L;

		@Override
		protected Scan getScanner() {
			return new Scan();
		}

		@Override
		protected String getTableName() {
			return TEST_TABLE_1;
		}

		@Override
		protected Tuple1<Integer> mapResultToTuple(Result r) {
			return new Tuple1<>(Bytes.toInt(r.getValue(Bytes.toBytes(FAMILY1), Bytes.toBytes(F1COL1))));
		}
	}

}
