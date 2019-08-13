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

package org.apache.flink.table.runtime.parquet;

import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.dataformat.GenericRow;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.junit.After;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import static org.apache.flink.table.runtime.parquet.ParquetTestUtil.checkWriteParquet;
import static org.apache.flink.table.runtime.parquet.ParquetTestUtil.setField;
import static org.apache.hadoop.hdfs.server.common.Storage.deleteDir;

/**
 * Tests for {@link OutputFormat} and {@link RowParquetOutputFormat}.
 */
public class RowParquetOutputFormatTest {

	private String[] fieldNames = new String[]{"f1", "f2", "f3", "f4", "f5"};
	private LogicalType[] fieldTypes = new LogicalType[]{
			new BooleanType(),
			new SmallIntType(),
			DataTypes.STRING().getLogicalType(),
			new DoubleType(),
			new TinyIntType()};
	private String path = System.getProperty("java.io.tmpdir") + "/" + UUID.randomUUID();

	@Test
	public void testWriteToOneBlock() throws IOException {
		int blockSize = 1024;
		int generatorSize = 1024;
		int split = 1;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				setField(r, 0, null);
				setField(r, 1, (short) random.nextInt());
				setField(r, 2, random.nextInt() + "testWriteToOneBlock");
				setField(r, 3, random.nextDouble());
				setField(r, 4, (byte) random.nextInt(256));
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		checkWriteParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			false,
			CompressionCodecName.UNCOMPRESSED,
			split,
			rowIterator,
			expertRows);
	}

	@Test
	public void testWriteToMultiBlock() throws IOException {
		int blockSize = 256;
		int generatorSize = 2048;
		int split = 2;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				setField(r, 0, true);
				setField(r, 1, (short) random.nextInt());
				setField(r, 2, random.nextInt() + "testWriteToMultiBlock");
				setField(r, 3, random.nextDouble());
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		checkWriteParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			false,
			CompressionCodecName.UNCOMPRESSED,
			split,
			rowIterator,
			expertRows);
	}

	@Test
	public void testDictionaryWrite() throws IOException {
		int blockSize = 256;
		int generatorSize = 2048;
		int split = 4;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				setField(r, 0, true);
				setField(r, 1, (short) random.nextInt());
				setField(r, 2, random.nextInt() + "testDictionaryWrite");
				setField(r, 3, random.nextDouble());
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {

			}
		};

		checkWriteParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			true,
			CompressionCodecName.UNCOMPRESSED,
			split,
			rowIterator,
			expertRows);
	}

	@Test
	public void testSNAPPYWrite() throws IOException {
		int blockSize = 256;
		int generatorSize = 2048;
		int split = 4;
		final Random random = new Random(generatorSize);
		final List<GenericRow> expertRows = new ArrayList<>();
		Iterator<GenericRow> rowIterator = new ParquetTestUtil.GeneratorRow(generatorSize) {
			@Override
			public GenericRow next() {
				GenericRow r = new GenericRow(fieldNames.length);
				setField(r, 0, true);
				setField(r, 1, (short) random.nextInt());
				setField(r, 2, random.nextInt() + "testSNAPPYWrite");
				setField(r, 3, random.nextDouble());
				expertRows.add(r);
				return r;
			}

			@Override
			public void remove() {
			}
		};

		checkWriteParquet(
			path,
			fieldTypes,
			fieldNames,
			blockSize,
			true,
			CompressionCodecName.SNAPPY,
			split,
			rowIterator,
			expertRows);
	}

	@After
	public void after() throws IOException {
		File file = new File(path);
		if (file.exists()) {
			deleteDir(file);
		}
	}
}
