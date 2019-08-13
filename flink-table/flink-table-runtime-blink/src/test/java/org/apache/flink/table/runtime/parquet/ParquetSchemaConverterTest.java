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

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TinyIntType;

import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.apache.parquet.schema.Types;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BOOLEAN;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.DOUBLE;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link ParquetSchemaConverter}.
 */
public class ParquetSchemaConverterTest {

	@Test
	public void testSupportedConversionWithRequired() throws Exception {
		testSupportedConversion(true);
	}

	@Test
	public void testSupportedConversionWithOptional() throws Exception {
		testSupportedConversion(false);
	}

	private void testSupportedConversion(boolean required) {
		Type.Repetition repetition = required ? Type.Repetition.REQUIRED : Type.Repetition.OPTIONAL;

		MessageType parquetSchema = Types.buildMessage().addFields(
				Types.primitive(BOOLEAN, repetition).named("boolean"),
				Types.primitive(FLOAT, repetition).named("float"),
				Types.primitive(DOUBLE, repetition).named("double"),
				Types.primitive(INT32, repetition).as(null).named("int_original_null"),
				Types.primitive(INT32, repetition).as(OriginalType.INT_8).named("int32_original_int8"),
				Types.primitive(INT32, repetition).as(OriginalType.INT_16).named("int32_original_int16"),
				Types.primitive(INT32, repetition).as(OriginalType.INT_32).named("int32_original_int32"),
				Types.primitive(INT32, repetition).as(OriginalType.DATE).named("int32_original_date"),
				Types.primitive(INT32, repetition).as(OriginalType.DECIMAL).precision(9).scale(2)
						.named("int32_original_decimal"),
				Types.primitive(INT64, repetition).as(null).named("int64_original_null"),
				Types.primitive(INT64, repetition).as(OriginalType.INT_64).named("int64_original_int64"),
				Types.primitive(INT64, repetition).as(OriginalType.DECIMAL).precision(18).scale(2)
						.named("int64_original_decimal"),
				Types.primitive(BINARY, repetition).as(null).named("binary_original_null"),
				Types.primitive(BINARY, repetition).as(OriginalType.UTF8).named("binary_original_uft8"),
				Types.primitive(BINARY, repetition).as(OriginalType.ENUM).named("binary_original_enum"),
				Types.primitive(BINARY, repetition).as(OriginalType.JSON).named("binary_original_json"),
				Types.primitive(BINARY, repetition).as(OriginalType.BSON).named("binary_original_bson"),
				Types.primitive(BINARY, repetition).as(OriginalType.DECIMAL).precision(9).scale(2)
						.named("binary_original_decimal"))
				.named("flink-parquet");

		ParquetSchemaConverter converter = new ParquetSchemaConverter();
		Map<String, LogicalType> fieldName2TypeMap = converter.convertToLogicalType(parquetSchema);

		Map<String, LogicalType> expectedFieldName2TypeMap = new HashMap<String, LogicalType>() {
			{
				put("boolean", new BooleanType());
				put("float", new FloatType());
				put("double", new DoubleType());
				put("int_original_null", new IntType());
				put("int32_original_int8", new TinyIntType());
				put("int32_original_int16", new SmallIntType());
				put("int32_original_int32", new IntType());
				put("int32_original_date", new DateType());
				put("int32_original_decimal", new DecimalType(9, 2));
				put("int64_original_null", new BigIntType());
				put("int64_original_int64", new BigIntType());
				put("int64_original_decimal", new DecimalType(18, 2));
				put("binary_original_null", DataTypes.BYTES().getLogicalType());
				put("binary_original_uft8", DataTypes.STRING().getLogicalType());
				put("binary_original_enum", DataTypes.STRING().getLogicalType());
				put("binary_original_json", DataTypes.STRING().getLogicalType());
				put("binary_original_bson", DataTypes.BYTES().getLogicalType());
				put("binary_original_decimal", new DecimalType(9, 2));
			}
		};

		assertEquals(expectedFieldName2TypeMap.size(), fieldName2TypeMap.size());
		for (Map.Entry<String, LogicalType> entry : expectedFieldName2TypeMap.entrySet()) {
			String expectedFieldName = entry.getKey();
			LogicalType expectedFieldType = entry.getValue();
			LogicalType actualTypeInfo = fieldName2TypeMap.get(expectedFieldName);
			assertEquals(expectedFieldType, actualTypeInfo);
		}
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_REPEATED() {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.repeated(BOOLEAN).named("repeated_boolean"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_GroupType() {
		MessageType parquetSchema = Types.buildMessage()
				.addFields(Types.requiredGroup().addFields(
						Types.required(BOOLEAN).named("boolean"),
						Types.optional(INT32).named("int32")
				).named("group_boolean"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_INT96() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT96).named("int96"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_FIXED_LEN_BYTE_ARRAY() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(FIXED_LEN_BYTE_ARRAY).length(10).named("fixed_len_byte_array"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT8() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.UINT_8).named("int32_uint_8"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT16() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.UINT_16).named("int32_uint_16"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT32() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.UINT_32).named("int32_uint_32"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_UINT64() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT64).as(OriginalType.UINT_64).named("int64_uint_64"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_TIME_MILLIS() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT32).as(OriginalType.TIME_MILLIS).named("int32_time_millis"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}

	@Test(expected = UnsupportedOperationException.class)
	public void testUnsupportedConversion_TIMESTAMP_MILLIS() {
		MessageType parquetSchema = Types.buildMessage()
				.addField(Types.required(INT64).as(OriginalType.TIMESTAMP_MILLIS).named("int64_timestamp_millis"))
				.named("flink-parquet");
		new ParquetSchemaConverter().convertToLogicalType(parquetSchema);
	}
}
