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

import org.apache.flink.table.dataformat.DataFormatConverters.DateConverter;
import org.apache.flink.table.dataformat.DataFormatConverters.TimeConverter;
import org.apache.flink.table.dataformat.DataFormatConverters.TimestampConverter;
import org.apache.flink.table.dataformat.Decimal;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.types.Row;
import org.apache.flink.util.Preconditions;

import org.apache.parquet.io.api.Binary;
import org.apache.parquet.io.api.Converter;
import org.apache.parquet.io.api.GroupConverter;
import org.apache.parquet.io.api.PrimitiveConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ParquetRecordConverter} is used to convert Parquet records into {@link Row}.
 *
 *<p>TODO supports more Flink Type
 */
public class ParquetRecordConverter extends GroupConverter {

	/**
	 * currentRow is reused to avoid creating row instance for each record.
	 */
	private final Row currentRow;
	private final List<Converter> fieldConverters;

	// as a key to match any Decimal(p,s)
	private static final DecimalType ANY_DEC_TYPE = Decimal.DECIMAL_SYSTEM_DEFAULT;

	private static ConverterCreator getConverterCreator(LogicalType type) {
		switch (type.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
				return new StringConverterCreator();
			case BOOLEAN:
				return new BooleanConverterCreator();
			case BINARY:
			case VARBINARY:
				return new ByteArrayConverterCreator();
			case DECIMAL:
				return new BigDecConverterCreator();
			case TINYINT:
				return new ByteConverterCreator();
			case SMALLINT:
				return new ShortConverterCreator();
			case INTEGER:
				return new IntConverterCreator();
			case BIGINT:
				return new LongConverterCreator();
			case FLOAT:
				return new FloatConverterCreator();
			case DOUBLE:
				return new DoubleConverterCreator();
			case DATE:
				return new SqlDateConverterCreator();
			case TIME_WITHOUT_TIME_ZONE:
				return new SqlTimeConverterCreator();
			case TIMESTAMP_WITHOUT_TIME_ZONE:
				return new SqlTimestampConverterCreator();
			default:
				throw new UnsupportedOperationException(type + " is not support");
		}
	}

	public ParquetRecordConverter(GroupType parquetType, LogicalType[] fieldTypes) {
		Preconditions.checkArgument(parquetType.getFieldCount() == fieldTypes.length);

		int fieldCount = parquetType.getFieldCount();
		currentRow = new Row(fieldCount);

		fieldConverters = new ArrayList<>(fieldCount);
		for (int i = 0; i < fieldCount; ++i) {
			Type type = parquetType.getType(i);
			LogicalType fieldType = fieldTypes[i];
			LogicalType fieldKey = (fieldType instanceof DecimalType) ? ANY_DEC_TYPE : fieldType;
			Converter converter = getConverterCreator(fieldKey).createConverter(type, new RowFieldSetter(currentRow, i));
			fieldConverters.add(converter);
		}
	}

	@Override
	public Converter getConverter(int fieldIndex) {
		return fieldConverters.get(fieldIndex);
	}

	public Row getCurrentRecord() {
		return currentRow;
	}

	@Override
	public void start() {
		// clear last value for each field
		for (int i = 0; i < currentRow.getArity(); ++i) {
			currentRow.setField(i, null);
		}
	}

	@Override
	public void end() {
	}

	/**
	 * Sets a value to the row at the specified position.
	 */
	private static class RowFieldSetter {
		private final Row currentRow;
		private final int position;

		RowFieldSetter(Row currentRow, int position) {
			this.currentRow = currentRow;
			this.position = position;
		}

		public void set(Object value) {
			currentRow.setField(position, value);
		}
	}

	/**
	 * A converter for primitive type.
	 */
	private abstract static class ParquetPrimitiveConverter extends PrimitiveConverter {

		final RowFieldSetter fieldSetter;

		ParquetPrimitiveConverter(RowFieldSetter fieldSetter) {
			this.fieldSetter = fieldSetter;
		}
	}

	/**
	 * A {@link ConverterCreator} is used to create a new {@link Converter} for specified {@link Type}.
	 */
	private abstract static class ConverterCreator {

		private Converter createConverter(Type type, RowFieldSetter fieldSetter) {
			validateType(type);
			return newConverter(fieldSetter);
		}

		protected abstract void validateType(Type type);

		protected abstract Converter newConverter(RowFieldSetter fieldSetter);
	}

	/**
	 * A {@link PrimitiveConverterCreator} is used to create a new {@link Converter} for specified
	 * {@link PrimitiveType}.
	 */
	private abstract static class PrimitiveConverterCreator extends ConverterCreator {
		@Override
		protected void validateType(Type type) {
			Preconditions.checkArgument(type.isPrimitive());
			validatePrimitiveType(type.asPrimitiveType());
		}

		protected abstract void validatePrimitiveType(PrimitiveType type);
	}

	private static class StringConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addBinary(Binary value) {
					fieldSetter.set(new String(value.getBytes()));
				}
			};
		}
	}

	private static class BooleanConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BOOLEAN);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addBoolean(boolean value) {
					fieldSetter.set(value);
				}
			};
		}
	}

	private static class ByteConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32
					&& type.getOriginalType() == OriginalType.INT_8);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addInt(int value) {
					fieldSetter.set((byte) value);
				}
			};
		}
	}

	private static class ShortConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32
					&& type.getOriginalType() == OriginalType.INT_16);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addInt(int value) {
					fieldSetter.set((short) value);
				}
			};
		}
	}

	private static class IntConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32 &&
					(type.getOriginalType() == null || type.getOriginalType() == OriginalType.INT_32));
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addInt(int value) {
					fieldSetter.set(value);
				}
			};
		}
	}

	private static class LongConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64 &&
					(type.getOriginalType() == null || type.getOriginalType() == OriginalType.INT_64));
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addLong(long value) {
					fieldSetter.set(value);
				}
			};
		}
	}

	private static class FloatConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.FLOAT);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addFloat(float value) {
					fieldSetter.set(value);
				}
			};
		}
	}

	private static class DoubleConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.DOUBLE);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addDouble(double value) {
					fieldSetter.set(value);
				}
			};
		}
	}

	private static class BigDecConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32 ||
					type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64 ||
					type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY);
			Preconditions.checkArgument(type.getOriginalType() == OriginalType.DECIMAL);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addInt(int value) {
					fieldSetter.set(new BigDecimal(value));
				}

				@Override
				public void addLong(long value) {
					fieldSetter.set(new BigDecimal(value));
				}

				@Override
				public void addBinary(Binary value) {
					fieldSetter.set(new BigDecimal(new BigInteger(value.getBytes())));
				}
			};
		}
	}

	private static class ByteArrayConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.BINARY &&
					(type.getOriginalType() == null
							|| type.getOriginalType() == OriginalType.BSON
							|| type.getOriginalType() == OriginalType.UTF8));
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {

				@Override
				public void addBinary(Binary value) {
					fieldSetter.set(value.getBytes());
				}
			};
		}
	}

	private static class SqlDateConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32
					&& type.getOriginalType() == OriginalType.DATE);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addInt(int value) {
					fieldSetter.set(DateConverter.INSTANCE.toExternal(value));
				}
			};
		}
	}

	private static class SqlTimeConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT32
					&& type.getOriginalType() == OriginalType.TIME_MILLIS);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addInt(int value) {
					fieldSetter.set(TimeConverter.INSTANCE.toExternal(value));
				}
			};
		}
	}

	private static class SqlTimestampConverterCreator extends PrimitiveConverterCreator {

		@Override
		protected void validatePrimitiveType(PrimitiveType type) {
			Preconditions.checkArgument(type.getPrimitiveTypeName() == PrimitiveType.PrimitiveTypeName.INT64
					&& type.getOriginalType() == OriginalType.TIMESTAMP_MILLIS);
		}

		@Override
		protected Converter newConverter(RowFieldSetter fieldSetter) {
			return new ParquetPrimitiveConverter(fieldSetter) {
				@Override
				public void addLong(long value) {
					fieldSetter.set(TimestampConverter.INSTANCE.toExternal(value));
				}
			};
		}
	}

}
