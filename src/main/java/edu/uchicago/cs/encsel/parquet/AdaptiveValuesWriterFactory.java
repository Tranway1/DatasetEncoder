package edu.uchicago.cs.encsel.parquet;

import static org.apache.parquet.column.Encoding.PLAIN;
import static org.apache.parquet.column.Encoding.RLE_DICTIONARY;

import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.Encoding;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.column.values.bitpacking.BitPackingValuesWriter;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForInteger;
import org.apache.parquet.column.values.delta.DeltaBinaryPackingValuesWriterForLong;
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter;
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter;
import org.apache.parquet.column.values.dictionary.DictionaryValuesWriter;
import org.apache.parquet.column.values.factory.ValuesWriterFactory;
import org.apache.parquet.column.values.plain.FixedLenByteArrayPlainValuesWriter;
import org.apache.parquet.column.values.plain.PlainValuesWriter;
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridValuesWriter;

import edu.uchicago.cs.encsel.model.FloatEncoding;
import edu.uchicago.cs.encsel.model.IntEncoding;
import edu.uchicago.cs.encsel.model.StringEncoding;

public class AdaptiveValuesWriterFactory implements ValuesWriterFactory {

	private ParquetProperties parquetProperties;

	public static ThreadLocal<EncodingSetting> encodingSetting = new ThreadLocal<EncodingSetting>() {
		@Override
		protected EncodingSetting initialValue() {
			return new EncodingSetting();
		}
	};

	@Override
	public void initialize(ParquetProperties parquetProperties) {
		this.parquetProperties = parquetProperties;
	}

	private Encoding getEncodingForDataPage() {
		return RLE_DICTIONARY;
	}

	private Encoding getEncodingForDictionaryPage() {
		return PLAIN;
	}

	@Override
	public ValuesWriter newValuesWriter(ColumnDescriptor descriptor) {
		if (parquetProperties.isEnableDictionary()) {
			return dictionaryWriter(descriptor, parquetProperties, getEncodingForDictionaryPage(),
					getEncodingForDataPage());
		}
		switch (descriptor.getType()) {
		case BOOLEAN:
			return getBooleanValuesWriter();
		case FIXED_LEN_BYTE_ARRAY:
			return getFixedLenByteArrayValuesWriter(descriptor);
		case BINARY:
			return getBinaryValuesWriter(descriptor);
		case INT32:
			return getInt32ValuesWriter(descriptor);
		case INT64:
			return getInt64ValuesWriter(descriptor);
		case INT96:
			return getInt96ValuesWriter(descriptor);
		case DOUBLE:
			return getDoubleValuesWriter(descriptor);
		case FLOAT:
			return getFloatValuesWriter(descriptor);
		default:
			throw new IllegalArgumentException("Unknown type " + descriptor.getType());
		}
	}

	private ValuesWriter getBooleanValuesWriter() {
		// no dictionary encoding for boolean
		return new RunLengthBitPackingHybridValuesWriter(1, parquetProperties.getInitialSlabSize(),
				parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
	}

	private ValuesWriter getFixedLenByteArrayValuesWriter(ColumnDescriptor path) {
		return new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(),
				parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
	}

	private ValuesWriter getBinaryValuesWriter(ColumnDescriptor path) {
		EncodingSetting es = encodingSetting.get();
		switch (es.stringEncoding) {
		case DELTA:
			return new DeltaByteArrayWriter(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case DELTAL:
			return new DeltaLengthByteArrayValuesWriter(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case PLAIN:
			return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		default:
			return null;
		}

	}

	private ValuesWriter getInt32ValuesWriter(ColumnDescriptor path) {
		EncodingSetting es = encodingSetting.get();
		switch (es.intEncoding) {
		case RLE:
			return new RunLengthBitPackingHybridValuesWriter(es.intBitLength, parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case BP:
			return new BitPackingValuesWriter(es.intBitLength, parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case DELTABP:
			return new DeltaBinaryPackingValuesWriterForInteger(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case PLAIN:
			return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		default:
			return null;
		}
	}

	private ValuesWriter getInt64ValuesWriter(ColumnDescriptor path) {
		EncodingSetting es = encodingSetting.get();
		switch (es.intEncoding) {
		case RLE:
			return new RunLengthBitPackingHybridValuesWriter(es.intBitLength, parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case BP:
			return new BitPackingValuesWriter(es.intBitLength, parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case DELTABP:
			return new DeltaBinaryPackingValuesWriterForLong(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		case PLAIN:
			return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		default:
			return null;
		}
	}

	private ValuesWriter getInt96ValuesWriter(ColumnDescriptor path) {
		return new FixedLenByteArrayPlainValuesWriter(12, parquetProperties.getInitialSlabSize(),
				parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
	}

	private ValuesWriter getDoubleValuesWriter(ColumnDescriptor path) {
		EncodingSetting es = encodingSetting.get();
		switch (es.floatEncoding) {
		case PLAIN:
			return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		default:
			return null;
		}
	}

	private ValuesWriter getFloatValuesWriter(ColumnDescriptor path) {
		EncodingSetting es = encodingSetting.get();
		switch (es.floatEncoding) {
		case PLAIN:
			return new PlainValuesWriter(parquetProperties.getInitialSlabSize(),
					parquetProperties.getPageSizeThreshold(), parquetProperties.getAllocator());
		default:
			return null;
		}
	}

	static DictionaryValuesWriter dictionaryWriter(ColumnDescriptor path, ParquetProperties properties,
			Encoding dictPageEncoding, Encoding dataPageEncoding) {
		switch (path.getType()) {
		case BOOLEAN:
			throw new IllegalArgumentException("no dictionary encoding for BOOLEAN");
		case BINARY:
			return new DictionaryValuesWriter.PlainBinaryDictionaryValuesWriter(
					properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
					properties.getAllocator());
		case INT32:
			return new DictionaryValuesWriter.PlainIntegerDictionaryValuesWriter(
					properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
					properties.getAllocator());
		case INT64:
			return new DictionaryValuesWriter.PlainLongDictionaryValuesWriter(
					properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
					properties.getAllocator());
		case INT96:
			return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
					properties.getDictionaryPageSizeThreshold(), 12, dataPageEncoding, dictPageEncoding,
					properties.getAllocator());
		case DOUBLE:
			return new DictionaryValuesWriter.PlainDoubleDictionaryValuesWriter(
					properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
					properties.getAllocator());
		case FLOAT:
			return new DictionaryValuesWriter.PlainFloatDictionaryValuesWriter(
					properties.getDictionaryPageSizeThreshold(), dataPageEncoding, dictPageEncoding,
					properties.getAllocator());
		case FIXED_LEN_BYTE_ARRAY:
			return new DictionaryValuesWriter.PlainFixedLenArrayDictionaryValuesWriter(
					properties.getDictionaryPageSizeThreshold(), path.getTypeLength(), dataPageEncoding,
					dictPageEncoding, properties.getAllocator());
		default:
			throw new IllegalArgumentException("Unknown type " + path.getType());
		}
	}

	public static class EncodingSetting {
		public IntEncoding intEncoding = IntEncoding.PLAIN;
		public int intBitLength = 0;
		public StringEncoding stringEncoding = StringEncoding.PLAIN;
		public FloatEncoding floatEncoding = FloatEncoding.PLAIN;
	}

}