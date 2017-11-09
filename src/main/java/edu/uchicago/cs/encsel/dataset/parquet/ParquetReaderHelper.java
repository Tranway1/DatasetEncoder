package edu.uchicago.cs.encsel.dataset.parquet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.DataPage;
import org.apache.parquet.column.page.DictionaryPage;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.column.page.PageReader;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.util.HiddenFileFilter;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;

public class ParquetReaderHelper {
    public static void read(URI file) throws IOException {
        Configuration conf = new Configuration();
        Path path = new Path(file);
        FileSystem fs = path.getFileSystem(conf);
        List<FileStatus> statuses = Arrays.asList(fs.listStatus(path, HiddenFileFilter.INSTANCE));
        List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);
        if (footers.isEmpty()) {
            return;
        }
        for (Footer footer : footers) {
            ParquetFileReader fileReader = ParquetFileReader.open(conf, footer.getFile(), footer.getParquetMetadata());
            PageReadStore rowGroup = null;
            int blockCounter = 0;
            List<ColumnDescriptor> cols = footer.getParquetMetadata().getFileMetaData().getSchema().getColumns();
            while ((rowGroup = fileReader.readNextRowGroup()) != null) {
                BlockMetaData blockMeta = footer.getParquetMetadata().getBlocks().get(blockCounter);
                // Read each column
                for (ColumnChunkMetaData ccMeta : blockMeta.getColumns()) {
                    for (ColumnDescriptor cd : cols) {
                        PageReader pageReader = rowGroup.getPageReader(cd);
                        ColumnReader colReader = new ColumnReaderImpl(cd,pageReader,)
                    }
                }


                blockCounter++;
            }
        }
    }
}