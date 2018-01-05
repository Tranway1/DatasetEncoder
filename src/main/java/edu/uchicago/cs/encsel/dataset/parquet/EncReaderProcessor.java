/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 */

package edu.uchicago.cs.encsel.dataset.parquet;

import org.apache.parquet.VersionParser;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.schema.MessageType;

import java.util.Map;

public abstract class EncReaderProcessor implements ReaderProcessor {
    /**
     * Load data from file footer and put that in ThreadLocal for decoding purpose
     * @param footer
     */
    @Override
    public void processFooter(Footer footer) {
        Map<String, String> meta = footer.getParquetMetadata().getFileMetaData().getKeyValueMetaData();
        MessageType schema = footer.getParquetMetadata().getFileMetaData().getSchema();

        schema.getColumns().forEach(col -> {
            if (meta.containsKey(String.format("%s.0", col.toString()))) {
                String data1 = meta.get(String.format("%s.0", col.toString()));
                String data2 = meta.get(String.format("%s.1", col.toString()));
                EncContext.context.get().put(col.toString(), new Object[]{data1, data2});
            }
        });

    }

}
