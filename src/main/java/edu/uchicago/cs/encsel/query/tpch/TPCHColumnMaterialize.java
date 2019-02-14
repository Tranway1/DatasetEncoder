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

package edu.uchicago.cs.encsel.query.tpch;

import edu.uchicago.cs.encsel.parquet.EncReaderProcessor;
import edu.uchicago.cs.encsel.query.MemBufferPrimitiveConverter;
import edu.uchicago.cs.encsel.query.NonePrimitiveConverter;
import org.apache.parquet.VersionParser;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ColumnReader;
import org.apache.parquet.column.impl.ColumnReaderImpl;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.io.File;

public class TPCHColumnMaterialize {

    public static void main(String[] args) throws Exception {
        final int numThread = Integer.valueOf(args[1]);
        new TPCHWorker(new EncReaderProcessor() {
            @Override
            public int expectNumThread() {
                return numThread;
            }

            @Override
            public void processRowGroup(VersionParser.ParsedVersion version,
                                        BlockMetaData meta, PageReadStore rowGroup) {
                for (ColumnDescriptor cd : schema.getColumns()) {
                    ColumnReader cr = new ColumnReaderImpl(cd, rowGroup.getPageReader(cd),
                            new NonePrimitiveConverter(), version);
                    for (int i = 0; i < rowGroup.getRowCount(); i++) {
                        if (cr.getCurrentDefinitionLevel() == cr.getDescriptor().getMaxDefinitionLevel()) {
                            cr.writeCurrentValueToConverter();
                        } else {
                            cr.skip();
                        }
                        cr.consume();
                    }
                }
            }
        }, TPCHSchema.lineitemSchema()).work(args[0]);
    }
}
