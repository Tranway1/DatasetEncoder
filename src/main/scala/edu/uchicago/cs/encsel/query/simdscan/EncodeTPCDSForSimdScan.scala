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

package edu.uchicago.cs.encsel.query.simdscan

import edu.uchicago.cs.encsel.adapter.parquet.{EncContext, ParquetWriterHelper}
import edu.uchicago.cs.encsel.query.tpch.EncodeTPCHColumn.args
import edu.uchicago.cs.encsel.query.tpch.TPCHSchema
import org.apache.parquet.column.Encoding
import org.apache.parquet.hadoop.metadata.CompressionCodecName

import java.io.File

object EncodeTPCDSForSimdScan extends App {
  val folder = "/data/dataset/"
  //  val folder = args(0)
  val inputsuffix = ".dat"
  val outputsuffix = "_snappy_op.parquet"
//  val schema = TPCHSchema.catalog_salesSchema;
  val schema = TPCHSchema.catalog_salesSchema;
  schema.getColumns.forEach(cd => {
    EncContext.encoding.get().put(cd.toString, Encoding.RLE_DICTIONARY)
  })
  ParquetWriterHelper.write(
    new File("%s%s%s".format(folder, schema.getName, inputsuffix)).toURI,
    schema,
    new File("%s%s%s".format(folder, schema.getName, outputsuffix)).toURI,
    "\\|", false, CompressionCodecName.SNAPPY)
}
