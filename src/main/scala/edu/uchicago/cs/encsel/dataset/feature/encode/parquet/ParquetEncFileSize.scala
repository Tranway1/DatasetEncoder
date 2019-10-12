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

package edu.uchicago.cs.encsel.dataset.feature.encode.parquet

import java.io.{File, InputStream}

import edu.uchicago.cs.encsel.adapter.parquet.ParquetWriterHelper
import edu.uchicago.cs.encsel.dataset.column.Column
import edu.uchicago.cs.encsel.dataset.feature.{Feature, FeatureExtractor}
import edu.uchicago.cs.encsel.model._
import org.slf4j.LoggerFactory

/**
  * Encode files using Parquet
  */
object ParquetEncFileSize extends FeatureExtractor {

  val logger = LoggerFactory.getLogger(getClass)

  def featureType = "EncFileSize"

  def supportFilter: Boolean = false

  def extract(col: Column, input: InputStream, prefix: String): Iterable[Feature] = {
    // Ignore filter
    val fType = "%s%s".format(prefix, featureType)
    col.dataType match {
      case DataType.STRING => {
        StringEncoding.values().filter(_.parquetEncoding() != null).map { e => {
          try {
            val f = ParquetWriterHelper.singleColumnString(col.colFile, e)
            new Feature(fType, "%s_file_size".format(e.name()), new File(f).length)
          } catch {
            case ile: IllegalArgumentException => {
              // Unsupported Encoding, ignore
              logger.warn("Failed to encode %s with %s".format(col.colFile, e.toString), ile)
              null
            }
          }
        }
        }.filter(_ != null)
      }
      case DataType.LONG => {
        Iterable()
      }
      case DataType.INTEGER => {
        IntEncoding.values().filter(_.parquetEncoding() != null).map { e => {
          try {
            val f = ParquetWriterHelper.singleColumnInt(col.colFile, e)
            new Feature(fType, "%s_file_size".format(e.name()), new File(f).length)
          } catch {
            case ile: IllegalArgumentException => {
              logger.warn("Failed to encode %s with %s".format(col.colFile, e.toString), ile)
              null
            }
          }
        }
        }.filter(_ != null)
      }
      case DataType.FLOAT => {
        Iterable()
      }
      case DataType.DOUBLE => {
        Iterable()
      }
      case DataType.BOOLEAN =>
        //        val f = ParquetWriterHelper.singleColumnBoolean(col.colFile)
        //        Iterable(new Feature(fType, "PLAIN_file_size", new File(f).length))
        Iterable()
    }
  }

}
