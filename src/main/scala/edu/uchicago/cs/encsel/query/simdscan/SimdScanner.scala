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
 * under the License,
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 */

package edu.uchicago.cs.encsel.query.simdscan

import java.nio.ByteBuffer

import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.column.impl.ColumnReaderImpl
import org.apache.parquet.column.page.DataPage.Visitor
import org.apache.parquet.column.page.{DataPageV1, DataPageV2, PageReader}

class SimdScanner {
  System.loadLibrary("simdscan")
  @native def scanBitpacked(input: ByteBuffer, offset: Int, size: Int, target: Int, entryWidth: Int): ByteBuffer;
  @native def decodeBitpacked(input: ByteBuffer, offset: Int, size: Int, entryWidth: Int): ByteBuffer;
}


