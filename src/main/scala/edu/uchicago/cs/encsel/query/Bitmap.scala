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

package edu.uchicago.cs.encsel.query

class Bitmap(val length:Long) {

  val data = new Array[Long](Math.ceil(length.toDouble/64).toInt)


  def set(index:Long, value:Boolean) ={

    val li = (index/64).toInt;
    val offset = index % 64;

    data.update(li, data(li)| (1L<<offset));
  }

  def test(index:Long) = {
    val li = (index / 64).toInt
    val offset = index % 64
    (data(li) & (1L << offset))!=0
  }
}
