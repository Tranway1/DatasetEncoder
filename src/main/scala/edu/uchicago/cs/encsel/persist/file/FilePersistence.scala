/**
 * *****************************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * Contributors:
 *     Hao Jiang - initial API and implementation
 *
 * *****************************************************************************
 */
package edu.uchicago.cs.encsel.persist.file

import java.io.File
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream

import scala.Iterable
import scala.collection.mutable.ArrayBuffer

import edu.uchicago.cs.encsel.persist.Persistence
import edu.uchicago.cs.encsel.column.Column
import edu.uchicago.cs.encsel.persist.Persistence
import java.io.FileNotFoundException

/**
 * A thread safe implementation of <code>Persistence</code> backed by file storage
 */
class FilePersistence extends Persistence {

  var storage = new File("storage.dat")
  var datalist = new scala.collection.mutable.HashSet[Column]()
  datalist ++= load()

  def save(datalist: Iterable[Column]) = {
    this.synchronized {
      this.datalist ++= datalist

      var objwriter = new ObjectOutputStream(new FileOutputStream(storage))
      objwriter.writeObject(this.datalist)
      objwriter.close()
    }
  }

  def clean() = {
    this.synchronized {
      this.datalist.clear()

      var objwriter = new ObjectOutputStream(new FileOutputStream(storage))
      objwriter.writeObject(this.datalist)
      objwriter.close()
    }
  }

  def load(): Iterable[Column] = {
    this.synchronized {
      try {
        var objreader = new ObjectInputStream(new FileInputStream(storage))
        var data = objreader.readObject().asInstanceOf[ArrayBuffer[Column]]
        objreader.close()

        return data.clone()
      } catch {
        case e: FileNotFoundException => {
          return Iterable[Column]()
        }
      }
    }
  }
}