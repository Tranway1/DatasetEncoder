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
package edu.uchicago.cs.encsel

import java.io.File
import java.net.URI
import java.util.Properties
import java.util.zip.ZipFile

import edu.uchicago.cs.encsel.util.ZipUtils
import org.slf4j.LoggerFactory

object Config {

  var collectorThreadCount = 10

  var columnReaderEnableCheck = true
  var columnReaderErrorLimit = 100
  var columnFolder = "./columns"
  val tempFolder = "./temp"

  var distJmsHost = "vm://localhost"

  var predictIntModelPath = "./int_model"
  var predictStringModelPath = "./string_model"
  var predictNumFeature = 19


  val logger = LoggerFactory.getLogger(getClass)

  load()
  extract()


  def load(): Unit = {
    try {
      val p = new Properties()
      p.load(Thread.currentThread().getContextClassLoader.getResourceAsStream("config.properties"))

      collectorThreadCount = Integer.parseInt(p.getProperty("collector.threadCount"))
      columnReaderEnableCheck = "true".equals(p.getProperty("column.readerEnableCheck"))
      columnReaderErrorLimit = Integer.parseInt(p.getProperty("column.readerErrorLimit"))
      columnFolder = p.getProperty("column.folder")

      distJmsHost = p.getProperty("dist.jms.host")

      predictIntModelPath = p.getProperty("predict.model.int.path")
      predictStringModelPath = p.getProperty("predict.model.string.path")
      predictNumFeature = Integer.parseInt(p.getProperty("predict.numFeature"))
    } catch {
      case e: Exception => {
        logger.warn("Failed to load configuration", e)
      }
    }
  }

  // Extract Resources from jar to current folder
  def extract(): Unit = {
    try {
      val jarURI = Config.getClass.getResource("/edu/uchicago/cs/encsel/Config.class").toURI
      if (jarURI.getScheme == "jar") {
        logger.info("Extracting Resources...")
        val jarPath = new URI(jarURI.getSchemeSpecificPart.replaceFirst("!.*$", ""))

        val zipFile = new ZipFile(new File(jarPath))

        ZipUtils.extractFolder(zipFile, "int_model", new File("int_model"))
        ZipUtils.extractFolder(zipFile, "string_model", new File("string_model"))
        logger.info("Resource extracted")
      } else {
        logger.info("Resource not packed, no extracting needed")
      }
    } catch {
      case e: Exception => {
        logger.error("Failed to load resources", e)
        System.exit(1)
      }
    }
  }
}

