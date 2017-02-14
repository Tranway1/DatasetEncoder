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

package edu.uchicago.cs.encsel.wordvec

import edu.uchicago.cs.encsel.parser.csv.CSVParser
import java.io.File
import edu.uchicago.cs.encsel.schema.Schema
import edu.uchicago.cs.encsel.model.DataType
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.HashMap
import scala.collection.mutable.HashSet
import edu.uchicago.cs.encsel.util.WordUtils

object Dict {

  val abbrvMatch = 1.1

  val dictFile = "src/main/word/google_10000.txt"
  //  val dictSchema = new Schema(Array((DataType.INTEGER, "seq"), (DataType.STRING, "word"), (DataType.STRING, "pos")), true)
  val dictSchema = new Schema(Array((DataType.STRING, "word")), false)

  protected var words = new HashMap[Char, ArrayBuffer[(String, Int)]]()
  protected var abbrvs = new HashMap[Char, ArrayBuffer[(String, Int)]]()
  protected var index = new HashMap[String, Int]()
  protected var abbrvIdx = new HashMap[String, String]()
  protected var count = 0

  init()

  def init(): Unit = {
    var parser = new CSVParser()
    var records = parser.parse(new File(dictFile).toURI(), dictSchema)

    records.zipWithIndex.foreach { record =>
      {
        var word = record._1(0)
        index += ((word, count))
        abbrvIdx.getOrElseUpdate(abbrv(word), word)
        words.getOrElseUpdate(word(0), new ArrayBuffer[(String, Int)]()) += ((word, count))
        abbrvs.getOrElseUpdate(word(0), new ArrayBuffer[(String, Int)]()) += ((abbrv(word), count))
        count += 1
      }
    }
    words.foreach(_._2.sortBy(_._1))
  }

  /**
   * This algorithm first match full words, then look for abbreviation
   * Finally look for entries having smallest distance
   *
   * @return pair of (string in dictionary, fidelity)
   */
  def lookup(raw: String): (String, Double) = {
    var input = raw.toLowerCase()

    var candidates = new ArrayBuffer[(String, Double, Double)]

    if (index.contains(input)) {
      // Fully match, 0 distance
      candidates += ((input, 0, freq_penalty(index.getOrElse(input, Int.MaxValue))))
    }
    if (abbrvIdx.contains(input)) {
      // Known abbreviation
      var originWord = abbrvIdx.getOrElse(input, "")
      var originIdx = index.getOrElse(originWord, Int.MaxValue)
      candidates += ((originWord, abbrvMatch - 1, abbrvMatch * freq_penalty(originIdx)))
    }

    if (candidates.isEmpty) {
      var partials = words.getOrElse(input(0), ArrayBuffer.empty[(String, Int)])
        .map(word => (word._1, WordUtils.levDistance2(input, word._1), freq_penalty(word._2)))
      if (!partials.isEmpty) {
        var partial = partials.minBy(t => t._2 + t._3)
        candidates += ((partial._1, partial._2, partial._2 + partial._3))
      }

      var abbrvPartials = abbrvs.getOrElse(input(0), ArrayBuffer.empty[(String, Int)])
        .map(abb =>
          {
            var word = abbrvIdx.getOrElse(abb._1, "")
            var wordPrio = index.getOrElse(word, Int.MaxValue)
            (word, WordUtils.levDistance2(input, abb._1), freq_penalty(wordPrio))
          })
      if (!abbrvPartials.isEmpty) {
        var abbrvp = abbrvPartials.minBy(t => t._2 + t._3)
        candidates += ((abbrvp._1, abbrvp._2, abbrvp._2 + abbrvp._3))
      }
    }
    // Normalize the fidelity
    var candidate = candidates.minBy(_._3)
    var normalized = normalize(candidate._2)
    if (normalized < 0.5)
      // No guess
      (input, 1)
    else
      (candidate._1, normalize(candidate._2))
  }

  def abbrv(input: String) = {
    // Remove any non-leading aeiou and or
    var abbrv = input.replaceAll("""(?!^)or""", "")
    abbrv.replaceAll("""(?!^)[aeiou]""", "")
  }

  protected def distance(input: String, target: String, targetPrio: Int): Double = {
    // TODO Take target priority into account
    var dist = WordUtils.levDistance2(input, target)
    dist
  }

  protected def freq_penalty(idx: Int): Double = idx.toDouble * 5 / count

  protected def normalize(levdist: Double): Double = 1 / (levdist + 1)
}
