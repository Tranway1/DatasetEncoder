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

package edu.uchicago.cs.encsel.ptnmining.rule

import edu.uchicago.cs.encsel.ptnmining._
import edu.uchicago.cs.encsel.ptnmining.parser.{TDouble, TInt, TWord}

/**
  * If the union size is too large, e.g., 90% of total size
  * Use Any to replace big Union
  */
object UseAnyRule {
  // Execute the rule if union size is greater than threshold * data size
  val threshold = 0.3
}

class UseAnyRule extends DataRewriteRule {

  override def condition(ptn: Pattern): Boolean = {
    ptn.isInstanceOf[PUnion] && {
      val union = ptn.asInstanceOf[PUnion]
      union.content.length >= UseAnyRule.threshold * originData.length &&
        union.content.view.forall(_.isInstanceOf[PToken])
    }
  }


  override protected def update(ptn: Pattern): Pattern = {
    val union = ptn.asInstanceOf[PUnion]
    val anyed = union.content.view.map(
      _ match {
        case token: PToken => {
          token.token match {
            case word: TWord => new PWordAny(word.numChar)
            case int: TInt => new PIntAny(int.numChar, int.isHex)
            case double: TDouble => new PDoubleAny(double.numChar)
            case tother => token
          }
        }
        case other => other
      }
    ).groupBy(_.getClass)
    anyed.size match {
      case 1 => {
        happen()
        anyed.map(kv => {
          kv._1 match {
            case wa if wa == classOf[PWordAny] => {
              kv._2.reduce((a, b) => {
                val aw = a.asInstanceOf[PWordAny]
                val bw = b.asInstanceOf[PWordAny]
                new PWordAny(Math.min(aw.minLength, bw.minLength),
                  Math.max(aw.maxLength, bw.maxLength))
              })
            }
            case ia if ia == classOf[PIntAny] => {
              kv._2.reduce((a, b) => {
                val ai = a.asInstanceOf[PIntAny]
                val bi = b.asInstanceOf[PIntAny]
                new PIntAny(Math.min(ai.minLength, bi.minLength),
                  Math.max(ai.maxLength, bi.maxLength),
                  ai.hasHex || bi.hasHex)
              })
            }
            case da if da == classOf[PDoubleAny] => {

              kv._2.reduce((a, b) => {
                val ad = a.asInstanceOf[PDoubleAny]
                val bd = b.asInstanceOf[PDoubleAny]
                new PDoubleAny(Math.min(ad.minLength, bd.minLength),
                  Math.max(ad.maxLength, bd.maxLength))
              })
            }
            case _ => throw new IllegalArgumentException
          }
        }).head
      }
      case _ => ptn
    }
  }
}
