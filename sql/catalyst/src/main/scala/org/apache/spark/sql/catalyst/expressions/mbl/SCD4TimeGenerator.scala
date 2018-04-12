/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.catalyst.expressions.mbl

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TypeCheckResult
import org.apache.spark.sql.catalyst.expressions.{Expression, Generator}
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenFallback
import org.apache.spark.sql.types.{LongType, StructField, StructType}

case class SCD4TimeGenerator(children: Seq[Expression])
  extends Expression with Generator with CodegenFallback {

  /**
    * The output element schema.
    */
  override def elementSchema: StructType = StructType(
    Seq(StructField("id", LongType), StructField("round", LongType))
  )

  override def checkInputDataTypes(): TypeCheckResult = {
    if (children.lengthCompare(3) != 0) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName requires 3 arguments.")
    } else if (children.head.dataType != LongType
      || children.apply(1).dataType != LongType
      || children.apply(2).dataType != LongType) {
      TypeCheckResult.TypeCheckFailure(s"$prettyName requires long type.")
    } else {
      TypeCheckResult.TypeCheckSuccess
    }
  }

  /** Should be implemented by child classes to perform specific Generators. */
  override def eval(input: InternalRow): TraversableOnce[InternalRow] = {
    val values = children.map(_.eval(input)).toArray
    var start = values(1).asInstanceOf[Long] / 1000
    var end = values(2).asInstanceOf[Long] / 1000
    start = start - start % 3600
    end = end - end % 3600
    for (i <- start.to(end).by(3600)) yield {
      InternalRow(Array(values(0), i * 1000): _*)
    }
  }
}
