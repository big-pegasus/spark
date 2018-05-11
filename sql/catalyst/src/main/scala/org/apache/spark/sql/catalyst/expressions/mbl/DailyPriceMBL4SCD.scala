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

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Expression, If, Literal}
import org.apache.spark.sql.types._


case class DailyPriceMBL4SCD(children: Seq[Expression]) extends DeclarativeAggregate {

  private lazy val points = AttributeReference("points", ArrayType(IntegerType), nullable = false)()
  private lazy val dayStartTime =
    AttributeReference("day_start_time", IntegerType, nullable = false)()
  private lazy val stepInSeconds: Int = children.head.eval().asInstanceOf[Int]
  private lazy val durationInSeconds: Int = children(1).eval().asInstanceOf[Int]
  private lazy val numRows: Int = 86400 / stepInSeconds
  private lazy val numPoints: Int = numRows * 3
  private lazy val arraySize = numPoints + 2
  private lazy val pointSpan: Int =
    if (durationInSeconds <= stepInSeconds) 1 else durationInSeconds / stepInSeconds

  override val initialValues: Seq[Expression] = Seq(
    GenerateArray(Literal(arraySize),
      UDFUtils.makeIter("mbl_initalValues"), Literal(-1, IntegerType)),
    Literal(-1, IntegerType)
  )

  override val updateExpressions: Seq[Expression] = {
    val i = UDFUtils.makeIter("mbl_updateExpressions")

    val startTimeLong = children(2) / 1000
    val endTimeLong = children(3) / 1000
    val compPrice = children(4)
    val mtPrice = children(5)

    val diff = (endTimeLong % 86400 / stepInSeconds) - (startTimeLong % 86400 / stepInSeconds) + 1
//    diff = If(diff > pointSpan, diff, pointSpan)

    Seq(
      DoSeq(
        ForStep4Expr(Cast(diff, IntegerType), 1, i, {
          val pointIndex0 =
            Cast((((startTimeLong + 28800) % 86400) / stepInSeconds + i) * 3, IntegerType)
          val pointIndex1 = pointIndex0 + 1
          val pointIndex2 = pointIndex0 + 2

          val prevWeight = GetArrayItemWithSize(arraySize, points, pointIndex0)
          val prevCompPrice = GetArrayItemWithSize(arraySize, points, pointIndex1)
          val prevMtPrice = GetArrayItemWithSize(arraySize, points, pointIndex2)

          val weight = children(6)

          If(pointIndex0 < numPoints &&
            (prevWeight < 0 || compPrice < prevCompPrice ||
              compPrice === prevCompPrice && mtPrice < prevMtPrice),
            Then(
              SetArrayItem(points, pointIndex0, weight),
              SetArrayItem(points, pointIndex1, compPrice),
              SetArrayItem(points, pointIndex2, mtPrice),
              points),
            Else(
              points
            ))
        }),
        points),
      If(dayStartTime < 0,
        Cast(startTimeLong - (startTimeLong + 28800) % 86400, IntegerType), dayStartTime)
    )
  }

  override val mergeExpressions: Seq[Expression] = {
    val i = UDFUtils.makeIter("mbl_mergeExpressions")
    Seq(
      DoSeq(
        ForStep(numPoints, 3, i, {
          val leftWeight = GetArrayItemWithSize(arraySize, points.left, i)
          val leftCompPrice = GetArrayItemWithSize(arraySize, points.left, i + 1)
          val leftMtPrice = GetArrayItemWithSize(arraySize, points.left, i + 2)

          val rightWeight = GetArrayItemWithSize(arraySize, points.right, i)
          val rightCompPrice = GetArrayItemWithSize(arraySize, points.right, i + 1)
          val rightMtPrice = GetArrayItemWithSize(arraySize, points.right, i + 2)

          If(leftCompPrice < rightCompPrice ||
            (leftCompPrice === rightCompPrice && leftMtPrice < rightMtPrice),
            Then(
              SetArrayItem(points, i, leftWeight),
              SetArrayItem(points, i + 1, leftCompPrice),
              SetArrayItem(points, i + 2, leftMtPrice)),
            Else(
              SetArrayItem(points, i, rightWeight),
              SetArrayItem(points, i + 1, rightCompPrice),
              SetArrayItem(points, i + 2, rightMtPrice))
          )
        }),
        points),
      dayStartTime
    )
  }

  override val evaluateExpression: Expression = {
    DoSeq(
      // 把startTime和step添加到数据最后两个元素中，为sum时候使用
      SetArrayItem(points, Literal(numPoints), dayStartTime),
      SetArrayItem(points, Literal(numPoints + 1), Literal(stepInSeconds)),
      points
    )
  }

  override def aggBufferAttributes: Seq[AttributeReference] = points :: dayStartTime :: Nil

  override def inputTypes: Seq[AbstractDataType] = Seq(
    IntegerType, // 1: stepInSeconds
    IntegerType, // 2: durationInSeconds
    LongType, // 3: crawTime-->startTimeLong
    LongType, // 4: crawTime-->endTimeLong
    IntegerType, // 5: compPrice
    IntegerType, // 6: mtPrice
    IntegerType // 7: weight
  )

  override def nullable: Boolean = false

  override def dataType: DataType = ArrayType(IntegerType)
}
