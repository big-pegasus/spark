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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.DeclarativeAggregate
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

case class ReasonAnalysisSumMBL(children: Seq[Expression]) extends DeclarativeAggregate {


  override def inputTypes: Seq[DataType] = Seq(ArrayType(IntegerType),
    StringType, StringType, StringType)

  private def mblPointType: DataType = new StructType()
    .add("ts", IntegerType) // unix timestamp in seconds
    .add("meet", IntegerType)
    .add("beat", IntegerType)
    .add("lose", IntegerType)
    .add("reason", IntegerType)

  override def dataType: DataType = ArrayType(mblPointType)

  override def nullable: Boolean = false


  private lazy val meetReasons: UTF8String = children(1).eval().asInstanceOf[UTF8String]
  private lazy val beatReasons: UTF8String = children(2).eval().asInstanceOf[UTF8String]
  private lazy val loseReasons: UTF8String = children(3).eval().asInstanceOf[UTF8String]


  private lazy val meetReasonLength: Int = (meetReasons.toString.length() + 1) / 2
  private lazy val beatReasonLength: Int = (beatReasons.toString.length() + 1) / 2
  private lazy val loseReasonLength: Int = (loseReasons.toString.length() + 1) / 2

  // length for reason
  private lazy val reasonsLength: Integer = meetReasonLength + beatReasonLength + loseReasonLength -1

  // number of points
  private def numBufferPoints: Integer = 1024 * reasonsLength


  private lazy val sumPoints =
    AttributeReference("sum_points", ArrayType(IntegerType), nullable = false)()

  private lazy val numPoints =
    AttributeReference("num_points", IntegerType, nullable = false)()

  private lazy val startTimeInSeconds =
    AttributeReference("start_time_in_seconds", IntegerType, nullable = false)()

  private lazy val stepInSeconds =
    AttributeReference("step_in_seconds", IntegerType, nullable = false)()

  private lazy val reasonIndexes =
    AttributeReference("reason_indexes", MapType(IntegerType, IntegerType), nullable = false)()

  private lazy val revertReasonIndexes =
    AttributeReference("revert_reason_indexes",
      MapType(IntegerType, IntegerType), nullable = false)()

  override lazy val aggBufferAttributes: Seq[AttributeReference] =
    sumPoints :: numPoints :: startTimeInSeconds :: stepInSeconds ::
        reasonIndexes :: revertReasonIndexes :: Nil


  override lazy val initialValues: Seq[Expression] = Seq(
    {
      // for result
      val i = UDFUtils.makeIter("ra_sum_mbl_initalValues")
      GenerateArray(Literal(numBufferPoints), i, Literal(0, IntegerType))
    },
    Literal(0),
    Literal(0),
    Literal(0),
    // for reason map(reason index)
    ReasonIndexExpr(reasonIndexes,
      children(1),
      children(2),
      children(3),
      Literal(false)
    ),
    ReasonIndexExpr(revertReasonIndexes,
      children(1),
      children(2),
      children(3),
      Literal(true)
    )
  )

  override lazy val updateExpressions: Seq[Expression] = {
    val i = UDFUtils.makeIter("ra_sum_mbl_updateExpressions")
    val arraySize = GetArraySize(children.head)


    Seq(
      DoSeq(
        ForStep(numBufferPoints , reasonsLength.intValue(), i, {
          val j = (i/Literal(reasonsLength.intValue()))*4

          val weight = GetArrayItem(children.head, j)
          val compValue = GetArrayItem(children.head, j + 1)
          val mtValue = GetArrayItem(children.head, j + 2)
          val reason = GetArrayItem(children.head, j + 3)


          val meet = compValue === mtValue
          val beat = compValue > mtValue
          val lose = compValue < mtValue

          // convert actual reason to encoded reason for mbl separately in case of duplicated
          val encodedReason = If(meet, reason | ((1 & 0xF)<<24),
                                  If(beat, reason | ((2 & 0xF)<<24),
                                    If(lose, reason | ((3 & 0xF)<<24), Literal(-1))))

          val ri = GetMapValue(reasonIndexes, encodedReason)

          val meetCount = GetArrayItem(sumPoints, i + ri)
          val beatCount = GetArrayItem(sumPoints, i + ri)
          val loseCount = GetArrayItem(sumPoints, i + ri)


          If (weight > 0 && ri >= 0,
            If (meet, DoSeq(SetArrayItem(sumPoints, i + ri, meetCount  + weight), sumPoints),
              If (beat, DoSeq(SetArrayItem(sumPoints, i + ri, beatCount + weight), sumPoints),
                If (lose, DoSeq(SetArrayItem(sumPoints, i + ri, loseCount + weight), sumPoints),
                  sumPoints)
              )
            ),
            sumPoints)
        }),
        sumPoints),
      arraySize - 2, // numPoints = 数组长度-2
      GetArrayItem(children.head, arraySize - 2), // startTimeInSeconds = 数组倒数第2个元素
      GetArrayItem(children.head, arraySize - 1)  // stepInSeconds = 数组倒数第1个元素
    )
  }

  override lazy val mergeExpressions: Seq[Expression] = Seq(
    {
      val i = UDFUtils.makeIter("ra_sum_mbl_mergeExpressions")
      DoSeq(
        ForStep(numBufferPoints, reasonsLength, i, {

          val j = UDFUtils.makeIter("ra_sum_mbl_mergeExpressions")

          DoSeq(
            ForStep(reasonsLength, 1, j, {
              val leftValue = GetArrayItem(sumPoints.left, i + j)
              val rightValue = GetArrayItem(sumPoints.right, i + j)
              SetArrayItem(sumPoints, i + j, leftValue + rightValue)
            })
          )
        }),
        sumPoints)
    },

    If(numPoints.left === 0, numPoints.right, numPoints.left), // num_points
    If(startTimeInSeconds.left === 0,
      startTimeInSeconds.right, startTimeInSeconds.left), // start_time_in_seconds
    If(stepInSeconds.left === 0, stepInSeconds.right, stepInSeconds.left) // step_in_seconds
  )

  override lazy val evaluateExpression: Expression = {
    val i = UDFUtils.makeIter("ra_sum_mbl_evaluateExpression")

    val reasonIndex = i % reasonsLength.intValue()

    val beMeet = reasonIndex < meetReasonLength
    val beBeat = reasonIndex >= meetReasonLength &&
                 reasonIndex < (meetReasonLength + beatReasonLength)
    val beLose = reasonIndex >= (meetReasonLength + beatReasonLength) &&
                 reasonIndex < (meetReasonLength + beatReasonLength + loseReasonLength)

    val meet = If(beMeet, GetArrayItem(sumPoints, i), Literal(0))
    val beat = If(beBeat, GetArrayItem(sumPoints, i), Literal(0))
    val lose = If(beLose, GetArrayItem(sumPoints, i), Literal(0))
    val encodedReason = GetMapValue(revertReasonIndexes, reasonIndex)
    val reason = encodedReason & 0x0FFF


    GenerateArray((numPoints/4)*reasonsLength.intValue(), i,
      CreateLocalStruct(Seq(
        startTimeInSeconds + (i/Literal(reasonsLength) + 1) * stepInSeconds,
        meet,
        beat,
        lose,
        reason
      ))
    )
  }
}
