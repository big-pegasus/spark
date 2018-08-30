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

import org.apache.spark.sql.catalyst.expressions.codegen._
import org.apache.spark.sql.catalyst.expressions.{Expression, _}
import org.apache.spark.sql.catalyst.util.ArrayBasedMapData
import org.apache.spark.sql.types._



case class ReasonIndexExpr(reasonIndexes: Expression,
    meetReasonStr: Expression,
    beatReasonStr: Expression,
    loseReasonStr: Expression,
    revert: Expression)
    extends TernaryExpression with NonSQLExpression {


  override def children: Seq[Expression] = reasonIndexes ::
      meetReasonStr :: beatReasonStr :: loseReasonStr :: Nil

  override def deterministic: Boolean = false


  override def dataType: DataType = reasonIndexes.dataType
  override def nullable: Boolean = false

  override def toString: String = {
    s"$reasonIndexes"
  }

  protected override def nullSafeEval(input: Any, ordinal: Any, value: Any): Any = {
    true
  }

  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {


    val mapClass = classOf[ArrayBasedMapData].getName
    val javaType = ctx.javaType(dataType)


    val i = ctx.freshName("i")

    val isRevert = ctx.freshName("isRevert")

    val keyArray = ctx.freshName("keyArray")
    val valueArray = ctx.freshName("valueArray")

    val meetReasons = ctx.freshName("meetReasons")
    val beatReasons = ctx.freshName("beatReasons")
    val loseReasons = ctx.freshName("loseReasons")

    val mapKeys = ctx.freshName("mapKeys")
    val mapValues = ctx.freshName("mapValues")


    ev.copy(code = s"""
           boolean ${isRevert} = ${revert.eval().toString};

           String[] ${meetReasons} = "${meetReasonStr.eval().toString}".split("_");
           String[] ${beatReasons} = "${beatReasonStr.eval().toString}".split("_");
           String[] ${loseReasons} = "${loseReasonStr.eval().toString}".split("_");

           int ${i}=0;

           int [] ${mapKeys} =
                  new int[${meetReasons}.length + ${beatReasons}.length + ${loseReasons}.length];
           int [] ${mapValues} =
                  new int[${meetReasons}.length + ${beatReasons}.length + ${loseReasons}.length];

           for(int j=0;j<${meetReasons}.length;j++){
               ${mapKeys}[${i}]= Integer.valueOf(${meetReasons}[j]).intValue() | ((1 & 0xF)<<24);
               ${mapValues}[${i}]= ${i};
               ${i}++;
           }

           for(int j=0;j<${beatReasons}.length;j++){
               ${mapKeys}[${i}]= Integer.valueOf(${beatReasons}[j]).intValue() | ((2 & 0xF)<<24);
               ${mapValues}[${i}]= ${i};
               ${i}++;
           }

           for(int j=0;j<${loseReasons}.length;j++){
               ${mapKeys}[${i}]= Integer.valueOf(${loseReasons}[j]).intValue() | ((3 & 0xF)<<24);
               ${mapValues}[${i}]= ${i};
               ${i}++;
           }

          org.apache.spark.sql.catalyst.util.GenericArrayData ${keyArray} =
            new org.apache.spark.sql.catalyst.util.GenericArrayData(${mapKeys});
          org.apache.spark.sql.catalyst.util.GenericArrayData ${valueArray} =
            new org.apache.spark.sql.catalyst.util.GenericArrayData(${mapValues});

          ${javaType} ${ev.value} = ${isRevert}? new ${mapClass}(${valueArray},${keyArray})
          :new ${mapClass}(${keyArray}, ${valueArray});
          """ /* for code end here */
      , isNull = "false", ev.value)
  }
}


// case class RevertReasonIndexes(reasonIndexes: Expression,
//    meetReasonStr: Expression,
//    beatReasonStr: Expression,
//    loseReasonStr: Expression)
//    extends TernaryExpression with NonSQLExpression {
//
//  //  override def children: Seq[Expression] = reasonIndexes ::
//  //      Literal(meetReasonStr, StringType) :: Literal(beatReasonStr, StringType) ::
//  //      Literal(loseReasonStr, StringType) :: Nil
//
//
//  override def children: Seq[Expression] = reasonIndexes ::
//      meetReasonStr :: beatReasonStr :: loseReasonStr :: Nil
//
//  override def deterministic: Boolean = false
//
//  //  lazy val childSchema: ArrayType = reasonIndexes.dataType.asInstanceOf[ArrayType]
//
//  override def dataType: DataType = reasonIndexes.dataType
//  override def nullable: Boolean = false
//
//  override def toString: String = {
//    s"$reasonIndexes"
//  }
//
//  protected override def nullSafeEval(input: Any, ordinal: Any, value: Any): Any = {
//    //    input.asInstanceOf[UnsafeArrayData].update(ordinal.asInstanceOf[Integer], value)
//    true
//  }
//
//  override def doGenCode(ctx: CodegenContext, ev: ExprCode): ExprCode = {
//
//
//    val mapClass = classOf[ArrayBasedMapData].getName
//    val javaType = ctx.javaType(dataType)
//
//
//    val i = ctx.freshName("i")
//    val keyArray = ctx.freshName("keyArray")
//    val valueArray = ctx.freshName("valueArray")
//
//    val meetReasons = ctx.freshName("meetReasons")
//    val beatReasons = ctx.freshName("beatReasons")
//    val loseReasons = ctx.freshName("loseReasons")
//
//    val mapKeys = ctx.freshName("mapKeys")
//    val mapValues = ctx.freshName("mapValues")
//
//
//    ev.copy(code = s"""
//
//           String[] ${meetReasons} = "${meetReasonStr.eval().toString}".split("_");
//           String[] ${beatReasons} = "${beatReasonStr.eval().toString}".split("_");
//           String[] ${loseReasons} = "${loseReasonStr.eval().toString}".split("_");
//
//
//           int ${i}=0;
//
//           int [] ${mapKeys} =
//                  new int[${meetReasons}.length + ${beatReasons}.length + ${loseReasons}.length];
//           int [] ${mapValues} =
//                  new int[${meetReasons}.length + ${beatReasons}.length + ${loseReasons}.length];
//
//           for(int j=0;j<${meetReasons}.length;j++){
//               ${mapKeys}[${i}]= Integer.valueOf(${meetReasons}[j]);
//               ${mapValues}[${i}]= ${i};
//               ${i}++;
//           }
//
//           for(int j=0;j<${beatReasons}.length;j++){
//               ${mapKeys}[${i}]= Integer.valueOf(${beatReasons}[j]);
//               ${mapValues}[${i}]= ${i};
//               ${i}++;
//           }
//
//           for(int j=0;j<${loseReasons}.length;j++){
//               ${mapKeys}[${i}]= Integer.valueOf(${loseReasons}[j]);
//               ${mapValues}[${i}]= ${i};
//               ${i}++;
//           }
//
//          org.apache.spark.sql.catalyst.util.GenericArrayData ${keyArray} =
//            new org.apache.spark.sql.catalyst.util.GenericArrayData(${mapValues});
//          org.apache.spark.sql.catalyst.util.GenericArrayData ${valueArray} =
//            new org.apache.spark.sql.catalyst.util.GenericArrayData(${mapKeys});
//          ${javaType} ${ev.value} = new ${mapClass}(${keyArray}, ${valueArray});
//          """ /* for code end here */
//      , isNull = "false", ev.value)
//  }
// }
