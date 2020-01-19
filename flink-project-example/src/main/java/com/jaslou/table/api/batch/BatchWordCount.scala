package com.jaslou.table.api.batch

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.table.api.scala.BatchTableEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

object BatchWordCount {
  def main(args: Array[String]): Unit = {

    // set up execution environment
    val env = ExecutionEnvironment.getExecutionEnvironment
    val tEnv = BatchTableEnvironment.create(env)

    val input = env.fromElements(WC("hello", 1), WC("hello", 1), WC("ciao", 1))
    // 1. toTable， 返回的是Table
    val tableA = input.toTable(tEnv)
    // 2. registerDataSet 返回的是Unit，需要通过tEnv.scan来获取Table
    val table = tEnv.registerDataSet("testB", input)
    val tableB = tEnv.scan("testB")
    // 3. fromDataSet 返回的是Table类型
    val tableC = tEnv.fromDataSet(input)

    val resultA = tableA
      .groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .toDataSet[WC]

    val resultB = tableB.groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .toDataSet[WC]

    val resultC = tableC.groupBy('word)
      .select('word, 'frequency.sum as 'frequency)
      .toDataSet[WC]

    resultA.print()
    resultB.print()
    resultC.print()
  }
  case class WC(word: String, frequency: Long)
}
