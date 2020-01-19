package com.jaslou.table.api.stream

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.descriptors.{Csv, FileSystem, OldCsv, Schema}
import org.apache.flink.types.Row
import org.apache.flink.api.scala._
import org.apache.flink.table.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tEnv = StreamTableEnvironment.create(env)

    val path = this.getClass.getClassLoader.getResource("words.txt").getPath
    val input = env.fromElements(WC("hello", 1), WC("scala", 1),  WC("flink", 1), WC("flink", 1))

    // 1. register a table from filesystem
    tEnv.connect(new FileSystem().path(path))
//      .withFormat(new Csv().schema(new Row("frequency")))
      .withSchema(new Schema().field("frequency", Types.STRING))
      .inAppendMode()
      .registerTableSource("flieSource")

    val resultTable = tEnv.scan("flieSource")
      .groupBy('frequency)
      .select('frequency, 'frequency.count as 'cnt)

    // conver to DataStream by toRetractStream
    implicit val tpe: TypeInformation[Row] = Types.ROW(Types.STRING, Types.LONG) // tpe is automatically
    resultTable.toRetractStream[Row].print()

    // 2. convert to Table from DataStream
    val tableA = input.toTable(tEnv)
    val resultTableA = tableA.groupBy('word).select('word, 'frequency.count as 'frequency)
    val resultStreamA1 = resultTableA.toRetractStream[WC].print()
    val resultStreamA2 = tEnv.toAppendStream[WC](tableA).print()

    // 3. registerDataStream
    tEnv.registerDataStream("testB", input)
    val tableB = tEnv.scan("testB")
    val resultB = tableB.groupBy('word).select('word, 'frequency.sum as 'frequency)
    val dataStreamB = resultB.toRetractStream[WC]
    dataStreamB.print()

    // 4. convert to Table from fromDataStream by tEnv
    val tableC = tEnv.fromDataStream(input, 'word, 'frequency)
    val resultC = tableC.groupBy('word).select('word, 'frequency.sum as 'frequency)
    val dataStreamC = resultC.toRetractStream[WC]
    dataStreamC.print()

    env.execute()

  }

  case class WC(word: String, frequency: Long)
}
