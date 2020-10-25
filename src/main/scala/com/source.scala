package com



import java.sql.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import scala.collection.mutable.ListBuffer


//Define case class
case class Reading(srcid: Long, tgtid: Long, timestamp: Long)

case class Result(srcid: Long, windowEnd: Long, count: Long)

object source {

  def main(args: Array[String]): Unit = {

    //Create Environment
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //Read Data
    val inputPath = "C:\\Users\\10442\\IdeaProjects\\test\\src\\main\\resources\\CollegeMsg.txt"
    val inputStream = env.readTextFile(inputPath)

    //Convert into case class
    val dataStream = inputStream.map(
      data => {
        val arr = data.split(" ")
        Reading(arr(0).toLong, arr(1).toLong, arr(2).toLong)
      }
    ).assignAscendingTimestamps(_.timestamp * 1000L)

    //Set Window
    val aggStream: DataStream[Result] = dataStream
      .keyBy("srcid")
      .timeWindow(Time.days(7))
      .aggregate(new CountAgg(), new WindowResult())

    val resultStream = aggStream
      .keyBy("windowEnd") //Data collection based on windows
      .process(new Top(10))

    //Execute
    resultStream.print()
    env.execute("source test")

  }
}


//For each Data , count+1
class CountAgg() extends AggregateFunction[Reading, Long, Long] {
  override def add(value: Reading, accumulator: Long): Long = accumulator + 1
  override def createAccumulator(): Long = 0L
  override def getResult(accumulator: Long): Long = accumulator
  override def merge(a: Long, b: Long): Long = a + b
}

class WindowResult() extends WindowFunction[Long, Result, Tuple, TimeWindow] {
  override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[Result]): Unit = {
    val srcid = key.asInstanceOf[Tuple1[Long]].f0
    val windowEnd = window.getEnd
    val count = input.iterator.next()
    out.collect(Result(srcid, windowEnd, count))
  }
}


//Define ListState
class Top(topSize: Int) extends KeyedProcessFunction[Tuple, Result, String] {
  private var ResultListState: ListState[Result] = _
  override def open(parameters: Configuration): Unit = {
    ResultListState = getRuntimeContext.getListState(new ListStateDescriptor[Result]("resultcount-list", classOf[Result]))
  }

  //Register a timer that windowEnd+1
  override def processElement(value: Result, ctx: KeyedProcessFunction[Tuple, Result, String]#Context, collector: Collector[String]): Unit = {
    ResultListState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  //Sort now
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, Result, String]#OnTimerContext, out: Collector[String]): Unit = {
    val allCounts: ListBuffer[Result] = ListBuffer()
    val iter = ResultListState.get().iterator()
    while (iter.hasNext) {
      allCounts += iter.next()
    }

    //Clear State
    ResultListState.clear()

    //Sort by count size
    val sortedCounts = allCounts.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
    val result: StringBuilder = new StringBuilder
    result.append("WindowEndTime:").append(new Timestamp(timestamp-1)).append("\n")

    //Iterate over each result in Result
    for (i <- sortedCounts.indices) {
      val currentCount = sortedCounts(i)
      result.append("Nr.").append(i + 1).append(":")
        .append("UserId = ").append(currentCount.srcid).append("\t")
        .append("NumberOfMessages = ").append(currentCount.count).append("\n")
    }
    result.append("======================================\n\n")
    out.collect(result.toString())
  }
}
