package com.atguigu.test4

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author shkstart 
 * @create 2020-09-27 18:43 
 */
object require01_top10Category_method5 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val rdd1: RDD[UserVisitAction] = lineRDD.map {
      line => {
        val datas: Array[String] = line.split("_")
        UserVisitAction(
          datas(0),
          datas(1).toLong,
          datas(2),
          datas(3).toLong,
          datas(4),
          datas(5),
          datas(6).toLong,
          datas(7).toLong,
          datas(8),
          datas(9),
          datas(10),
          datas(11),
          datas(12).toLong
        )
      }
    }
    //累加器四步曲
    //创建累加器
    val acc: CategoryCountAccumulator = new CategoryCountAccumulator
    //注册累加器
    sc.register(acc, "sum")
    //累加器添加数据
    rdd1.foreach(action => acc.add(action))
    //获取累加器的值
    val accMap: mutable.Map[(String, String), Long] = acc.value
    val group: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)
    val infoes: immutable.Iterable[CategoryCountInfo] = group.map {
      case (id, map) => {
        val click: Long = map.getOrElse((id, "click"), 0L)
        val order: Long = map.getOrElse((id, "order"), 0L)
        val pay: Long = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }
    //将转换后的数据进行排序
    infoes.toList.sortWith({
      (left, Right) => {
        if (left.clickCount > Right.clickCount) {
          true
        } else if (left.clickCount == Right.clickCount) {
          if (left.orderCount > Right.orderCount) {
            true
          } else if (left.orderCount == left.orderCount) {
            left.payCount > Right.payCount

          } else {
            false
          }
        } else {
          false
        }
      }
    }).take(10).foreach(println)


    //4.关闭连接
    sc.stop()
  }

}

//累加器
class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {
  var map: mutable.Map[(String, String), Long] = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = map.clear()

  override def add(action: UserVisitAction): Unit = {
    if (action.click_category_id != -1) {
      val key = (action.click_category_id.toString, "click")
      map(key) = map.getOrElse(key, 0L) + 1L
    } else if (action.order_category_ids != "null") {
      val ids: Array[String] = action.order_category_ids.split(",")
      for (id <- ids) {
        val key = (id, "order")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    } else if (action.pay_category_ids != "null") {
      val ids: Array[String] = action.pay_category_ids.split(",")
      for (id <- ids) {
        val key = (id, "pay")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    }
  }


  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach {
      case (category, count) => {
        map(category) = map.getOrElse(category, 0L) + count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}

