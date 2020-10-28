package com.atguigu.test5

import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author shkstart 
 * @create 2020-09-28 8:59 
 */
object Test2 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    //第一步 输入值封装到样例类中去 通过map
    val rdd1: RDD[UserVisitAction] = lineRDD.map(
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
    )
    //第二步 自定义累加器 继承ACCUMV2类  输入泛型UserVisitAction 输出泛型 muable.map((id,"click",10)
    //3.5 创建累加器
    //val acc: CategoryCountAccumulator = new CategoryCountAccumulator()
    val acc: CategoryCountAccumulator = new CategoryCountAccumulator()
    //3.6 注册累加器
    //sc.register(acc, "CategoryCountAccumulator")
    sc.register(acc, "sum")
    //3.7 累加器添加数据
    //rdd1.foreach(action => acc.add(action))
    rdd1.foreach(action => acc.add(action))
    //获取累加器值  ((id,"click"),value)
    //val accMap: mutable.Map[(String, String), Long] = acc.value
    val accMap: mutable.Map[(String, String), Long] = acc.value
    // 3.9 将累加器的值进行结构的转换  ((id,"click"),value)
    //val group: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)
    val group: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)
    //格式转换
    val infoes: immutable.Iterable[CategoryCountInfo] = group.map {
      case (id, mapacc) => {
        val click: Long = mapacc.getOrElse((id, "click"), 0L)
        val order: Long = mapacc.getOrElse((id, "order"), 0L)
        val pay: Long = mapacc.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }
    //排序 迭代器转换list sortwith
    infoes.toList.sortWith(
      (left, right) => {
        if (left.clickCount > right.clickCount) {
          true
        } else if (left.clickCount == right.clickCount) {
          if (left.orderCount > right.orderCount) {
            true

          } else if (left.orderCount == right.orderCount) {
            left.payCount > right.payCount
          } else {
            false
          }
        } else {
          false
        }
      }
    ).take(10).foreach(println)


    //4.关闭连接
    sc.stop()
  }

}

class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] {
  var map = mutable.Map[(String, String), Long]()

  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    new CategoryCountAccumulator()
  }

  override def reset(): Unit = map.clear()

  override def add(acc: UserVisitAction): Unit = {
    if (acc.click_category_id != -1) {
      val key = (acc.click_category_id.toString, "click")
      map(key) = map.getOrElse(key, 0L) + 1L
    } else if (acc.order_category_ids != "null") {
      val ids: Array[String] = acc.order_category_ids.split(",")
      for (id <- ids) {
        val key = (id, "order")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    }
    else if (acc.pay_category_ids != "null") {
      val ids: Array[String] = acc.pay_category_ids.split(",")
      for (id <- ids) {
        val key = (id, "pay")
        map(key) = map.getOrElse(key, 0L) + 1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach {
      case (cate, count) => {
        map(cate) = map.getOrElse(cate, 0L) + count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long]

  = map
}

//用户访问动作表  输入数据
case class UserVisitAction(date: String, //用户点击行为的日期
                           user_id: Long, //用户的ID
                           session_id: String, //Session的ID
                           page_id: Long, //某个页面的ID
                           action_time: String, //动作的时间点
                           search_keyword: String, //用户搜索的关键词
                           click_category_id: Long, //某一个商品品类的ID
                           click_product_id: Long, //某一个商品的ID
                           order_category_ids: String, //一次订单中所有品类的ID集合
                           order_product_ids: String, //一次订单中所有商品的ID集合
                           pay_category_ids: String, //一次支付中所有品类的ID集合
                           pay_product_ids: String, //一次支付中所有商品的ID集合
                           city_id: Long) //城市 id
// 输出结果表  输出数据
case class CategoryCountInfo(categoryId: String, //品类id
                             var clickCount: Long, //点击次数
                             var orderCount: Long, //订单次数
                             var payCount: Long) //支付次数
