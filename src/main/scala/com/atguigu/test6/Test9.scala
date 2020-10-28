package com.atguigu.test6

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.util.AccumulatorV2
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.{immutable, mutable}

/**
 * @author shkstart 
 * @create 2020-09-28 19:04 
 */
object Test9 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //第一步读取数据
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    //第二步转换  把数据封装到样例类中去
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
    //第三步创建累加器
    val acc: CategoryCountAccumulator = new CategoryCountAccumulator()
    //注册累加器
    sc.register(acc,"sum")
    //累加器添加数据
    rdd1.foreach(action=>acc.add(action))
    //获取累加器的值
    val accMap: mutable.Map[(String, String), Long] = acc.value
    //转换数据格式
    val group: Map[String, mutable.Map[(String, String), Long]] = accMap.groupBy(_._1._1)
   //输出结果
    val infoes: immutable.Iterable[CategoryCountInfo] = group.map {
      case (id, map) => {
        val click: Long = map.getOrElse((id, "click"), 0L)
        val order: Long = map.getOrElse((id, "order"), 0L)
        val pay: Long = map.getOrElse((id, "pay"), 0L)
        CategoryCountInfo(id, click, order, pay)
      }
    }
    //排序  scala排序
    val sort_top10: List[CategoryCountInfo] = infoes.toList.sortWith(
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
    ).take(10)
    //********************需求二********************************
    //获取前10的种类ID
    val ids: List[String] = sort_top10.map(_.categoryId)
    //将原始数据进行过滤
    //将ids声明为广播变量
    val broadids: Broadcast[List[String]] = sc.broadcast(ids)
    val rdd2: RDD[UserVisitAction] = rdd1.filter(
      action => {
        if (action.click_category_id != -1) {
          broadids.value.contains(action.click_category_id.toString)
        } else {
          false
        }
      })
    //对 上述rdd2 过滤后数据 转换成自己需要的数据格式
    val rdd3: RDD[(String, Int)] = rdd2.map(
      action => {
        (action.click_category_id + "--" + action.session_id, 1)
      }
    )
    //对action.click_category_id + "--" + action.session_id 聚合统计
    val rdd4: RDD[(String, Int)] = rdd3.reduceByKey(_+_)
    //转换数据结构  (id,(session,sum))
    val rdd5: RDD[(String, (String, Int))] = rdd4.map {
      case (key, sum) => {
        val key1: Array[String] = key.split("--")
        (key1(0), (key1(1), sum))
      }
    }
   //在干key1(0) id进行分组
    val rdd6: RDD[(String, Iterable[(String, Int)])] = rdd5.groupByKey()
    //按sum排序
    val rdd7: RDD[(String, List[(String, Int)])] = rdd6.mapValues(
      datas => {
        datas.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        )
      }
    )
    rdd7.take(10).foreach(println)
    //4.关闭连接
    sc.stop()
  }
}

class CategoryCountAccumulator extends AccumulatorV2[UserVisitAction,mutable.Map[(String,String),Long]]{
  val map=mutable.Map[(String,String),Long]()
  override def isZero: Boolean = map.isEmpty

  override def copy(): AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]] = {
    new CategoryCountAccumulator
  }

  override def reset(): Unit = map.clear()

  override def add(acc: UserVisitAction): Unit = {
    if(acc.click_category_id != -1){
      //CategoryCountInfo(acc.click_category_id.toString,1,0,0)
      val key=(acc.click_category_id.toString,"click")
      map(key)=map.getOrElse(key,0L)+1L
    }else if (acc.order_category_ids != "null"){
      val ids: Array[String] = acc.order_category_ids.split(",")
      for (id <- ids) {
        val key=(id,"order")
        map(key)=map.getOrElse(key,0L)+1L
      }
    }else if (acc.pay_category_ids != "null"){
      val ids: Array[String] = acc.pay_category_ids.split(",")
      for (id <- ids) {
        val key=(id,"pay")
        map(key)=map.getOrElse(key,0l)+1L
      }
    }
  }

  override def merge(other: AccumulatorV2[UserVisitAction, mutable.Map[(String, String), Long]]): Unit = {
    other.value.foreach{
      case(category,count)=>{
        map(category)=map.getOrElse(category,0L)+count
      }
    }
  }

  override def value: mutable.Map[(String, String), Long] = map
}

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