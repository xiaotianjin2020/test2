package com.atguigu.test4

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author shkstart 
 * @create 2020-09-27 14:59 
 */
object require01_top10Category_method4 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    //第一步 转换map格式
    val rdd1: RDD[UserVisitAction] = lineRDD.map(
      line => {
        val datas: Array[String] = line.split("_")
        //把上面的属性封装到样例类中去
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
    //第二步  把种类点击ID、订单下单ID、支付订单ID 一对多打散  并判断
    val rdd2: RDD[(String, CategoryCountInfo)] = rdd1.flatMap {
      case (act: UserVisitAction) => {
        if (act.click_category_id != -1) {
          List((act.click_category_id.toString, CategoryCountInfo(act.click_category_id.toString, 1, 0, 0)))
        } else if (act.order_category_ids != "null") {
          val list: ListBuffer[(String, CategoryCountInfo)] = new ListBuffer[(String, CategoryCountInfo)]
          val ids: Array[String] = act.order_category_ids.split(",")
          for (id <- ids) {
            list.append((id, CategoryCountInfo(id, 0, 1, 0)))
          }
          list
        }
        else if (act.pay_category_ids != "null") {
          val list: ListBuffer[(String, CategoryCountInfo)] = new ListBuffer[(String, CategoryCountInfo)]
          val ids: Array[String] = act.pay_category_ids.split(",")
          for (id <- ids) {
            list.append((id, CategoryCountInfo(id, 0, 0, 1)))
          }
          list
        } else {
          Nil
        }
      }
    }
    //第三步 reduceByKey  是元组 (key，value) 形式的  通过key分组
    val rdd3: RDD[CategoryCountInfo] = rdd2.reduceByKey(
      (info1, info2) => {
 /*       info1.clickCount += info2.clickCount
        info1.orderCount += info2.orderCount
        info1.payCount += info2.payCount*/
        info1.orderCount = info1.orderCount + info2.orderCount
        info1.clickCount = info1.clickCount + info2.clickCount
        info1.payCount = info1.payCount + info2.payCount
        info1
      }
    ).map(_._2)
    rdd3
   //第四步 用map转换成categoryCountInfo 格式
  val rdd4: RDD[CategoryCountInfo] = rdd3.sortBy(info =>(info.clickCount,info.orderCount,info.payCount),false)
    //第五步 取前10
    val rdd5: Array[CategoryCountInfo] = rdd4.take(10)
    rdd5.foreach(println)
    //4.关闭连接
    sc.stop()
  }
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
// 输出结果表
case class CategoryCountInfo(categoryId: String, //品类id
                            var clickCount: Long, //点击次数
                            var orderCount: Long, //订单次数
                           var payCount: Long) //支付次数