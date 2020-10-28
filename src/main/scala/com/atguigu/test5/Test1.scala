package com.atguigu.test5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author shkstart 
 * @create 2020-09-28 8:26 
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    //第一步 数据 转换  并封装到样例类中去
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
    //因为种类订单产品 、种类支付产品 多个ID  需要 1转换多  所以用flatmat  返回的是list集合 用啥拿啥 缺啥补啥
    //种类id-点击次数产品ID-订单次数产品ID-支付产品ID 封装到结果对象里面  因为是reduceBYkey key-value(种类id,(种类id,点击次数、订单次数、支付产品次数)
    val rdd2: RDD[(String, CategoryCountInfo)] = rdd1.flatMap {
      case (acc: UserVisitAction) => {
        if (acc.click_category_id != -1) {
          List((acc.click_category_id.toString, CategoryCountInfo(acc.click_category_id.toString, 1, 0, 0)))
        } else if (acc.order_category_ids != "null") {
          val ids: Array[String] = acc.order_category_ids.split(",")
          val list: ListBuffer[(String, CategoryCountInfo)] = new ListBuffer[(String, CategoryCountInfo)]
          for (id <- ids) {
            list.append((id, CategoryCountInfo(id, 0, 1, 0)))
          }
          list
        } else if (acc.pay_category_ids != "null") {
          val ids: Array[String] = acc.pay_category_ids.split(",")
          val list: ListBuffer[(String, CategoryCountInfo)] = new ListBuffer[(String, CategoryCountInfo)]
          for (id <- ids) {
            list.append((id, CategoryCountInfo(id, 0, 0, 1)))
          }
          list
        } else {
          Nil
        }
      }
    }
    //第三步 根据reduceBykey来聚合
    val rdd3: RDD[CategoryCountInfo] = rdd2.reduceByKey(
      (info1, info2) => {
        info1.clickCount += info2.clickCount
        info1.orderCount += info2.orderCount
        info1.payCount += info2.payCount
        info1
      }
    ).map(_._2)
    rdd3
    //第四步 按照结果点击次数、订单次数、支付次数排序
    val rdd4: RDD[CategoryCountInfo] = rdd3.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false)
   //第五步取前10条
    rdd4.take(10).foreach(println)
    //4.关闭连接
    sc.stop()
  }
}

//样例类   输入数据

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



