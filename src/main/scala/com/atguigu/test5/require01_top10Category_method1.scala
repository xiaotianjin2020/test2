package com.atguigu.test5

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
 * @author shkstart 
 * @create 2020-09-27 22:41 
 */
object require01_top10Category_method1 {
  def main(args: Array[String]): Unit = {
    //通过样例类实现  需要啥就留啥 缺啥就补啥  种类-点击次数-订单次数-支付次数
    //其中种类ID 就是 点击产品类ID  订单产品类ID  支付产品类ID
    //第一步新建样例类  样例类属性与值进行绑定
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    val rdd1: RDD[UserVisitAction] = lineRDD.map(
      //map是一行对一行  输入 =>输出
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
    //第二步  需要把点击ID产品、订单ID产品、支付ID产品 打散，一个一个进行遍历，继续次数
    val rdd2: RDD[CategoryCountInfo] = rdd1.flatMap {
      case acc: UserVisitAction => {
        if (acc.click_category_id != -1) {
          List(CategoryCountInfo(acc.click_category_id.toString, 1, 0, 0))
        } else if (acc.order_category_ids != "null") {
          val ids: Array[String] = acc.order_category_ids.split(",")
          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 1, 0))
          }
          list
        } else if (acc.pay_category_ids != "null") {
          val ids: Array[String] = acc.order_category_ids.split(",")
          val list: ListBuffer[CategoryCountInfo] = new ListBuffer[CategoryCountInfo]
          for (id <- ids) {
            list.append(CategoryCountInfo(id, 0, 0, 1))
          }
          list
        } else {
          Nil
        }
      }
    }
    //第三步通过groupby  其中的 种类ID 分组 ,返回值是  种类id  跟迭代器  元组类型
    val rdd3: RDD[(String, Iterable[CategoryCountInfo])] = rdd2.groupBy(CategoryCountInfo => CategoryCountInfo.categoryId)
    //第四步取values值
    val rdd4: RDD[CategoryCountInfo] = rdd3.mapValues(datas => datas.reduce({
      (info1, info2) => {
        info1.clickCount += info2.clickCount
        info1.orderCount += info2.orderCount
        info1.payCount += info2.payCount
        info1
      }
    })).map(_._2)
    //第五步取前10
    val rdd5: RDD[CategoryCountInfo] = rdd4.sortBy(info=>(info.clickCount,info.orderCount,info.payCount),false)
    rdd5.take(10).foreach(println)
    //4.关闭连接
    sc.stop()
  }

}

//新建样例类  一个是初始化样例类  一个是存储的结果样例类
//初始化数据的样例类
case class UserVisitAction(
                            date: String, //用户点击行为的日期
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

case class CategoryCountInfo(var categoryId: String, //品类id
                             var clickCount: Long, //点击次数
                             var orderCount: Long, //订单次数
                             var payCount: Long) //支付次数

