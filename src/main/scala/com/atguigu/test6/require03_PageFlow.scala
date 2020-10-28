package com.atguigu.test6

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-28 14:30 
 */
object require03_PageFlow {
  //需求是   1-2   访问次数/  1   访问次数
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val lineRDD: RDD[String] = sc.textFile("input/user_visit_action.txt")
    //第一步 将数据封装到样例类中去
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
    //第二步过滤页面ID 页面只在1 2 3 4 5 6 7 页面中的,并转换页面结构(id,1) - 统计每个id 的次数 (id,5)  分母次数求出
    val list = List(1, 2, 3, 4, 5, 6, 7)
    val page_count: Map[Long, Long] = rdd1.filter(action => list.contains(action.page_id)).map(action => (action.page_id, 1L)).reduceByKey(_ + _).collect().toMap
    //第三步  定义要统计页面的跳转率格式 为了下一步过滤用 (1,2)=> 1-2 2-3 3-4 4-5 5-6 采用拉链形式 与自己的尾部进行拉链
    val pageTopage: List[String] = list.zip(list.tail).map {
      case (page1, page2) => {
        page1 + "-" + page2
      }
    }
    //第四步  求分子  同一个会话 session 对原始数据进行分组根据session =>(session,Iter)
    val groupRDD: RDD[(String, Iterable[UserVisitAction])] = rdd1.groupBy(_.session_id)
    //第五步 对迭代器中的日期按 时间排序
    val resultRDD: RDD[List[String]] = groupRDD.mapValues(
      datas => {
        val actions: List[UserVisitAction] = datas.toList.sortWith(
          (left, right) => {
            left.action_time < right.action_time
          }
        )
        val pagelist: List[Long] = actions.map(_.page_id)
        //拉链
        val pageTopageNew: List[(Long, Long)] = pagelist.zip(pagelist.tail)
        //变换结构  (page1,page2)=>page1-page2
        val pageTopage1: List[String] = pageTopageNew.map {
          case (page1, page2) => {
            page1 + "-" + page2
          }
        }
        pageTopage1.filter(data => pageTopage.contains(data))
      }).map(_._2)

    //第六步统计 1-2 的个数  类似wordcount统计
    val pagetopageRDD: RDD[(String, Long)] = resultRDD.flatMap(list => list).map((_, 1L)).reduceByKey(_ + _)
    //第七步计算页面单转化率
   pagetopageRDD.foreach {
      case (pageflow, sum) => {
        val pageid: Array[String] = pageflow.split("-")
        val result_count: Long = page_count.getOrElse(pageid(0).toLong, 1L)
        println(pageflow + "=" + sum.toDouble / result_count)
      }
    }
    //4.关闭连接
    sc.stop()
  }


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
