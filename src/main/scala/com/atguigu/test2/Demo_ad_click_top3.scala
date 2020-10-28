package com.atguigu.test2

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 22:04 
 */
object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    //第一步获取数据  统计出省份 广告点击次数 前三
    val dataRDD: RDD[String] = sc.textFile("input/agent.log")
    //第二步获取省份 广告,并结构转换  string=>(prv-adv,1)
    val prvRDD: RDD[(String, Int)] = dataRDD.map {
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    }
    //prvRDD.collect().foreach(println)
//第三步 对上述结果进行聚合操作  (prv-adv,1)=>(prv-adv,sum)
    val prv_sum: RDD[(String, Int)] = prvRDD.reduceByKey(_+_)

    //prv_sum.collect().foreach(println)
//第四步  把上述结果转换成  (prv-adv,sum)=>(prv,(adv,sum))
    val prv_dataRDD: RDD[(String, (String, Int))] = prv_sum.map {
      case (prv_adv, sum) => {
        val prv: Array[String] = prv_adv.split("-")
        (prv(0), (prv(1), sum))
      }
    }
  //第五步 把上述结果转换成(prv,(adv,sum))=>(prv,Inter[(adv,sum)])
    val resultRDD: RDD[(String, Iterable[(String, Int)])] = prv_dataRDD.groupByKey()
    //第六步 把分组的后的迭代器转换成list 元组 进行排序
    val resultRDD1: RDD[(String, List[(String, Int)])] = resultRDD.mapValues {
      datas => {
        datas.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    }
    resultRDD1.collect().foreach(println)


    //4.关闭连接
    sc.stop()
  }

}
