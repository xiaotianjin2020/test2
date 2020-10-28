package com.atguigu.createrdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 * @author shkstart 
 * @create 2020-09-24 20:46 
 */
object Demo_ad_click_top3 {
  def main(args: Array[String]): Unit = {
    //1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val dataRDD: RDD[String] = sc.textFile("input/agent.log")
    //第一步：将原始数据进行结构转换  string=>(prv-adv,1)
    val prvAndAdvTooneRDD: RDD[(String, Int)] = dataRDD.map {
      line => {
        val datas: Array[String] = line.split(" ")
        (datas(1) + "-" + datas(4), 1)
      }
    }

    //第二步 将转换的结构的数据进行聚合统计(Prv-adv,1)=>(prv-adv,sum)
    val prvSUMRDD: RDD[(String, Int)] = prvAndAdvTooneRDD.reduceByKey(_+_)
    //第三步 将(prv-adv,sum)=>(prv,(adv,sum))
    val prvs: RDD[(String, (String, Int))] = prvSUMRDD.map {
      case (prv_adv, sum) => {
        val ks: Array[String] = prv_adv.split("-")
        (ks(0), (ks(1), sum))
      }
    }

   //第四步 根据省份对数据进行分组 (prv,(adv,sum))=>(prv,Iterator(adv,sum))
  val groupRDD: RDD[(String, Iterable[(String, Int)])] = prvs.groupByKey()
    groupRDD.collect().foreach(println)
  //第五步 对相同省份的广告进行排序取 前三名
    val resultRdd: RDD[(String, List[(String, Int)])] = groupRDD.mapValues {
      datas => {
        datas.toList.sortWith(
          (left, right) => {
            left._2 > right._2
          }
        ).take(3)
      }
    }
    resultRdd.collect().foreach(println)

    //4.关闭连接
    sc.stop()
  }

}
