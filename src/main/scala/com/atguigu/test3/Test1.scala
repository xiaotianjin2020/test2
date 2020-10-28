package com.atguigu.test3

/**
 * @author shkstart 
 * @create 2020-09-25 8:29 
 */
object Test1 {
  def main(args: Array[String]): Unit = {
    /*//1、创建SparkConf并设置APP名称
    val conf = new SparkConf().setAppName("wc").setMaster("local[*]")
    //2、创建SparkContext,该对象是提交SparkApp入口
    val sc = new SparkContext(conf)
    val rdd: RDD[Int] = sc.makeRDD(1 to 4, 3)
    val listRDD = sc.makeRDD(List(List(1, 2), List(3, 4), List(5, 6), List(7)), 2)
    //map()
    val rdd1: RDD[Int] = rdd.map(_ * 2)
    //flatmap()
    val rdd2: RDD[Int] = listRDD.flatMap(list => list)
    //mapPatitions
    val rdd4: RDD[Int] = rdd.mapPartitions(_.map(_ * 2))
    //mapPatitionsWithIndexs  带分区
    rdd.mapPartitionsWithIndex((index, items) => {
      val tuples: Iterator[(Int, Int)] = items.map((index, _))
      //groupbyKEY
      val rdd4 = sc.makeRDD(List(("a", 1), ("b", 5), ("a", 5), ("b", 2)))
      val rdd5: RDD[(String, Iterable[Int])] = rdd4.groupByKey()
      //reduceByKEY  聚合  分区内 combine 分区间聚合
      val rdd6: RDD[(String, Int)] = rdd4.reduceByKey(_+_)
      //distinct去重
      val rdd7: RDD[Int] = rdd.distinct()
      //repartition()重新分区  经过shuffle
      val rdd8: RDD[Int] = rdd.repartition(2)
      //coalesce()合并分区
      val rdd9: RDD[Int] = rdd.coalesce(2)
      //sortBy排序 默认正序
      val rdd10: RDD[Int] = rdd.sortBy(num=>num)
      //partitionBy() 默认分区  hashPartitioner
      //val rdd11: RDD[(String, Int)] = rdd4.partitionBy(new HashPartitioner(2))
      //自定义分区  实现两个方法 一个是分区数  另一个是分区逻辑  数据倾斜

      //aggregateBykEY 分区内 和分区间业务逻辑不一样的情况下

      //foldBykey 分区内和分区间业务逻辑相同
      //combineBykey  把第一个值变为特殊结构
      //mapValues 只对V进行操作，返回值是key  和 迭代器
      //4.关闭连接
       */

  }

}
