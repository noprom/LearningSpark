package learningspark.rdd

import org.apache.spark.SparkContext

/**
 * Created by noprom on 1/17/16.
 */
object Map {

  /**
   * 主函数
   * @param args
   */
  def main (args: Array[String]) {
    var sc = new SparkContext("local[2]", "Basic RDD: Map")

    // Scala squaring the values in an RDD
    val input = sc.parallelize(List(1, 2, 3, 4))
    val result = input.map(x => x * x)
    println(result.collect().mkString(","))

    Thread.sleep(10 * 60 * 1000) // 挂住 10 分钟; 这时可以去看 SparkUI: http://localhost:4040
  }
}