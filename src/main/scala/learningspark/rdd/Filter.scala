package learningspark.rdd

import org.apache.spark.SparkContext

/**
 * Created by noprom on 1/16/16.
 */
object Filter {

  /**
   * 主函数
   * @param args
   */
  def main (args: Array[String]) {
    var sc = new SparkContext("local[2]", "First Spark App")
    var data = sc.textFile("src/main/resources/UserPurchaseHistory.csv")
      .map(line => line.split(","))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

    // let's count the number of purcharse
    var numPurchases = data.count()
    // let's count how many unique users made purchases
    var uniqueUsers = data.map{case (user, product, price) => user}.distinct().count()
    // let's sum up our total revenue
    var totalRevenue = data.map{case (user, product, price) => price.toDouble}.sum()
    // let's find our most popular product
    var productsByPopularity = data.map{case (user, product, price) => (product, 1)}
      .reduceByKey(_ + _).collect().sortBy(-_._2)
    var mostPopular = productsByPopularity(0)

    // print the result
    println("Total purchases :" + totalRevenue)
    println("Most popular product: %s with %d purchases".format(mostPopular._1, mostPopular._2))

    Thread.sleep(10 * 60 * 1000) // 挂住 10 分钟; 这时可以去看 SparkUI: http://localhost:4040
  }
}