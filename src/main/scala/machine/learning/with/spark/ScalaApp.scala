package machine.learning.with.spark

import org.apache.spark.SparkContext

/**
 * Created by noprom on 12/14/15.
 */
object ScalaApp {

  /**
   * 主函数
   * @param args
   */
  def main (args: Array[String]){
    var sc = new SparkContext("local[2]", "First Spark App")
    var data = sc.textFile("resources/UserPurchaseHistory.csv")
      .map(line => line.split(","))
      .map(purchaseRecord => (purchaseRecord(0), purchaseRecord(1), purchaseRecord(2)))

    //count the number of purcharse
    var numPurchases = data.count()
    // let's count how many unique users made purchases
    var uniqueUsers = data.map{case (user, product, price) => user}.distinct().count()
    
  }
}