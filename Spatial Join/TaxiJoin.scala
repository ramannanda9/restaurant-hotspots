package analysis

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object TaxiJoin {
  def main(args: Array[String]) {
    
    if (args.length < 3) {
      args.foreach { println }
      println("Usage: <taxiDataFile> <yelpData> <outputPath>")
      System.exit(0)
    }
    
    val conf = new SparkConf().setAppName("Taxi Join")
    //val conf = new SparkConf().setAppName("KMeans Clustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val taxi = sc.textFile(args(0))
    
    val taxi_night_pick = taxi.map(_.split(","))
      .filter(row => row(1).substring(11,13).toInt <= 3 || row(1)
      .substring(11,13).toInt >= 22).filter(r=> r(5).toDouble >= -74.02 && r(5).toDouble <= -73.9)
      .filter(r => r(6).toDouble >= 40.7 && r(6).toDouble <= 40.85 )

    val taxi_grid_pick = taxi_night_pick.map(r => ( ((r(6).toDouble - 40.7 )/0.002)
       .toInt *120 + ((r(5).toDouble + 74.02 )/0.001).toInt , 1)).reduceByKey(_+_)
        
    val taxi_night_drop = taxi.map(_.split(","))
      .filter(row => row(1).substring(11,13).toInt <= 3 || row(1)
      .substring(11,13).toInt >= 22).filter(r=> r(9).toDouble >= -74.02 && r(9)
      .toDouble <= -73.9).filter(r => r(10).toDouble >= 40.7 && r(10).toDouble <= 40.85 )

    val taxi_grid_drop = taxi_night_pick.map(r => ( ((r(10).toDouble - 40.7 )/0.002)
       .toInt *120 + ((r(9).toDouble + 74.02 )/0.001).toInt , 1)).reduceByKey(_+_)
    
    val taxi_night_tip = taxi.map(_.split(",")).filter(row => row(1).substring(11,13)
        .toInt <= 3 || row(1).substring(11,13).toInt >= 22).filter(r=> r(9)
        .toDouble >= -74.02 && r(9).toDouble <= -73.9).filter(r => r(10).toDouble >= 40.7 && r(10)
        .toDouble <= 40.85 ).filter(r => r(15).toFloat > 0.0 )

    val taxi_grid_tip= taxi_night_pick.map(r => ( ((r(10).toDouble - 40.7 )/0.002)
        .toInt *120 + ((r(9).toDouble + 74.02 )/0.001).toInt , r(15).toFloat )).reduceByKey(_+_)

    val taxi_grid_stats = taxi_grid_drop.join(taxi_grid_pick).join(taxi_grid_tip)   
       
    val yelpdata = sc.textFile(args(1))
    
    val yelp_grid_manhattan = yelpdata.map(_.split(","))
      .filter(r=> r(3).toDouble >= -74.02 && r(3).toDouble <= -73.9)
      .filter(r => r(2).toDouble >= 40.7 && r(2).toDouble <= 40.85 )
      .map(r => ( ((r(2).toDouble - 40.7 )/0.002).toInt *120 + ((r(3).toDouble + 74.02 )/0.001)
      .toInt , (1, r(0).toInt, r(1).toFloat) ))
    
    val yelp_grid_stats = yelp_grid_manhattan
       .reduceByKey((p1, p2) => (p1._1 + p2._1, p1._2 + p2._2, p1._3 + p2._3 ))
       
    val taxi_yelp_grid = taxi_grid_stats.join(yelp_grid_stats)
    
    val taxi_yelp_grid_flat = taxi_yelp_grid
        .map(x => (x._1, x._2._1._1._1,  x._2._1._1._2,  x._2._1._2,  x._2._2._1,  x._2._2._2,  x._2._2._3))
      
    taxi_yelp_grid_flat.saveAsTextFile(args(2))

  }
}