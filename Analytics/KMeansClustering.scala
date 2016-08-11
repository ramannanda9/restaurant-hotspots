package analysis

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors
import java.lang.NumberFormatException;

object KMeansClustering {
  def main(args: Array[String]) {
    if (args.length < 4) {
      args.foreach { println }
      println("Usage: <dataFile> <pathToSaveModel> <numClusters> <numIterations>")
      System.exit(0)
    }
    
    var numClusters = args(2).toInt
    var numIterations = args(3).toInt
    
    val conf = new SparkConf().setAppName("KMeans Clustering")
    //val conf = new SparkConf().setAppName("KMeans Clustering").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    val data = sc.textFile(args(0))
    val extractedData = data.map(line => line.split(","))
      .map(arr => (arr.apply(6), arr.apply(5)))
      
    val taxiPickUpCoords = extractedData
      .map(tuple => Vectors.dense(tuple._1.toDouble, tuple._2.toDouble)).cache() 
    
    val clusters = KMeans.train(taxiPickUpCoords, numClusters, numIterations)
    
    val clusterCoords = clusters.clusterCenters
    val coordStrings = clusterCoords.map(x => x.apply(0) + "," + x.apply(1))
    val clusterCoordsRDD = sc.parallelize(clusterCoords, 1)
    clusterCoordsRDD.saveAsObjectFile(args(1))
    //val stringsRDD = sc.parallelize(coordStrings, 1);
    //stringsRDD.saveAsTextFile(args(1))

    sc.stop()
  }
}