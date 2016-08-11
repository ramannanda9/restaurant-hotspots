package analysis

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.regression.LinearRegressionModel
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}

object Regression {
  
  def main(args: Array[String]) {
    if (args.length < 4) {
      args.foreach { println }
      println("Usage: <clusterFilePath> <joinedDataFilePath> <outputPath> <numIterations>")
      System.exit(0)
    }
    
    val conf = new SparkConf().setAppName("Cluster Regression")
    //val conf = new SparkConf().setAppName("Cluster Regression").setMaster("local[*]")
    val sc = new SparkContext(conf)
    
    //load the joined data and cluster coordinates
    val clusterData = sc.objectFile(args(0), 1);
    val joinedData = sc.textFile(args(1))
    val extractedData = joinedData.map { line =>
      val lineArr = line.split(',')
      //create a labeled point were the first field is the dependent variable and the 
      //rest of the array are independent variables
      LabeledPoint(lineArr(0).toDouble, Vectors.dense(lineArr(1).split(',').map(_.toDouble)))}.cache()
    
    //create linear model
    val numIters = args(3).toInt
    val model = LinearRegressionWithSGD.train(extractedData, numIters, 0.00000001)
    
    val valuesAndPreds = extractedData.map { point =>
    val pred = model.predict(point.features)
    (point.label, pred)
   }.persist()
      
    val coorCoeff = Statistics.corr(valuesAndPreds.keys, valuesAndPreds.values)
    
    
    //save
    model.save(sc, args(2))
    
  }
}