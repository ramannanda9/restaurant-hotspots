import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.ml.PipelineStage
import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.OneHotEncoder
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.{ Vector, Vectors }
import org.apache.spark.sql.Row;
import org.apache.spark.ml.tuning.CrossValidator
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.ml.regression.LinearRegression
import org.apache.spark.ml.evaluation.BinaryClassificationEvaluator
import org.apache.spark.mllib.evaluation.RegressionMetrics
import org.apache.spark.ml.tuning.ParamGridBuilder
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.StructType
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.sql.DataFrame
import scala.collection.mutable.ListBuffer
import org.apache.spark.ml.PipelineModel
import org.apache.hadoop.mapred.InvalidInputException
import org.apache.spark.ml.tuning.CrossValidatorModel
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.types.FloatType
import org.apache.spark.ml.regression.LinearRegressionModel

object HotspotAnalytics {
  val sqlContext = new SQLContext(sc)
  val hiveCtxt = new HiveContext(sc)
  var linearModel:LinearRegressionModel=_
  var df: DataFrame = _
  def initializeDataFrame(query: String): DataFrame = {
    //cache the dataframe for performance benefit
    if (df == null) {
      df = hiveCtxt.sql(query).na.drop().cache()
    }
    df.describe().show()
    return df
  }
  def preprocessFeatures(df: DataFrame): DataFrame = {
    val stringColumns = Array("grid_id")

    var indexModel: PipelineModel = null;
    var oneHotModel: PipelineModel = null;
    try {
      indexModel = sc.objectFile[PipelineModel]("Restaurant-Hotspots.model.indexModel").first()

    } catch {
      case e: InvalidInputException => println()
    }
    if (indexModel == null) {
      val stringIndexTransformer: Array[PipelineStage] = stringColumns.map(
        cname => new StringIndexer().setInputCol(cname).setOutputCol(s"${cname}_index"))
      val indexedPipeline = new Pipeline().setStages(stringIndexTransformer)
      indexModel = indexedPipeline.fit(df)
      sc.parallelize(Seq(indexModel), 1).saveAsObjectFile("Restaurant-Hotspots.model.indexModel")

    }

    var df_indexed = indexModel.transform(df)
    stringColumns.foreach { x => df_indexed = df_indexed.drop(x) }
    val indexedColumns = df_indexed.columns.filter(colName => colName.contains("_index"))
    val oneHotEncodedColumns = indexedColumns
    try {
      oneHotModel = sc.objectFile[PipelineModel]("Restaurant-Hotspots.model.onehot").first()
    } catch {
      case e: InvalidInputException => println()
    }

    if (oneHotModel == null) {
      val oneHotTransformer: Array[PipelineStage] = oneHotEncodedColumns.map { cname =>
        new OneHotEncoder().
          setInputCol(cname).setOutputCol(s"${cname}_vect")
      }
      val oneHotPipeline = new Pipeline().setStages(oneHotTransformer)
      oneHotModel = oneHotPipeline.fit(df_indexed)

      sc.parallelize(Seq(oneHotModel), 1).saveAsObjectFile("Restaurant-Hotspots.model.onehot")
    }

    df_indexed = oneHotModel.transform(df_indexed)
    indexedColumns.foreach { colName => df_indexed = df_indexed.drop(colName) }
    df_indexed
  }

  def buildModel(query: String){
    initializeDataFrame(query)
    var df_indexed = df
    val df_splitData: Array[DataFrame] = df_indexed.randomSplit(Array(0.9, 0.1), 11l)
    val trainData = df_splitData(0)
    val testData = df_splitData(1)
    val testData_x = testData.drop("tip")
    val testData_y = testData.select("tip")
    val columnsToTransform = trainData.drop("tip").columns
    val vectorAssembler = new VectorAssembler().
      setInputCols(columnsToTransform).setOutputCol("features")
    columnsToTransform.foreach { x => println(x) }
    val trainDataTemp = vectorAssembler.transform(trainData).withColumnRenamed("tip", "label")
    val testDataTemp = vectorAssembler.transform(testData_x)
    var trainDataFin = trainDataTemp.select("features", "label")
    var testDataFin = testDataTemp.select("features")
    val linearRegression = new LinearRegression()
    val paramGridMap = new ParamGridBuilder().
      addGrid(linearRegression.maxIter, Array(10, 100, 1000)).
      addGrid(linearRegression.regParam, Array(0.1, 0.01, 0.001, 0.0001,1, 10)).build()
    //5 fold cross validation
    val cv = new CrossValidator().setEstimator(linearRegression).
      setEvaluator(new RegressionEvaluator()).setEstimatorParamMaps(paramGridMap).setNumFolds(5)
    //Fit the model
    val model = cv.fit(trainDataFin)
    val modelResult = model.transform(testDataFin)
    model.bestModel.params.foreach { x => println(x.name, " ", x.toString()) }
    val predictionAndLabels = modelResult.map(r => r.getAs[Double]("prediction")).zip(testData_y.map(R => R.getAs[Double](0)))
    val regressionMetrics = new RegressionMetrics(predictionAndLabels)
    //Print the results
    println(s"R-Squared= ${regressionMetrics.r2}")
    println(s"Explained Variance=${regressionMetrics.explainedVariance}")
    println(s"MAE= ${regressionMetrics.meanAbsoluteError}")

    val lrModel = model.bestModel.asInstanceOf[LinearRegressionModel]
    linearModel=lrModel
    println(lrModel.explainParams())
    println(lrModel.weights)
    // sc.parallelize(Seq(model), 1).saveAsObjectFile("Restaurant-Hotspots.model")
  }

  def runRegressions() {
    var regressionModel: CrossValidatorModel = null;
    // try {
    //   lyftModel = sc.objectFile[CrossValidatorModel]("Restaurant-Hotspots.model").first()
    // } catch {
    //   case e: InvalidInputException => println()
    // }

    if (regressionModel == null) {
      // buildModel("""select cast(tip as double)/(pickups+dropoffs) as tip ,total_review_cnts*1.0/number_of_restaurants, sum_of_ratings*1.0/number_of_restaurants from default.yelp_taxi_stats""")      buildModel("""select cast(tip as double)/(pickups+dropoffs) as tip ,total_review_cnts*1.0/number_of_restaurants, sum_of_ratings*1.0/number_of_restaurants from default.yelp_taxi_stats""")
      buildModel(""" select pickups as pickups,dropoffs as dropoffs , cast(tip as double)/(pickups+dropoffs) as tip ,
      total_review_cnts*1.0/number_of_restaurants as total_review_cnts, sum_of_ratings*1.0/number_of_restaurants as sum_of_ratings from default.yelp_taxi_stats """)
    }
    // regressionModel = sc.objectFile[CrossValidatorModel]("Restaurant-Hotspots.model").first()
    // var schema = StructType(Array(
    //   StructField("name", StringType, true),
    //   StructField("duration", IntegerType, true),
    //   StructField("distance", FloatType, true),
    //   StructField("primetime", FloatType, true),
    //   StructField("hour", IntegerType, true),
    //   StructField("minute", IntegerType, true),
    //   StructField("second", IntegerType, true)))
    // var rows: ListBuffer[Row] = new ListBuffer
    // list.foreach(x => rows += Row(x.name, x.duration, x.distance, x.primetime, x.hour, x.minute, x.hour))
    // val row = sc.parallelize(rows)
    // var dfStructure = sqlContext.createDataFrame(row, schema)
    // var preprocessed = preprocessFeatures(dfStructure)
    // preprocessed.describe().show()
    // val vectorAssembler = new VectorAssembler().
    //   setInputCols(preprocessed.columns).setOutputCol("features")
    // preprocessed = vectorAssembler.transform(preprocessed)
    // var results = lyftModel.transform(preprocessed.select("features"))
    // return results
  }
}
