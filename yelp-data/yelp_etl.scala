
// import scala.collection.mutable.ListBuffer

// def clean(arrStr : Array[String]): (ListBuffer[Any] ,Array[String]) = {
// 	val len = arrStr.length
// 	val num_of_categories = (len-9)/2
// 	var categories = new Array[String](num_of_categories)

// 	for ( i <- 0 until num_of_categories) {
// 		categories(i) = arrStr(2+i*2).split("'")(1)
// 	}
// 	var clean_list =  new ListBuffer[Any]()
// 	for (i <- 0 until len) {
// 		if (i == 2) {
// 	 		//clean_list + = (categories)
// 		} else {
// 			if( i < 2) {
// 				clean_list += arrStr(i)
// 			} else {
// 				clean_list += arrStr(i + num_of_categories)
// 			}
// 		}
// 	}
// 	return (clean_list, categories)
// }

val yelpdata = sc.textFile("taxihotspots/businesses.csv")

val header = yelpdata.first()

val yelpdata_noheader = yelpdata.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val yelpdata_clean = yelpdata_noheader.map(x=> x.split(",")).map(x => clean(x))
