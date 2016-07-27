import scala.collection.mutable.ListBuffer

def clean(arrStr : Array[String]): ListBuffer[Any] = {
	val len = arrStr.length
	val num_of_categories = (len-9)/2
	
	var clean_list =  new ListBuffer[Any]()
	for (i <- 0 until len) {
		if (i >= 2 && i<(2+num_of_categories*2)) {
			// TODO
		} else {
				clean_list += arrStr(i)
		}
	}
	return clean_list
}

val yelpdata = sc.textFile("taxihotspots/businesses.csv")

val header = yelpdata.first()

val yelpdata_noheader = yelpdata.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }

val yelpdata_clean = yelpdata_noheader.map(x=> x.split(",")).map(x => clean(x))

yelpdata_clean.saveAsTextFile("yelpdata_clean")