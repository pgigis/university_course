
val pagecounts = sc.textFile("/home/petros/Desktop/pagecounts-20160101-000000_parsed.out")

 
// 1 (done)
def retrieveFirst_k_Records(pagecounts:org.apache.spark.rdd.RDD[String], k:Int) : Unit = {
	pagecounts.take(k).foreach(println)
}



// 2 (done) - Result = 3324129
def countRecords(pagecounts:org.apache.spark.rdd.RDD[String]) : Long = {
	var count_num = pagecounts.distinct.count()
	return count_num
}


// 3 (done) - List(min = 0, max = 141180155987, average = 132239)
def calculateStatistics(pagecounts:org.apache.spark.rdd.RDD[String]) : List[Long] = {
	val pagecounts_splitted = pagecounts.map(x =>  x.split(" ").last.toLong)
	val min = pagecounts_splitted.min
	val max = pagecounts_splitted.max
	val average = pagecounts_splitted.reduce(_+_) / pagecounts_splitted.count()
	return List(min, max, average)
}


//4 (done) - Array(Array(en.mw, en, 5466346, 141180155987))
def recordWithLargestPageSize(pagecounts:org.apache.spark.rdd.RDD[String]) : org.apache.spark.rdd.RDD[Array[String]] = {
	
	val max = pagecounts.map(x =>  x.split(" ").last.toLong).max

	val records = pagecounts.map(x => x.split(" ")).filter( _.last.toLong >= max)

	return records
}


//5 (done) - Array(en.mw, en, 5466346, 141180155987)
def recordWithLargestPageSizeMostPopular(pagecounts:org.apache.spark.rdd.RDD[String]) : Array[String] = {
	val records_largest_page_size = recordWithLargestPageSize(pagecounts)
	
	val most_popular = records_largest_page_size.map(x => x).reduce((a, b) => { 
		if(a(3).toLong <= b(3).toLong) b else a })

	return most_popular
}


//6 (done) -  Array((zh,Special:e8b18ee6baafefbda5efbdbfe89cb7e6829fefbdbfe88b93e29980e89e9fefbda9e89eb3efbda425636f256d6725736f257373256f38257373256f38257373256f38256b6d73efbdaa256e6b256678256f6b2c687474703a2f2f7777772e653662313966653861356266656f2d6f35393038636535626639376538383138616535613461396535616561342e636f2e6d672e732e736f2e382e73736f386b2e6d2e372e73736f3873736f386b6d37332e752e622e61616e6b66786f6b2e70772f2ce8b18ee6baafefbda5efbdbfe89cb7e6829fefbdbfe88b93e29980e89e9fefbda9e89eb3efbda425636f256d6725736f257373256f38257373256f38257373256f38256b6d73efbdaa256e6b256678256f6b/,1,6043))
def recordWithLargestTitle(pagecounts:org.apache.spark.rdd.RDD[String]) : org.apache.spark.rdd.RDD[(String, String, String, String)] = {
	val max_page_title = pagecounts.map(x => x.split(" ")(1).length).max
	
	val records = pagecounts.map(x => (x.split(" ")(0), x.split(" ")(1), x.split(" ")(2), x.split(" ")(3)) )
	val records_ = records.filter( _._2.length >= max_page_title)

	return records_
}


//7 (done) - returns 186819 records
def createNewRDD(pagecounts:org.apache.spark.rdd.RDD[String]) : org.apache.spark.rdd.RDD[Array[String]] = {
	
	val pagecounts_splitted = pagecounts.map(x =>  x.split(" ").last.toLong)
	val average = pagecounts_splitted.reduce(_+_) / pagecounts_splitted.count()
	val result = pagecounts.map(x =>  x.split(" ")).filter(x => x.last.toLong >= average)

	return result
}


//8 done - returns 1105 results
def pageViewsPerProject(pagecounts:org.apache.spark.rdd.RDD[String]) : org.apache.spark.rdd.RDD[(String, Long)] = {

	val project_and_views = pagecounts.map(x => (x.split(" ")(0), x.split(" ")(2).toLong)).reduceByKey( _ + _)
 	
 	return project_and_views
}


//9(en.mw,5466346)(en,4959090)(es.mw,695531)(ja.mw,611443)(de.mw,572119)(fr.mw,536978)(ru.mw,466742)(it.mw,400297)(de,315929)(commons.m,285796) 
def mostPopularPageviewsSortedByHits(pagecounts:org.apache.spark.rdd.RDD[String]) : Unit = {

	val project_and_views = pagecounts.map(x => (x.split(" ")(0), x.split(" ")(2).toLong)).reduceByKey( _ + _).sortBy(_._2, false)

	project_and_views.take(10).foreach(println)

}

//10 a - returns 41931
def pageTitlesStartingWith_The(pagecounts:org.apache.spark.rdd.RDD[String]) : Long = {

	val results_with_The = pagecounts.map(x =>  x.split(" ")).filter(x => x(1).startsWith("The") && (x(1).matches("The[^a-zA-Z].*") || x(1).length == 3) ).count()

	return results_with_The
}

//10 b - 8026
def pageTitlesStartingWith_The_and_not_en(pagecounts:org.apache.spark.rdd.RDD[String]) : Long = {

	val results_with_The = pagecounts.map(x =>  x.split(" ")).filter(x => x(1).startsWith("The") && (x(1).matches("The[^a-zA-Z].*") || x(1).length == 3) && !x(0).startsWith("en")).count()

	return results_with_The
}

//11 done - returns 76.96248
def percent_of_pages_with_one_view(pagecounts:org.apache.spark.rdd.RDD[String]) : Float = {

	val total = pagecounts.map(x => x).count()
	val pages_with_one_view = pagecounts.map(x =>  x.split(" ")(2).toLong).filter(x => x == 1).count()

	return (pages_with_one_view.asInstanceOf[Float] / total.asInstanceOf[Float]) * 100
} 

//12 done - returns 849974
def unique_terms_pagetitle(pagecounts:org.apache.spark.rdd.RDD[String]) : Long = {

	val array = pagecounts.flatMap(x => x.split(" ")(1).toLowerCase.split("_") ).filter(_.matches("[a-zA-Z0-9]+"))

	val result = array.distinct.count()

	return result

} 

//13 done - (of,194307)
def most_frequent_term_pagetitle(pagecounts:org.apache.spark.rdd.RDD[String]) : Array[(String, Int)] = {

	val array = pagecounts.flatMap(x => x.split(" ")(1).toLowerCase.split("_") ).filter(_.matches("[a-zA-Z0-9]+"))

	val result = array.map(x => (x, 1)).reduceByKey( _ + _ ).sortBy(_._2, false).take(1)

	return result

} 