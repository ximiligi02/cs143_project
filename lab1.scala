val wordList = Array("cat", "elephant", "rat", "rat", "cat")
val wordsRDD = sc.parallelize(wordList)
wordsRDD.collect.foreach(w => println(w))

def makePlural(word: String): String = {
    word+'s'
}

println(makePlural("cat"))
val pluralRDD = wordsRDD.map( makePlural )
pluralRDD.collect.foreach(println)

assert(pluralRDD.collect() sameElements Array("cats", "elephants", "rats", "rats", "cats"),
                  "incorrect values for pluralRDD")

// TODO: Replace <FILL IN> with appropriate code
/*val pluralLambdaRDD = wordsRDD.map( x=>x+'s' )
pluralLambdaRDD.collect.foreach(println)
assert(pluralLambdaRDD.collect() sameElements Array("cats", "elephants", "rats", "rats", "cats"),
                  "incorrect values for pluralRDD")*/

// TODO: Replace <FILL IN> with appropriate code
val pluralLengths = pluralRDD.map( x=>x.length ).collect()
pluralLengths.foreach(print)
/*assert(pluralLengths sameElements Array(4, 9, 4, 4, 4),
                  "incorrect values for pluralLengths")*/
val wordPairs = wordsRDD.map( x=>(x,1) )
wordPairs.collect.foreach(println)
/*assert(wordPairs.collect sameElements Array(("cat", 1), ("elephant", 1), ("rat", 1), ("rat", 1), ("cat", 1)),
                  "incorrect value for wordPairs")*/
val wordsGrouped = wordPairs.groupByKey()
for { record <- wordsGrouped.collect
  key = record._1
  value = record._2} println(s"${key}: ${value}")
/*assert(wordsGrouped.collect.map(r => (r._1, r._2.toList)).sortBy(_._1) 
       sameElements Array(("cat", List(1, 1)), ("elephant", List(1)), ("rat", List(1, 1))),
                  "incorrect value for wordsGrouped")*/
val wordCountsGrouped = wordsGrouped.map( x=>(x._1,x._2.sum))

wordCountsGrouped.collect.foreach(group => println(group))
/*assert(wordCountsGrouped.collect.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCountsGrouped")*/
val wordCounts = wordPairs.reduceByKey((pre, after) => pre + after)
wordCounts.collect.foreach(println _)
/*assert(wordCounts.collect.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCounts")*/
val wordCountsCollected = wordsRDD.map( x=>(x,1) ).reduceByKey((pre, after) => pre + after).collect()
wordCountsCollected.foreach(println)
/*assert(wordCountsCollected.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCounts")*/
val uniqueWords = wordCounts.count
println(s"Unique words = ${uniqueWords}")
/*assert(uniqueWords == 3, "incorrect count of unique words")*/
val totalCount = wordCounts.map( x=>x._2 ).reduce( (pre, after) => pre + after )
val average = totalCount / uniqueWords.toFloat
println(s"total count = ${totalCount}, average = ${average}")

/*def truncate(value: Float) = (math floor value * 100) / 100
;
assert (truncate(average) == 1.66, "incorrect value of average")*/
import org.apache.spark.rdd.RDD

def wordCount(wordListRDD: RDD[String]): RDD[(String, Int)] = {
    wordListRDD.map( x=>(x,1) ).reduceByKey((pre, after) => pre + after)
}

wordCount(wordsRDD).collect().foreach(println)
/*assert(wordCount(wordsRDD).collect.sortBy(_._1) 
       sameElements Array(("cat", 2), ("elephant", 1), ("rat", 2)),
                  "incorrect value for wordCounts")*/
def removePunctuation(text: String): String = {

  ("[ a-zA-Z0-9]+".r findAllIn text).toList.mkString.trim().toLowerCase();
 
}
println(removePunctuation("Hi, you!"))
println(removePunctuation(" No under_score!"))

/*assert(removePunctuation(" The Elephant's 4 cats. ") == "the elephants 4 cats",
                  "incorrect definition for removePunctuation function")*/


import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils

FileUtils.copyURLToFile(new URL("http://web.cs.ucla.edu/~tcondie/cs143data/pg100.txt"), new File("/tmp/shakespeare.txt"))
dbutils.fs.mv("file:/tmp/shakespeare.txt", "dbfs:/tmp/shakespeare.txt")

val shakespeareRDD = sc.textFile("/tmp/shakespeare.txt", 8).map(removePunctuation)
println(shakespeareRDD.zipWithIndex().map({
  case (l, num) => s"${num}: ${l}"
}).take(15).mkString("\n"))

val shakespeareWordsRDD = shakespeareRDD.flatMap( x=>x.split(' ') )
val shakespeareWordCount = shakespeareWordsRDD.count()

println(shakespeareWordsRDD.top(5).mkString("\n"))
println(s"shakespeareWordCount = ${shakespeareWordCount}")
//assert(shakespeareWordsRDD.top(5).mkString(",") == "zwaggerd,zounds,zounds,zounds,zounds", "incorrect value for shakespeareWordsRDD")
val shakeWordsRDD = shakespeareWordsRDD.filter( _!="" )
val shakeWordCount = shakeWordsRDD.count()
println(s"shakeWordCount = ${shakeWordCount}")
//assert(shakeWordCount == 903705, "possible incorect value for shakeWordCount")
val top15WordsAndCounts = wordCount( shakeWordsRDD ).takeOrdered( 15 )(Ordering[Int].reverse.on(_._2))

println(top15WordsAndCounts.mkString("\n"))
/*assert(top15WordsAndCounts.mkString(",") == "(the,27825),(and,26791),(i,20681),(to,19261),(of,18289),(a,14667),(you,13716),(my,12481),(that,11135),(in,11027),(is,9621),(not,8745),(for,8261),(with,8046),(me,7769)", "incorrect value for top15WordsAndCounts")*/

