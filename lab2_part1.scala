import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils



FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/bible+shakes.nopunc"), new File("/tmp/bibleshakes.nopunc.txt"))
dbutils.fs.mv("file:/tmp/bibleshakes.nopunc.txt", "dbfs:/tmp/bibleshakes.nopunc.txt")

val dataFile = sc.textFile("/tmp/bibleshakes.nopunc.txt")
val lineWords = dataFile.map(line => line.trim.split(" "))

// Form the bigrams and get the the count of each
val biGramCounts = lineWords.flatMap(words => words.filter(!_.isEmpty).sliding(2)).filter(_.length==2).map(biGramList => (biGramList.mkString(" "),1)).reduceByKey((pre, after) => pre + after)
//biGramCounts.collect().foreach(println _)


// Form groups out of the bigrams
val groupWordBigrams = biGramCounts.flatMap({
 case (bigram, count) => bigram.split(" ").map(word => (word, (bigram,count)))}).groupByKey()
//groupWordBigrams.take(20).foreach(println _)


// For each word, get the top 5 bigrams
val top5BiGrams = groupWordBigrams.map({
  case (word, bigramTuples) => (word, bigramTuples.toList.sortBy({
    case (bigram, count) => -count
  }).take(5))
})

top5BiGrams.take(20).foreach(println _)  
