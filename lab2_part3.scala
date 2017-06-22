// Databricks notebook source
import java.net.URL
import java.io.File
import org.apache.commons.io.FileUtils

FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/ccl_header_file.csv"), new File("/tmp/ccl_header_file.csv"))
FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/ccl.txt"), new File("/tmp/ccl.txt"))
dbutils.fs.mv("file:/tmp/ccl.txt", "dbfs:/tmp/ccl.txt")
dbutils.fs.mv("file:/tmp/ccl_header_file.csv", "dbfs:/tmp/ccl_header_file.csv")

FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/cm_header_file.csv"), new File("/tmp/cm_header_file.csv"))
FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/cm.txt"), new File("/tmp/cm.txt"))
dbutils.fs.mv("file:/tmp/cm.txt", "dbfs:/tmp/cm.txt")
dbutils.fs.mv("file:/tmp/cm_header_file.csv", "dbfs:/tmp/cm_header_file.csv")

FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/cn_header_file.csv"), new File("/tmp/cn_header_file.csv"))
FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/cn.txt"), new File("/tmp/cn.txt"))
dbutils.fs.mv("file:/tmp/cn.txt", "dbfs:/tmp/cn.txt")
dbutils.fs.mv("file:/tmp/cn_header_file.csv", "dbfs:/tmp/cn_header_file.csv")

FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/indiv_header_file.csv"), new File("/tmp/indiv_header_file.csv"))
FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/itcont.txt"), new File("/tmp/itcont.txt"))
dbutils.fs.mv("file:/tmp/itcont.txt", "dbfs:/tmp/itcont.txt")
dbutils.fs.mv("file:/tmp/indiv_header_file.csv", "dbfs:/tmp/indiv_header_file.csv")

FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/oth_header_file.csv"), new File("/tmp/oth_header_file.csv"))
FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/itoth.txt"), new File("/tmp/itoth.txt"))
dbutils.fs.mv("file:/tmp/itoth.txt", "dbfs:/tmp/itoth.txt")
dbutils.fs.mv("file:/tmp/oth_header_file.csv", "dbfs:/tmp/oth_header_file.csv")

FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/pas2_header_file.csv"), new File("/tmp/pas2_header_file.csv"))
FileUtils.copyURLToFile(new URL("http://www.cs.ucla.edu/~tcondie/cs143data/fec_2016_3_25/itpas2.txt"), new File("/tmp/itpas2.txt"))
dbutils.fs.mv("file:/tmp/itpas2.txt", "dbfs:/tmp/itpas2.txt")
dbutils.fs.mv("file:/tmp/pas2_header_file.csv", "dbfs:/tmp/pas2_header_file.csv")

// COMMAND ----------

// Import Row.
import org.apache.spark.sql.Row;

// Import Spark SQL data types
import org.apache.spark.sql.types.{StructType,StructField,StringType};

// table: cm
val cmHeader = sc.textFile(s"/tmp/cm_header_file.csv").collect()(0).split(",")
val cmSchema = StructType(cmHeader.map(fieldName => StructField(fieldName, StringType, true)))
val cm = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","|").option("nullValue", "NULL").load(s"/tmp/cm.txt")
val cmWithSchema = sqlContext.createDataFrame(cm.rdd, cmSchema)
cmWithSchema.createOrReplaceTempView("cm")

// table: cn
val cnHeader = sc.textFile(s"/tmp/cn_header_file.csv").collect()(0).split(",")
val cnSchema = StructType(cnHeader.map(fieldName => StructField(fieldName, StringType, true)))
val cn = sqlContext.read.format("com.databricks.spark.csv").option("delimiter","|").option("nullValue", "NULL").load(s"/tmp/cn.txt")
val cnWithSchema = sqlContext.createDataFrame(cn.rdd, cnSchema)
cnWithSchema.createOrReplaceTempView("cn")

// table: itcont
val itcontHeader = sc.textFile(s"/tmp/indiv_header_file.csv").collect()(0).split(",")
val itcontSchema = StructType(itcontHeader.map(fieldName => StructField(fieldName, StringType, true)))
val itcont = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option ("nullValue", "NULL").load(s"/tmp/itcont.txt")
val itcontWithSchema = sqlContext.createDataFrame(itcont.rdd, itcontSchema)
itcontWithSchema.createOrReplaceTempView("itcont")

// table: ccl
val cclHeader = sc.textFile(s"/tmp/ccl_header_file.csv").collect()(0).split(",")
val cclSchema = StructType(cclHeader.map(fieldName => StructField(fieldName, StringType, true)))
val ccl = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option ("nullValue", "NULL").load(s"/tmp/ccl.txt")
val cclWithSchema = sqlContext.createDataFrame(ccl.rdd, cclSchema)
cclWithSchema.createOrReplaceTempView("ccl")

// table: itpas
val itpasHeader = sc.textFile(s"/tmp/pas2_header_file.csv").collect()(0).split(",")
val itpasSchema = StructType(itpasHeader.map(fieldName => StructField(fieldName, StringType, true)))
val itpas = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option ("nullValue", "NULL").load(s"/tmp/itpas2.txt")
val itpasWithSchema = sqlContext.createDataFrame(itpas.rdd, itpasSchema)
itpasWithSchema.createOrReplaceTempView("itpas")

// table: itoth
val itothHeader = sc.textFile(s"/tmp/oth_header_file.csv").collect()(0).split(",")
val itothSchema = StructType(itpasHeader.map(fieldName => StructField(fieldName, StringType, true)))
val itoth = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("delimiter","|").option ("nullValue", "NULL").load(s"/tmp/itoth.txt")
val itothWithSchema = sqlContext.createDataFrame(itoth.rdd, itothSchema)
itpasWithSchema.createOrReplaceTempView("itoth")

// COMMAND ----------

// MAGIC %sql 
// MAGIC SELECT cn.CAND_NAME,cn.CAND_ID,cn.CAND_PCC
// MAGIC FROM   cn
// MAGIC WHERE   (cn.CAND_NAME='CLINTON, HILLARY RODHAM' OR cn.CAND_NAME='TRUMP, DONALD J'OR cn.CAND_NAME='SANDERS, BERNARD'OR cn.CAND_NAME='CRUZ, RAFAEL EDWARD \"TED\"' ) AND cn.CAND_ELECTION_YR=2016

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT cn.CAND_NAME,itcont.CMTE_ID,COUNT(*)
// MAGIC FROM   cn,itcont
// MAGIC WHERE  (cn.CAND_NAME='CLINTON, HILLARY RODHAM' OR cn.CAND_NAME='TRUMP, DONALD J'OR cn.CAND_NAME='SANDERS, BERNARD'OR cn.CAND_NAME='CRUZ, RAFAEL EDWARD \"TED\"' )AND cn.CAND_ELECTION_YR=2016 AND cn.CAND_PCC=itcont.CMTE_ID 
// MAGIC GROUP BY cn.CAND_NAME,itcont.CMTE_ID

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT cn.CAND_NAME,itcont.CMTE_ID,SUM(itcont.TRANSACTION_AMT)
// MAGIC FROM   cn,itcont
// MAGIC WHERE  (cn.CAND_NAME='CLINTON, HILLARY RODHAM' OR cn.CAND_NAME='TRUMP, DONALD J'OR cn.CAND_NAME='SANDERS, BERNARD'OR cn.CAND_NAME='CRUZ, RAFAEL EDWARD \"TED\"' )AND cn.CAND_ELECTION_YR=2016 AND cn.CAND_PCC=itcont.CMTE_ID 
// MAGIC GROUP BY cn.CAND_NAME,itcont.CMTE_ID

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT DISTINCT cn.CAND_NAME, ccl.CMTE_ID, cm.CMTE_NM
// MAGIC FROM   cn,ccl,cm
// MAGIC WHERE  cn.CAND_ID=ccl.CAND_ID AND ccl.CMTE_ID=cm.CMTE_ID AND (cn.CAND_NAME='CLINTON, HILLARY RODHAM' OR cn.CAND_NAME='TRUMP, DONALD J'OR cn.CAND_NAME='SANDERS, BERNARD'OR cn.CAND_NAME='CRUZ, RAFAEL EDWARD \"TED\"' )AND cn.CAND_ELECTION_YR=2016 

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT cn.CAND_NAME,cn.CAND_ID,COUNT(*)
// MAGIC FROM   cn,itpas
// MAGIC WHERE  (cn.CAND_NAME='CLINTON, HILLARY RODHAM' OR cn.CAND_NAME='TRUMP, DONALD J'OR cn.CAND_NAME='SANDERS, BERNARD'OR cn.CAND_NAME='CRUZ, RAFAEL EDWARD \"TED\"' )AND cn.CAND_ELECTION_YR=2016 AND cn.CAND_ID=itpas.CAND_ID 
// MAGIC GROUP BY cn.CAND_NAME,cn.CAND_ID

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT cn.CAND_NAME, cn.CAND_ID, SUM(itpas.TRANSACTION_AMT)
// MAGIC FROM   cn,itpas
// MAGIC WHERE  (cn.CAND_NAME='CLINTON, HILLARY RODHAM' OR cn.CAND_NAME='TRUMP, DONALD J'OR cn.CAND_NAME='SANDERS, BERNARD'OR cn.CAND_NAME='CRUZ, RAFAEL EDWARD \"TED\"' )AND cn.CAND_ELECTION_YR=2016 AND cn.CAND_ID=itpas.CAND_ID
// MAGIC GROUP BY cn.CAND_NAME,cn.CAND_ID
// MAGIC ORDER BY SUM(itpas.TRANSACTION_AMT)

// COMMAND ----------



// COMMAND ----------


