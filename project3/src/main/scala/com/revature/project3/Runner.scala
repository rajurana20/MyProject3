package com.revature.project3

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.IntegerType
import com.google.flatbuffers.Struct
import org.apache.spark.sql.types.StructField
import org.apache.spark.sql.Row
import scala.io.BufferedSource
import java.io.FileInputStream
import org.apache.spark.sql.functions.split
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.round 
import java.util.Arrays
import java.sql
import org.apache.parquet.format.IntType
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.types.LongType

object Runner {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      .master("local[4]")
      // .config("spark.debug.maxToStringFields", 100)
      .getOrCreate()

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    val key = System.getenv(("DAS_KEY_ID"))
    val secret = System.getenv(("DAS_SEC"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    //Read WET data from the Common Crawl s3 bucket
    //Since WET is read in as one long text file, use lineSep to split on each header of WET record
    val fileName = "s3a://commoncrawl/crawl-data/CC-MAIN-2021-04/segments/1610703506640.22/wet/CC-MAIN-20210116104719-20210116134719-00799.warc.wet.gz"
    val commonCrawl = spark.read.option("lineSep", "WARC/1.0").text(fileName)
    .as[String]
    .map((str)=>{str.substring(str.indexOf("\n")+1)})
    .toDF("cut WET")

    // Splitting the header WARC data from the plain text content for WET files
    val cuttingCrawl = commonCrawl
      .withColumn("_tmp", split($"cut WET", "\r\n\r\n"))
      .select($"_tmp".getItem(0).as("WARC Header"), $"_tmp".getItem(1).as("Plain Text"))
  
    // Below is filtering for only Tech Jobs
    // First filter for URI's with career/job/employment
    // Second filter makes sure all results returned have keys words for tech jobs
    // This filters out non tech related jobs hat return from job in URI
    val filterCrawl = cuttingCrawl
      .filter(($"WARC Header" rlike ".*WARC-Target-URI:.*career.*" 
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*/job.*") 
        or ($"WARC Header" rlike ".*WARC-Target-URI:.*employment.*"))
      and(($"Plain Text" rlike ".*Frontend.*")
       or ($"Plain Text" rlike ".*Backend.*") 
       or ($"Plain Text" rlike ".*Fullstack.*")
       or ($"Plain Text" rlike ".*Cybersecurity.*") 
       or ($"Plain Text" rlike ".*Software.*") 
       or($"Plain Text" rlike ".*Computer.*")))
      .withColumn("is_entry_tech_job",($"Plain Text" rlike ".*entry.*" or ($"Plain Text" rlike ".*junior.*")))
      .withColumn("is_experience_required",($"Plain Text" rlike ".*experience.*"))
      .select($"WARC Header",$"is_entry_tech_job",$"is_experience_required")
      var cachedFilterCrawl=filterCrawl.persist();
      cachedFilterCrawl.show(50);

      val totalTechJob = cachedFilterCrawl.count();
      println("Total Tech Job: "+ totalTechJob)
      val totalEntryLevelTechJob = cachedFilterCrawl.filter($"is_entry_tech_job").count()
      println("Total Entry Level Tech jobs: " + totalEntryLevelTechJob)
      val totalEntryLevelTechJobRequiredExperience = cachedFilterCrawl.filter($"is_entry_tech_job" && $"is_experience_required").count()
      println("Total Entry Level Tech Jobs That required experience : " + totalEntryLevelTechJobRequiredExperience)
      val percentage = totalEntryLevelTechJobRequiredExperience.toDouble/totalTechJob * 100;
      println("Percentage: " + percentage  + " %")


     val simpleData = Seq(Row(fileName,totalTechJob,totalEntryLevelTechJob,totalEntryLevelTechJobRequiredExperience,percentage))

    val simpleSchema = StructType(Array(
      StructField("fileName",StringType,true),
      StructField("totalTechJob",LongType,true),
      StructField("totalEntryLevelTechJob",LongType,true),
      StructField("totalEntryLevelTechJobRequiredExperience", LongType, true),
      StructField("percentage", DoubleType, true)
    ))

    val dfToWrite = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),simpleSchema)
    dfToWrite.write.mode("append").csv("s3a://rajusecondbucket/crawlresult/")
  
    cachedFilterCrawl.unpersist()
  }

}