package com.revature.project3

import org.apache.spark.sql.SparkSession
import org.json4s._
import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.functions
import java.io.PrintWriter
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.catalyst.expressions.aggregate.Sum
//import org.archive.io.ArchiveReader;
// import org.archive.io.ArchiveRecord;
// import org.archive.io.warc.WARCReaderFactory;
import org.jets3t.service.S3Service;
import org.jets3t.service.S3ServiceException;
import org.jets3t.service.impl.rest.httpclient.RestS3Service;
import org.jets3t.service.model.S3Object;
import org.apache.commons.net.ntp.TimeStamp



object Runner {
  
  def main(args: Array[String]): Unit = {
    implicit def formats = DefaultFormats
    val spark = SparkSession
      .builder()
      .appName("scalas3read")
      .master("local[4]")
      .getOrCreate()

    // Reference: https://sparkbyexamples.com/spark/spark-read-text-file-from-s3/#s3-dependency
    val key = System.getenv(("DAS_KEY_ID"))
    val secret = System.getenv(("DAS_SEC"))

    spark.sparkContext.hadoopConfiguration.set("fs.s3a.access.key", key)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.secret.key", secret)
    spark.sparkContext.hadoopConfiguration.set("fs.s3a.endpoint", "s3.amazonaws.com")
    
    import spark.implicits._
    spark.sparkContext.setLogLevel("WARN")

    val rddFromFile = spark.read.parquet( "s3a://commoncrawl/cc-index/table/cc-main/warc/crawl=CC-MAIN-2021-04/subset=warc/part-00298-364a895c-5e5c-46bb-846e-75ec7de82b3b.c000.gz.parquet" );
    rddFromFile.printSchema();
    import org.apache.spark.sql.functions.udf

    def checkIsTechJob(fileName : String, url : String):Boolean={
    //   val spark2 = SparkSession
    //   .builder()
    //   .appName("scalas3read")
    //   .master("local[4]")
    //   .getOrCreate()

    //   import spark2.implicits._
    //   spark2.sparkContext.setLogLevel("WARN")

    //   val df2 = spark2.sparkContext.textFile(s"s3a://commoncrawl/$fileName").zipWithIndex().toDF()
    //   val start = df2.select("_2").where($"_1".contains(s"WARC-Target-URI: $url")).collect()
    //   val startInt = start(0).toString().substring(1,start(0).toString().length()-1).toInt

    //   val end = df2.filter($"_2" > startInt && $"_1"==="WARC/1.0").select($"_2")collect()
    //   val endInt = end(0).toString().substring(1,end(0).toString().length()-1).toInt
 
    //   val df4=df2.filter($"_2".between(startInt,endInt))
    //   val isItTech=df4.filter($"_2".rlike("sofware")).isEmpty
    
    //  // println(isItTech);
   
    //   if(isItTech) 0 else 1;

    true
    }

    def checkIsEntryTechJob(fileName : String, url : String):Boolean={
      // val df2 = spark.sparkContext.textFile(s"s3a://commoncrawl/$fileName").zipWithIndex()
      // val df3 = df2.toDF()
        
      // val start = df3.select("_2").where($"_1"===s"WARC-Target-URI: ${url}").collect()
      // val startInt = start(0).toString().substring(1,start(0).toString().length()-1).toInt

      // val end = df3.filter($"_2" > startInt && $"_1"==="WARC/1.0").select($"_2")collect()
      // val endInt = end(0).toString().substring(1,end(0).toString().length()-1).toInt
 
      // val df4=df3.filter($"_2".between(startInt,endInt))
      // val isItTechEntry=df4.filter($"_2".contains("Entry Level","fresher")).isEmpty
    
      // isItTechEntry

      true;
  }

    val isTechJob = udf((fileName : String, url : String)=> checkIsTechJob(fileName,url))
    val isEntryTechJob = udf((fileName : String, url : String)=> checkIsEntryTechJob(fileName,url))

    spark.udf.register("isTechJob", isTechJob)
    spark.udf.register("isEntryTechJob", isEntryTechJob)
    
    rddFromFile.createTempView("temp");
    val query =spark.sql("SELECT url_host_name, url, warc_filename, isTechJob(warc_filename,url) as is_tech_job, isEntryTechJob(warc_filename,url) as is_entry_tech_job from temp where url_path like '%job%'");
    //val query =spark.sql("SELECT url_host_name, url, warc_filename as content from temp where url_path like '%job%'");
    //val query =spark.sql("SELECT url_host_name, count(*) as n FROM temp WHERE url_path like '%job%' group by 1 order by n desc limit 200");
    query.take(1).foreach(println)
    query.show();
    //checkIsTechJob("crawl-data/CC-MAIN-2021-04/segments/1610703519923.26/warc/CC-MAIN-20210120054203-20210120084203-00078.warc.gz","https://servlife.org/executive-assistant-job-description/")

  

  def myTesting(fileName : String, url : String)={
    // val df2 = spark.sparkContext.textFile(s"s3a://commoncrawl/$fileName").zipWithIndex()
    // val df3 = df2.toDF()
    // println(s"WARC-Target-URI: ${url}")

    // val start = df3.select("_2").where($"_1".contains(s"WARC-Target-URI: ${url}")).collect()

    // if (start.take(1).isEmpty)
    // {
    //   println("Not Found")
    // }
    // else{
    //   val startInt = start(0).toString().substring(1,start(0).toString().length()-1).toInt

    // val end = df3.filter($"_2" > startInt && $"_1"==="WARC/1.0").select($"_2")collect()
    // val endInt = end(0).toString().substring(1,end(0).toString().length()-1).toInt
 
    // val df4=df3.filter($"_2".between(startInt,endInt))
    // df4.show(100,false)
    // }
    
  }
    
  }
  

   case class ColumnarClass (
 url_surtkey: String,
 url: String,
 url_host_name: String,
 url_host_tld: String,
 url_host_2nd_last_part: String,
 url_host_3rd_last_part: String,
 url_host_4th_last_part: String,
 url_host_5th_last_part: String,
 url_host_registry_suffix: String,
 url_host_registered_domain: String,
 url_host_private_suffix: String,
 url_host_private_domain: String,
 url_protocol: String,
 url_port: Int,
 url_path: String,
 url_query: String,
 fetch_time: String,
 fetch_status: Int,
 fetch_redirect: String,
 content_digest: String,
 content_mime_type: String,
 content_mime_detected: String,
 content_charset: String,
 content_languages: String,
 content_truncated: String,
 warc_filename: String,
 warc_record_offset: Int,
 warc_record_length: Int,
 warc_segment: String){}
}