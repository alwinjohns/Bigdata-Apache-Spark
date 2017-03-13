// Databricks notebook source
var business= sc.textFile("/FileStore/tables/bhv9wv0j1489262645388/business.csv")
var review= sc.textFile("/FileStore/tables/4vo8z2wf1489262685677/review.csv")
var user=sc.textFile("/FileStore/tables/bexocdw61489262747888/user.csv")

var businessData=business.map(line=>line.split("\\^"))
                        .filter(line=>(line(1).contains("TX")))
                        .map(line=>(line(0),line(1)))

var reviewData=review.map(line=>line.split("\\^"))
                        .map(line=>(line(2),1))
                        .reduceByKey(_+_)
    
var busDF=businessData.toDF("id","address")
var reviewDF=reviewData.toDF("id","count")
var output=reviewDF.join(busDF,"id")
                  .distinct() //added distinct to avoid duplicates
                  .sort($"count".desc)      
display(output)

// COMMAND ----------


