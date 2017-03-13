// Databricks notebook source
var business= sc.textFile("/FileStore/tables/bhv9wv0j1489262645388/business.csv")
var review= sc.textFile("/FileStore/tables/4vo8z2wf1489262685677/review.csv")
var user=sc.textFile("/FileStore/tables/bexocdw61489262747888/user.csv")

var reviewData = review.map(line=>line.split("\\^"))
    .map(line=>(line(1),1))
    .reduceByKey(_+_)
    .sortBy(-_._2)
    //.take(10)
    //.foreach(println)
var userData = user.map(line=>line.split("\\^"))
    .map(line=>(line(0),line(1)))

var reviewDF=reviewData.toDF("id","count").limit(10)
var userDF=userData.toDF("id","name")
var output = userDF.join(reviewDF,"id").sort($"count".desc)
display(output)


// COMMAND ----------


