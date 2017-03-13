// Databricks notebook source
var business= sc.textFile("/FileStore/tables/bhv9wv0j1489262645388/business.csv")
var review= sc.textFile("/FileStore/tables/4vo8z2wf1489262685677/review.csv")
var user=sc.textFile("/FileStore/tables/bexocdw61489262747888/user.csv")

var businessData = business.map(line=>line.split("\\^"))
    .filter(line=>line(1).contains("Stanford"))
    .map(line=>(line(0)))
    //.collect()
var reviewData = review.map(line=>line.split("\\^"))
    .map(line=>(line(1),line(2),line(3)))

var businessDF = businessData.toDF("Bus_id")
var reviewDF = reviewData.toDF("user id","Bus_id","rating")

var output = reviewDF.join(businessDF,"Bus_id").distinct().select("user id","rating")
display(output)

// COMMAND ----------


