// Databricks notebook source
var business= sc.textFile("/FileStore/tables/bhv9wv0j1489262645388/business.csv")
var review= sc.textFile("/FileStore/tables/4vo8z2wf1489262685677/review.csv")
var user=sc.textFile("/FileStore/tables/bexocdw61489262747888/user.csv")

var reviewData = review.map(line=>line.split("\\^"))
    .map(line=>(line(2),(line(3).toDouble,1)))
    .reduceByKey((a,b)=>((a._1 + b._1),(a._2 + b._2)))
    .map(line=>(line._1,((line._2._1/line._2._2))))
    .sortBy(-_._2)
    //.take(10)    //take returns an Arrary instead of RDD, so can't convert to DF

var businessData = business.map(line=>line.split("\\^"))
    .map(line=>(line(0),line(1),line(2)))
    
var reviewDF=reviewData.toDF("id","rating")
    .limit(10)
var businessDF=businessData.toDF("id","address","category")

var output=reviewDF.join(businessDF,"id")
display(output)



// COMMAND ----------


