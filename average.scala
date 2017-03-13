// Databricks notebook source
var bussiness= sc.textFile("/FileStore/tables/bhv9wv0j1489262645388/business.csv")
var review= sc.textFile("/FileStore/tables/4vo8z2wf1489262685677/review.csv")
var user=sc.textFile("/FileStore/tables/bexocdw61489262747888/user.csv")

var name="Matt J."
// var userdata = user.map(line=>line.split("\\^")).filter(line=>(line(1).equals(name))).map(line=>(line(1),line(0)))//.foreach(println)
// userdata.collect()

//var reviewTab=review.map()

// COMMAND ----------

var reviewdata=review.map(line=>line.split("\\^"))
    .map(line=>(line(2),(line(3).toDouble,1)))
    .reduceByKey((a,b)=>((a._1 + b._1),(a._2 + b._2)))
    .map(line=>(line._1,((line._2._1/line._2._2))))
    .sortBy(-_._2)
    .take(10)
    //reviewdata.collect()

// COMMAND ----------


