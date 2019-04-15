package Package1

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._

object Simple {

  
  def main(args: Array[String]){
    
    
    val conf = new SparkConf().setAppName("simple").setMaster("local").set("spark.executor.memory","5g")
    val sc = new org.apache.spark.SparkContext(conf)
    val sqlsc = new org.apache.spark.sql.SQLContext(sc)

   
    var mydata = sqlsc.read.format("csv").option("header","true").option("mode","failfast").option("path","C:\\Users\\sharnanu\\Desktop\\Admission.csv").load()
    
    //mydata.printSchema()
    val table = mydata.createOrReplaceTempView("Student")
    
    //val Sqldf = sqlsc.sql("Select * from Student").show() 
    //mydata.select("First Name","City","State","Pin code","Phone","Fax").show()
    //mydata.groupBy("Gender").count().show()
    //mydata.select("Admission No").show()
    //val Sqldf = sqlsc.sql("show Columns from Student").show()
    //val sqldf = sqlsc.sql("Select distinct(Gender) from Student").show()
    //val Sqldf = sqlsc.sql("Select First_Name,Gender from Student where Gender = 'Male' ").show() 
    //val sqldf = sqlsc.sql("select First_Name,Gender,Admission_Year from Student where Admission_Year < 2014 ").orderBy(asc("Admission_Year"))
    //val rddsql = sqldf.show()
    
    //val sqldf = sqlsc.sql("select First_Name,Gender,Admission_Year from Student where Admission_Year < 2015 and Gender = 'Male' ").orderBy(asc("Admission_Year"))
    //val rddsql = sqldf.rdd.saveAsTextFile("C:\\Users\\sharnanu\\Desktop\\Admission1.txt")
    //val Sqldf = sqlsc.sql("Select Max(Admission_Year) from Student").show() 
     
    //val Sqldf = sqlsc.sql("Select First_Name from Student where First_Name like '_a%' ")
    //val rddsql = Sqldf.rdd.saveAsTextFile("C:\\Users\\sharnanu\\Desktop\\Admission3.txt")
    
    //val Sqldf = sqlsc.sql("Select count(First_Name) from Student where First_Name in ('Aarav','Abeer') ")
    //val rddsql = Sqldf.rdd.saveAsTextFile("C:\\Users\\sharnanu\\Desktop\\Admission5.txt")
    
    val Sqldf = sqlsc.sql("Select First_Name,Admission_Year from Student where Admission_Year between 2015 and 2017  ")
    val rddsql = Sqldf.rdd.saveAsTextFile("C:\\Users\\sharnanu\\Desktop\\Admission7.txt")
    
  }
}