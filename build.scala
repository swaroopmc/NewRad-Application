name := “Nexrad” 

version := “1.6.3” 

scalaVersion := "2.10” 

libraryDependencies ++= Seq( 

"org.apache.spark" %% "spark-core" % "1.5.0”, 

"com.databricks" % "spark-csv_2.11" % “1.5.0”, 

"org.apache.commons" % "commons-csv" % “1.4”

) 

javacOptions ++= Seq("-source", "1.7")
