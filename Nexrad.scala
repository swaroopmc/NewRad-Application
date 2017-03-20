import org.apache.spark.SparkContext
	import org.apache.spark.SparkContext._
	import org.apache.spark.SparkConf
	import org.apache.spark.sql._
  import java.sql.DriverManager
	import java.sql.SQLException
	import com.ibm.db2.jcc._
	import org.apache.log4j.Logger
	import org.apache.log4j.Level
	import java.sql.Connection
	import org.apache.commons.csv.CSVFormat
	import java.io._
  import com.databricks.spark.csv._
	import org.apache.commons.csv._
	

	object Nexrad {
	  def main(args: Array[String]) {
	   
	    Logger.getLogger("org").setLevel(Level.OFF)
	    Logger.getLogger("akka").setLevel(Level.OFF)
	    val conf1 = new SparkConf().setAppName("Nexrad")
	    val sc = new org.apache.spark.SparkContext
	    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	


	    /* This part loads a file from amazon s3 with bucket name :amazon-noaa-radardata which contains radar data from california region(11 radars data for year 2015-01 to around 2016-03, creates a dataaframe */
	    /* the csv files are fetched from NOAA website https://www.ncdc.noaa.gov/nexradinv/   and stored in Amazon S3 bucket */
	    val df = sqlContext.load("com.databricks.spark.csv", Map("path" -> "s3n://AccessKey:SecretKey@/amazon-noaa-radardata/*", "header" -> "true")) 
	    println("Schema definitions of loaded file")
	    println(df.printSchema())
	    val tabColumns = df.columns
	    println("Columns of loaded file:\n" + tabColumns)
	    val tabColumnDef = df.dtypes
	    println("data types of loaded file:\n" + tabColumnDef)
	    val jdbcClassName="com.ibm.db2.jcc.DB2Driver"
	    val url="jdbc:db2://dashdb-entry-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB:sslConnection=true;" 
	    val user="dash5283" 
	    val password="**********" 
	    Class.forName(jdbcClassName)
	    val connection = DriverManager.getConnection(url, user, password)
	    val stmt = connection.createStatement()
	    
	

	stmt.executeUpdate("CREATE TABLE NEXRAD(" +
	    "STATION  string," +
	     "NAME  string," +
	     "LATITUDE  string," +
	      "LONGITUDE string," +
	       "ELEVATION: string," +
	        "DATE  string," +
	         "AWND string," +
	          "AWND_ATTRIBUTES string," +
	           "EMNT string," +
	            "EMNT_ATTRIBUTES string," +
	             "EMSN: string," +
	              "EMSN_ATTRIBUTES string," +
	               "EMXP: string," +
	                "EMXP_ATTRIBUTES string," +
	                 "EMXT string," +
	                  "EMXT_ATTRIBUTES string," +
	                   "PRCP string," + 
	                   "PRCP_ATTRIBUTES string," +
	                    "SNOW string," +
	                     "SNOW_ATTRIBUTES string," +
	                      "TAVG string," +
	                       "TAVG_ATTRIBUTES string," +
	                        "TMAX string," +
	                         "TMAX_ATTRIBUTES string," +
	                          "WDF5 string," +
	                           "WDF5_ATTRIBUTES string)")
	

	    stmt.close()
	    connection.commit()
	    connection.close()
	      
	        
	             
	    val jdbcdf = sqlContext.load("jdbc", Map("url" -> "jdbc:db2://dashdb-entry-yp-dal09-08.services.dal.bluemix.net:50000/BLUDB:user=dash5283;password=c9a8a88724ff;sslConnection=true;", "dbtable" -> "SYSCAT.TABLES", "driver" -> "com.ibm.db2.jcc.DB2Driver"))
	    jdbcdf.registerTempTable("jdbcdf")
	    println("User's Schema tables in dashDB: \n") 
	    val getNewTab = sqlContext.sql("SELECT TABSCHEMA,TABNAME FROM jdbcdf WHERE TABSCHEMA='CMPE297' AND TABNAME LIKE '%NEXRAD%'")
	    println(getNewTab.show())
	

	    /* for simple example....filter data for needed precipitation vals > 2.0 */
	    println("total row count of the data frame created from radar data csv file is:" + df.count())
	    val filter_df = df.filter("PRCP > 2.5")
	    println("trimmed data set:" + filter_df.count())
	    val trim_df = filter_df.limit(5000)
	    println("limiting the insert to be: " + trim_df.count() + " rows")
	
	    val jdbcurl  = "jdbc:db2://<ibmserverhostip>:50000/BLUDB:user=dash5283;password=*******;sslConnection=true;"
	
	    println("Inserting data set into NEXRAD Table")
	    trim_df.insertIntoJDBC(jdbcurl, "NEXRAD", false)
	    val GetFinalDataCount = sqlContext.jdbc(url = jdbcurl,"NEXRAD")
	    println("Total Number of Rows inserted into table:" + GetFinalDataCount.count())
	 	

	 }
	}
