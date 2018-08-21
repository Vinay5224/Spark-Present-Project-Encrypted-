package com.exf.spark;


import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Properties;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;

import com.exf.Utils.Migrationvar;
import com.exf.apicall.Txt2API;

/**
* The Spark2Hive class will gets the encrypted column dataset 
* from DB2API/Txt2API class and then remaining columns will be
* read from spark-jdbc connection and will join both the datasets.
* The final joined dataset will write to the hdfs, from hdfs
* hive external table will be created.
*
*
* @author  K. Vinay Kumar
* 
*/

public class Spark2Hive {


	static String hdfstoringpath;
	static Logger logger = Logger.getLogger(Spark2Hive.class);
	/**
	 * This method will load the plain text columns and joins with encrypted columns, then
	 * the final dataset is loaded into the hdfs.
	 * 
	 * @param secondset Encrypted columns data 
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void hadoop(Dataset<Row> secondset) throws IOException, ClassNotFoundException, SQLException {
		
		Migrationvar migspark = new Migrationvar();
		Dataset<Row> firstset = null;
		Logger.getLogger("org").setLevel(Level.OFF);
		Logger.getLogger("akka").setLevel(Level.OFF);
		String dbtype = migspark.dbType.toLowerCase();
		String encryptcolumns = migspark.encryptColNames;
		if (dbtype.equalsIgnoreCase("mysql")) {

			Properties prop = new Properties();
			prop.put("user", migspark.jdbcUsername);
			prop.put("password", migspark.jdbcPassword);
			firstset = migspark.sparkses.read().jdbc(
					"jdbc:" + dbtype + "://" + migspark.jdbcURL + "/" + migspark.jdbcSourceDatabasename,
					migspark.jdbcSourceTablename, prop);
			String finalkeys = finalcolumns(firstset, encryptcolumns);
			firstset.createOrReplaceTempView("TEST123");
			firstset = migspark.sparkses.sql("SELECT " + finalkeys + " FROM TEST123");
			firstset = firstset.withColumn("id", functions.row_number().over(Window.orderBy(finalkeys.split(",")[0])));

		} else if (dbtype.equalsIgnoreCase("text")) {

			firstset = migspark.sparkses.read().format("com.databricks.spark.csv").option("header", "true")
					.option("delimiter", ",").load(migspark.textFilepath).na().fill("");
			String finalkeys = finalcolumns(firstset, encryptcolumns);
			firstset.createOrReplaceTempView("TEST123");
			firstset = migspark.sparkses.sql("SELECT " + finalkeys + " FROM TEST123");
			firstset = firstset.withColumn("id", functions.row_number().over(Window.orderBy(finalkeys.split(",")[0])));

		} else {
			// hive
			firstset = migspark.sparkses
					.sql("SELECT * FROM " + migspark.jdbcSourceDatabasename + "." + migspark.jdbcSourceTablename);
			String finalkeys = finalcolumns(firstset, encryptcolumns);
			firstset.createOrReplaceTempView("TEST123");
			firstset = migspark.sparkses.sql("SELECT " + finalkeys + " FROM TEST123");
			firstset = firstset.withColumn("id", functions.row_number().over(Window.orderBy(finalkeys.split(",")[0])));
		}

		Dataset<Row> second = secondset;
		second = second.withColumn("id", functions.row_number().over(Window.orderBy(encryptcolumns.split(",")[0])));

		Dataset<Row> finalset = firstset.join(second, firstset.col("id").equalTo(second.col("id")), "outer").drop("id");
		finalset.show();

		// original one
		hdfstoringpath = migspark.hdfsPath + "/" + migspark.jdbcTargetDatabasename;
		finalset.write().mode("append").format("com.databricks.spark.csv").option("header", "false").save(hdfstoringpath);
		// .mode("append").csv(hdfstoringpath);
		String allcolumns[] = finalset.columns();
		String insertcol = "";
		for (String s : allcolumns)
			insertcol += s + " String, ";

		insertcol = insertcol.substring(0, insertcol.length() - 2);
		if(migspark.storageType.toLowerCase().equalsIgnoreCase("db")){
			Hadoop2hive(insertcol);	
		}
		else{
		//	Hadoop2hivedrop(insertcol);
			logger.info("The Source is Text File");
		}
		
		logger.info("Migariton Completed");
	}
	
	/**
	 * 
	 * @param allcolumns All column names to created in the Hive with schema
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 */
	public static void Hadoop2hive(String allcolumns) throws ClassNotFoundException, SQLException{
		Migrationvar mig = new Migrationvar();
		String path ="'"+mig.hdfsPath+"/"+mig.jdbcTargetDatabasename+"/'";
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con = DriverManager.getConnection("jdbc:hive2://192.168.0.184:10000/"+mig.jdbcTargetDatabasename,"","");
		logger.info("Connection obtained to hive");
		//impala		
		//Connection con = DriverManager.getConnection("jdbc:hive2://192.168.0.184:21050/individual_profile_schema_tar;auth=noSasl","","");
		Statement stmt = con.createStatement();
		allcolumns = "CREATE EXTERNAL TABLE "+mig.jdbcTargetDatabasename+"."+mig.jdbcTargetTablename+" ("+allcolumns+") ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' STORED AS TEXTFILE LOCATION "+path;
		logger.debug(allcolumns);
		stmt.execute(allcolumns);
		//loadstmt.execute(sql);
	    con.close();
	    mig.sparkses.close();

	}

	/**
	 * What are the encrypted columns will be removed and remaining,
	 * columns are passed to the query in the dataset.
	 * 
	 * @param coldataset
	 * @param encCol
	 * @return
	 */
	public static String finalcolumns(Dataset<Row> coldataset, String encCol){
		Dataset<Row> colnames = coldataset;
		String[] allcolumns = colnames.columns();
		String[] encol = encCol.split(","); 
		String finalcolumns = "";
		HashSet<String> allcol = new HashSet<String>();
		for(String s : allcolumns)
			allcol.add(s);
		for(String s: encol)
			allcol.remove(s);
		
		for(String keys: allcol)
			finalcolumns+=keys+", ";
		
		finalcolumns=finalcolumns.substring(0,finalcolumns.length()-2);
		return finalcolumns;
	}

}



