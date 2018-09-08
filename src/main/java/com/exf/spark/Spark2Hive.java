package com.exf.spark;


import java.io.IOException;

import java.sql.Connection;
import java.sql.DriverManager;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.expressions.Window;
import org.datanucleus.store.rdbms.query.ResultMetaDataROF;

import com.exf.Utils.Migrationvar;
import com.exf.apicall.DB2API;
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


		Dataset<Row> second = secondset;

		// original one
		hdfstoringpath = migspark.hdfsPath + "/" + migspark.jdbcTargetDatabasename;
		second.write().mode("append").format("com.databricks.spark.csv").option("header", "false").save(hdfstoringpath);
		// .mode("append").csv(hdfstoringpath);
	
		if(migspark.storageType.toLowerCase().equalsIgnoreCase("db")){
			String allcolumns[] = second.columns();
			String insertcol = "";
			DB2API db = new DB2API();
			int columnCount = db.rsmd.getColumnCount();
			ArrayList<String> colSchema = new ArrayList<String>();
			for (int i = 1; i <= columnCount; i++)
				colSchema.add(String.valueOf(db.rsmd.getColumnTypeName(i)));
				
			for(int i=0;i<allcolumns.length;i++)	
				insertcol += allcolumns[i] +" "+colSchema.get(i).toUpperCase()+", ";

			insertcol = insertcol.substring(0, insertcol.length() - 2);
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
		String hiveallurl = mig.hiveMetastoreURI.split("//")[1];
		String hiveurl = hiveallurl.split(":")[0];
		Connection con = DriverManager.getConnection("jdbc:hive2://"+hiveurl+":10000/"+mig.jdbcTargetDatabasename,"","");
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



}



