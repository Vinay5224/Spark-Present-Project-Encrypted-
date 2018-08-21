package com.exf.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.spark.sql.SparkSession;
import org.glassfish.hk2.utilities.reflection.Logger;

import com.exf.spark.Spark2Hive;
/** 
* The Migrationvar class will loads all the variables
* from the Migration.properties file.
*
*
* @author  K. Vinay Kumar
* 
*/
public class Migrationvar {
	
	public static String hiveMetastoreURI;
	public static String jdbcSourceDatabasename;
	public static String jdbcSourceTablename;
	public static String jdbcTargetDatabasename;
	public static String jdbcTargetTablename;
	public static String hdfsPath;
	public static String textFilepath;
	public static String jdbcURL;
	public static String jdbcUsername;
	public static String jdbcPassword;
	public static String storageType;
	public static String dbType;
	public static String aid;
	public static String apiIP;
	public static String encryptColNames;
	public static SparkSession sparkses;
	public static String migrationTablename;
	
	/**
	 * Initialize all variables from the properties file, The property
	 * file loaded from the same directory.
	 */
	public void loadProp(){ 
		try{
			String path = System.getProperty("user.dir")+"/migration.properties";
		InputStream in = new FileInputStream(new File(//path));
				"/home/exa1/Desktop/migration.properties"));
		Properties properties = new Properties();
		properties.load(in);
			
		hiveMetastoreURI=properties.getProperty("hive.metastore.uris").trim();
		jdbcSourceDatabasename=properties.getProperty("JDBC.source.databasename").trim();
		jdbcSourceTablename=properties.getProperty("JDBC.source.tablename").trim();
		jdbcTargetDatabasename=properties.getProperty("JDBC.target.databasename").trim();
		jdbcTargetTablename=properties.getProperty("JDBC.target.tablename").trim();
		hdfsPath=properties.getProperty("hdfs.path").trim();
		textFilepath=properties.getProperty("text.filepath").trim();
		jdbcURL=properties.getProperty("JDBC.url_Port").trim();
		jdbcUsername=properties.getProperty("JDBC.username").trim(); 
		jdbcPassword=properties.getProperty("JDBC.password").trim();
		storageType=properties.getProperty("storage.type").trim();
		dbType=properties.getProperty("db.type").trim();
		aid=properties.getProperty("ApplicationID").trim();
		apiIP=properties.getProperty("APIJarIP").trim();
		encryptColNames=properties.getProperty("EncryptColumnNames").trim();
		migrationTablename=properties.getProperty("Migration.Tablename","");
		String hiveallurl = hiveMetastoreURI.split("//")[1];
		String hiveurl = hiveallurl.split(":")[0];
		String thrift = hiveMetastoreURI;
		sparkses = SparkSession.builder().master("local").appName("Reading spark files").config("spark.sql.warehouse.dir","hdfs://"+hiveurl+":8020/user/hive/warehouse")
				.config("hive.metastore.uris",thrift ).enableHiveSupport().getOrCreate();

		
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
