package com.exf.Utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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
	public static String InserttextFilepath;
	public static String updateFilePath;
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
	public static FileSystem fileSystem;
	public static String externalHiveTablepath;
	public static String hiveURL;
	public static String ACIDtype;
	public static Dataset<Row> headerTable;
	public static List<String> allColumn = new ArrayList<String>();
	public static String PrimaryKey;
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
		InserttextFilepath=properties.getProperty("Insert.filepath").trim();
		updateFilePath=properties.getProperty("Update.filepath").trim();
		jdbcURL=properties.getProperty("JDBC.url_Port").trim();
		jdbcUsername=properties.getProperty("JDBC.username").trim(); 
		jdbcPassword=properties.getProperty("JDBC.password").trim();
		storageType=properties.getProperty("storage.type").trim().toLowerCase();
		dbType=properties.getProperty("db.type").trim().toLowerCase();
		aid=properties.getProperty("ApplicationID").trim();
		apiIP=properties.getProperty("APIJarIP").trim();
		//encryptColNames=properties.getProperty("EncryptColumnNames").trim();
		migrationTablename=properties.getProperty("Migration.Tablename","");
		externalHiveTablepath=properties.getProperty("external.hiveTablePath").trim();
		ACIDtype=properties.getProperty("ACID.Type").trim().toLowerCase();
		PrimaryKey=properties.getProperty("PrimaryKey").trim();
		String hiveallurl = hiveMetastoreURI.split("//")[1];
		hiveURL = hiveallurl.split(":")[0];
		String thrift = hiveMetastoreURI;
		
		
		//Spark Session
		sparkses = SparkSession.builder().master("local").appName("Reading spark files").getOrCreate();//.config("spark.sql.warehouse.dir","hdfs://"+hiveURL+":8020/user/hive/warehouse").
				//.config("hive.metastore.uris",thrift ).enableHiveSupport().getOrCreate();

		
		//Hadoop Configuration
		
		Configuration conf = new Configuration ();
		conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
        conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
        conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
        conf.set("fs.default.name", "hdfs://"+hiveURL+":8020");
        conf.set("fs.defaultFS", "hdfs://"+hiveURL+":8020");
        conf.set("hadoop.ssl.enabled", "false");

        fileSystem = FileSystem.get(conf);
        
        
        
        //Header Table
       	headerTable = Migrationvar.sparkses.read().format("jdbc") .option("url", "jdbc:hive2://"+Migrationvar.hiveURL+":10000/"+Migrationvar.jdbcSourceDatabasename) .option("dbtable", Migrationvar.jdbcSourceTablename).option("driver", "org.apache.hive.jdbc.HiveDriver").load();
       	String[] allCol = headerTable.columns();
       
       	//List<String> allColumn = new ArrayList<String>();
       	for(String s: allCol)
       		allColumn.add(s.split("\\.")[1].toLowerCase());
	
		
		}catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
}
