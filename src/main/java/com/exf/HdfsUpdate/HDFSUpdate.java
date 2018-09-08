package com.exf.HdfsUpdate;



import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.log4j.Level;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.glassfish.hk2.utilities.reflection.Logger;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.JSONArray;

import com.exf.Utils.Migrationvar;
import com.exf.apicall.DB2API;
import org.apache.spark.sql.functions;


public class HDFSUpdate {
static Dataset<Row> fin = null;
static JSONObject jsonupdate = new JSONObject();
static Dataset<Row> updatedFile = null;
//main(String[] args)
	public static void HDFSUpdate() throws IOException, ClassNotFoundException, JSONException, ParseException {
		// TODO Auto-generated method stub
		Configuration conf = new Configuration ();
		   conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
           conf.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName());
           conf.addResource(new Path("/etc/hadoop/conf/core-site.xml"));
           conf.addResource(new Path("/etc/hadoop/conf/hdfs-site.xml"));
           conf.addResource(new Path("/etc/hadoop/conf/mapred-site.xml"));
           conf.set("fs.default.name", "hdfs://192.168.0.184:8020");
           conf.set("fs.defaultFS", "hdfs://192.168.0.184:8020");
           //conf.set("hadoop.job.ugi", "remoteUser");
           conf.set("hadoop.ssl.enabled", "false");
          // readFromHDFS("hdfs://192.168.0.184:8020");
           FileSystem fileSystem = FileSystem.get(conf);
           Class.forName("org.apache.hive.jdbc.HiveDriver");
           	SparkSession spark = SparkSession.builder().master("local").appName("Reading spark files").config("spark.sql.warehouse.dir","hdfs://192.168.0.184:8020/user/hive/warehouse")
    				.config("hive.metastore.uris","thrift://192.168.0.184:9083" ).config("spark.sql.caseSensitive","true").enableHiveSupport().getOrCreate();
           	Dataset<Row> headerTable = spark.read().format("jdbc") .option("url", "jdbc:hive2://192.168.0.184:10000/exf_test") .option("dbtable", "uptest1").option("driver", "org.apache.hive.jdbc.HiveDriver").load();
           	String[] allCol = headerTable.columns();
           
           	List<String> allColumn = new ArrayList<String>();
           	for(String s: allCol)
           		allColumn.add(s.split("\\.")[1]);
           	String updatedFilePath = "hdfs://192.168.0.184:8020/user/cloudera/Individual_profile_schema_100.txt";
           	//Updated File
            Dataset<Row> updatedFile = spark.read().format("com.databricks.spark.csv").option("header", "true").load(updatedFilePath).limit(20);
		    int updatedCount = (int) updatedFile.count();
         	
    			String path = "hdfs://192.168.0.184:8020/user/cloudera/exf_test/";
    			//Dataset<Row> header = spark.sql("select * from exf_test.uptest1").limit(0);
    			
    			
        		BufferedReader br = new BufferedReader(new InputStreamReader (fileSystem.open(new Path(updatedFilePath)))); //hdfs://192.168.0.184:8020/user/cloudera/Individual_profile_schema_100.txt" //new FileReader(new File(fileSystem.open(new Path("hdfs://192.168.0.184:8020/user/cloudera/Individual_profile_schema_100.txt")))));
              	List<Integer> columnIndex =new ArrayList<Integer>();
               	String line1="";
               	while((line1=br.readLine())!=null){
    			 String[] colsplit =line1.split(",");
    			 if(allColumn.size() != colsplit.length)
    				 break; //here i have to write a custom exception
    			
    			 for(int i=0;i<allColumn.size();i++){
    					for(int j=0;j<colsplit.length;j++){
    						if(allColumn.get(i).equalsIgnoreCase(colsplit[j])){
    							columnIndex.add(j);
    						}
    					}
    				  }
    			 break;
    		 }
            	//For API column Headers
               	//Note Use allColumn and columnIndex
               	JSONArray arrtxt = new JSONArray();
               	for(int i=0;i<allColumn.size();i++){

    				JSONObject temp1 = new JSONObject();
    				temp1.put("name", allColumn.get(i));
    				temp1.put("tablename", "individual_profile_schema");
    				arrtxt.add(temp1);
               	}
               	
               	//Columns adding it into the file
               	JSONArray jsonrows = new JSONArray();
     		 
     		 while ((line1 = br.readLine()) != null){
     		  	String[] splitrow = line1.split(",");
     		  	int assign = 0;
     		  	//JSONObject temp = new JSONObject();
     		  	LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();
     		  	for (int i : columnIndex) {
     		  			temp.put(String.valueOf(assign), splitrow[i]);
     		  			assign++;
     		  	 }
     		  	jsonrows.add(temp);
     		 }
     		jsonupdate.put("RECORDS", jsonrows);
     		jsonupdate.put("COLUMNS", arrtxt);
     		jsonupdate.put("aid", "6");
     		jsonupdate.put("tid", "0");
     		 //logger.debug(jsonupdate.toString());
           	
     		
     		JSONObject outputjson = DB2API.readJsonFromUrl("http://localhost:45670/path7", jsonupdate);
   			Document doc = Document.parse(outputjson.toString());
   			ArrayList<Document> columns = (ArrayList<Document>) doc.get("COLUMNS");
   			List<String> columnames = new ArrayList<String>();
   			for(int i=0;i<columns.size(); i++){
   				Document doc2 = columns.get(i);
   				columnames.add(doc2.get("name").toString());
   			}
   			
   				List<Row> data2 =new ArrayList<Row>();
   		        List<Row> arrfulldata = new ArrayList<Row>();
   		       
   				
   				ArrayList<Document> records =(ArrayList<Document>) doc.get("RECORDS");
   				for(int i=0;i<records.size(); i++){
   					Document doc2 = records.get(i);
   					Set<String> keys = doc2.keySet();
   					String val ="";
   					 ArrayList<Integer> set=new ArrayList<Integer>();  
   					 for(String s: keys)
   						 set.add(Integer.parseInt(s));
   					Collections.sort(set);
   					for(int keyset : set)
   						val +=doc2.get(String.valueOf(keyset))+",";
   					 val = val.substring(0, val.length()-1);
   					 data2 = Arrays.asList(
   						        RowFactory.create(val.split(",")));
   					 arrfulldata.addAll(data2);
   				}
   				StructType schema = DB2API.createSchema(columnames);
   				updatedFile = spark.createDataFrame(arrfulldata, schema);
   				//Migrationvar.sparkses
   	           	//creating header files for temp
   	           	List<Row> headerdata = new ArrayList<Row>();
   	        	StructType schemaheader = DB2API.createSchema(allColumn);
   	        	Dataset<Row>  header = spark.createDataFrame(headerdata, schemaheader);
   		    //  header.show();
   	      
   	        	
   	        	
   	        	boolean recursive = false;
    			RemoteIterator<LocatedFileStatus> ri = fileSystem.listFiles(new Path(path), recursive);
    			int breakcount =0;
   		      //Partition of HDFS looping here
    			while (ri.hasNext()){
    				String name =ri.next().getPath().getName(); 
    			    if(name.startsWith("_")){
    			    	
    			    }else{
    				String pat = path+name;
    			    System.out.println(pat);           	
           
			    //here replace with partition dataset
			    Dataset<Row> partitionsHDFS = spark.read().format("com.databricks.spark.csv").option("header", "false").load(pat);
			    		//"hdfs://192.168.0.184:8020/user/cloudera/Test/Individual_profile_schema.txt");
			    partitionsHDFS = header.union(partitionsHDFS);
			 //  partition.show();
			    
			   // partition.createOrReplaceTempView("TEM");
			   String pk = "swid";
			      List<Row> val = new ArrayList<Row>();
		           val = updatedFile.select(pk).collectAsList();
		           String primary ="";
		           //partition.show();
		           for(Row r: val){
		        	   primary = r.toString().substring(1, r.toString().length()-1);
		        	  // String temp = pk+" != "+primary;
		        	 // partition.show();
		        	   //partition =  spark.sql("SELECT * FROM TEM WHERE `age` !="+primary); 
		        	   int orgCount = (int) partitionsHDFS.count();
		        	   partitionsHDFS= partitionsHDFS.filter(pk+" != "+primary);
		        	   int dumCount = (int) partitionsHDFS.count();
		        	  
		        	   if(orgCount==dumCount){
		        	   System.out.println("there is no change");
		        	   }else{
		        		   Dataset<Row> newrow = updatedFile.filter(pk+"="+primary);
		        		   partitionsHDFS =partitionsHDFS.union(newrow);
		        		   breakcount++;
		        	   }
		        	   if(updatedCount==breakcount)
		        		   break;
		           }
		           partitionsHDFS.show();
		           //
		           partitionsHDFS.repartition(1).write().mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save(path); //"hdfs://192.168.0.184:8020/user/cloudera/Test/"
		           fileSystem.delete(new Path(pat), true);
		           if(updatedCount==breakcount)
	        		   break;
		           //"hdfs://192.168.0.184:8020/user/cloudera/Test/Individual_profile_schema.txt"
			    }
			}
    			System.out.println("Migration Completed");
           //currently not working on the originial partitions of the encrypted
/*           Dataset<Row> fi = spark.read().format("com.databricks.spark.csv").option("header", "true").load("hdfs://192.168.0.184:8020/user/cloudera/Test/Individual_profile_schema.txt");
           Dataset<Row> sec = spark.read().format("com.databricks.spark.csv").option("header", "true").load("/home/exa1/Documents/Hive_updateDOc/Individual_profile_schema_100.csv").limit(1);
         //  Dataset<Row> header = spark.read().format("com.databricks.spark.csv").option("header", "true").load("/home/exa1/Documents/Hive_updateDOc/Individual_profile_schema_100.csv").limit(0);
           //Dataset<Row> sec = fi.filter("swid!=2");
           fi = header.union(fi);
           
          // Dataset<Row> fina = fi;
          // fina.show();
           fi.foreach((ForeachFunction<Row>) row -> {
        	   int index = row.fieldIndex("swid");
        	   fin = fina.filter("swid!=2");
        	  // fin = fin.union(sec);
        	 //  fin.show();
        	   //fin.write().mode("overwrite").option("header", "false").save("hdfs://192.168.0.184:8020/user/cloudera/Test/Individual_profile_schema.txt");
           });


           List<Row> val = new ArrayList<Row>();
           val = fi.select("swid").collectAsList();
           String primary ="";
           for(Row r: val)
        	   primary = r.toString().substring(1, r.toString().length()-1);*/
          
           //Update code
      /*     Dataset<Row> sec = spark.read().format("com.databricks.spark.csv").option("header", "true").load("/home/exa1/Documents/Hive_updateDOc/Individual_profile_schema_100.csv").filter("swid=2");
           Dataset<Row> third = fi.filter("swid!=2").union(sec);
           third.createOrReplaceTempView("DUMMY");
           third.filter("swid=2").show(2);
           System.out.println(third.count());*/
	}

}
