package com.exf.HdfsUpdate;


import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import org.apache.log4j.Logger;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.StructType;
import org.json.JSONException;
import org.json.JSONObject;
import com.exf.Utils.Migrationvar;
import com.exf.apicall.DB2API;
/**
 * 
 * This class iterates the updated File from the HDFS and updates the Hive 
 * External Table partitions
 * 
 * @author KAMBAM VINAY KUMAR
 *
 */

public class HDFSUpdate {
static Dataset<Row> fin = null;
static JSONObject jsonupdate = new JSONObject();
static Dataset<Row> updatedFile = null;

static Logger logger = Logger.getLogger(HDFSUpdate.class);

/**
 * This Method takes the dataset from the {@link DB2API} class for the updated
 * file then the hive external table partitions are compared with the updated file
 * and appended to the hive external path. 
 * 
 * @param updated
 * @throws IOException
 * @throws ClassNotFoundException
 * @throws JSONException
 * @throws ParseException
 */
	public static void HDFSUpdating(Dataset<Row> updated) throws IOException, ClassNotFoundException, JSONException, ParseException {
		

           Class.forName("org.apache.hive.jdbc.HiveDriver");
   	           	List<Row> headerdata = new ArrayList<Row>();
   	        	StructType schemaheader = DB2API.createSchema(Migrationvar.allColumn);
   	        	Dataset<Row>  header = Migrationvar.sparkses.createDataFrame(headerdata, schemaheader);
   		    //  header.show();
   	        	Dataset<Row> updatedFile = updated;
   	        	int updatedCount = (int) updatedFile.count();
   	        	
   	        	
   	        	boolean recursive = false;
    			RemoteIterator<LocatedFileStatus> ri = Migrationvar.fileSystem.listFiles(new Path(Migrationvar.externalHiveTablepath), recursive);
    			int breakcount =0;
   		      //Partition of HDFS looping here
    			while (ri.hasNext()){
    				String name =ri.next().getPath().getName(); 
    			    if(name.startsWith("_")){
    			    	
    			    }else{
    				String pat = Migrationvar.externalHiveTablepath+name;
    			    logger.info(pat);           	
           
			    //here replace with partition dataset
			    Dataset<Row> partitionsHDFS = Migrationvar.sparkses.read().format("com.databricks.spark.csv").option("header", "false").load(pat);
			    partitionsHDFS = header.union(partitionsHDFS);
			 
			    
			  
			  // String pk = "swid";
			      List<Row> val = new ArrayList<Row>();
		           val = updatedFile.select(Migrationvar.PrimaryKey).collectAsList();
		           String primary ="";
		     
		           for(Row r: val){
		        	   primary = r.toString().substring(1, r.toString().length()-1);
		 
		        	   int orgCount = (int) partitionsHDFS.count();
		        	   partitionsHDFS= partitionsHDFS.filter(Migrationvar.PrimaryKey+" != "+primary);
		        	   int dumCount = (int) partitionsHDFS.count();
		        	  
		        	   if(orgCount==dumCount){
		        	   logger.info("There is no change");
		        	   }else{
		        		   Dataset<Row> newrow = updatedFile.filter(Migrationvar.PrimaryKey+"="+primary);
		        		   partitionsHDFS =partitionsHDFS.union(newrow);
		        		   breakcount++;
		        	   }
		        	   if(updatedCount==breakcount)
		        		   break;
		           }
		           partitionsHDFS.show();
		           //
		           partitionsHDFS.repartition(1).write().mode(SaveMode.Append).format("com.databricks.spark.csv").option("header", "false").save(Migrationvar.externalHiveTablepath); //"hdfs://192.168.0.184:8020/user/cloudera/Test/"
		           Migrationvar.fileSystem.delete(new Path(pat), true);
		           if(updatedCount==breakcount)
	        		   break;
		          
			    }
			}
    			logger.info("Migration Completed");
       
	}

}
