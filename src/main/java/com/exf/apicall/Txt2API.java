package com.exf.apicall;


import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.JSONArray;
import com.exf.Utils.*;
/**
* The Txt2API class will reads the textfile path from the properties file
* from Migrationvar class. It reads all the columns and encrypts only the 
* particular columns using the API(path7) and puts it into a Spark Dataset.  
* At last the Spark Dataset is passed to the Spark2Hive class.
*
*
* @author  K. Vinay Kumar
* 
*/

public class Txt2API {
static JSONObject jsontxtarr = new JSONObject();
static Dataset<Row> txtrowencrypt;
static Logger logger = Logger.getLogger(Txt2API.class);
/**
 * This method reads the textfile and encrypts the particular columns using API(path7) and saves into the 
 * Spark Dataset and the sends that dataset to the Spark2Hive class, checks the text file columns from HDFS and Hive columns
 *  are equal are not.
 * 
 * @throws IOException
 * @throws JSONException
 * @throws ClassNotFoundException
 * @throws SQLException
 * @throws ParseException
 */
	public static void textfilesrc() throws IOException, JSONException, ClassNotFoundException, SQLException, ParseException {
		// TODO Auto-generated method stub
		Migrationvar migvar = new Migrationvar();
	//	JSONArray arrtxt = new JSONArray();
		logger.info("File is Reading from :"+migvar.InserttextFilepath);
		BufferedReader br = null;

		if(Migrationvar.ACIDtype.equalsIgnoreCase("update")){
			br = new BufferedReader(new InputStreamReader (Migrationvar.fileSystem.open(new Path(Migrationvar.updateFilePath))));
		}else if(Migrationvar.ACIDtype.equalsIgnoreCase("insert")){
		//it goes to the text file 
			br = new BufferedReader(new InputStreamReader (Migrationvar.fileSystem.open(new Path(Migrationvar.InserttextFilepath))));
		}else{
			logger.info("No Option is Selected in Properties File");
			System.exit(0);
		}
		//BufferedReader 
	
		
		 List<Integer> columnIndex =new ArrayList<Integer>();
        	String line1="";
        	while((line1=br.readLine())!=null){
			 String[] colsplit =line1.split(",");
			 if(Migrationvar.allColumn.size() != colsplit.length){
				 logger.info("Columns are Mismatching");
					System.exit(0);
				 break; //here i have to write a custom exception
			 }
			
			 for(int i=0;i<Migrationvar.allColumn.size();i++){
					for(int j=0;j<colsplit.length;j++){
						if(Migrationvar.allColumn.get(i).equalsIgnoreCase(colsplit[j])){
							columnIndex.add(j);
						}
					}
				  }
			 break;
		 }
     
        	JSONArray arrtxt = new JSONArray();
        	for(int i=0;i<Migrationvar.allColumn.size();i++){

				JSONObject temp1 = new JSONObject();
				temp1.put("name", Migrationvar.allColumn.get(i));
				temp1.put("tablename", Migrationvar.migrationTablename);
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

		 jsontxtarr.put("RECORDS", jsonrows);
		 jsontxtarr.put("COLUMNS", arrtxt);
		 jsontxtarr.put("aid", migvar.aid);
		 jsontxtarr.put("tid", "0");
		 logger.debug(jsontxtarr.toString());
		 
		 DB2API.APICall(jsontxtarr);

		 
	}

}

