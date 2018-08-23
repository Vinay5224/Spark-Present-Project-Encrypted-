package com.exf.apicall;


import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.simple.JSONArray;

import com.exf.spark.*;
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
 * Spark Dataset and the sends that dataset to the Spark2Hive class.
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
		if(migvar.encryptColNames.endsWith(","))
			migvar.encryptColNames = migvar.encryptColNames.substring(0, migvar.encryptColNames.length()-1);
		String columnames2encrypt = migvar.encryptColNames;
		JSONArray arrtxt = new JSONArray();
		String[] splittingcol = columnames2encrypt.split(",");
		for(int i=0;i<splittingcol.length;i++){
			JSONObject temp1 = new JSONObject();
			temp1.put("name", splittingcol[i]);
			temp1.put("tablename", migvar.migrationTablename);
			arrtxt.add(temp1);
		}
		logger.info("File is Reading from :"+migvar.textFilepath);
		BufferedReader br = new BufferedReader(new FileReader(new File(migvar.textFilepath)));
		  // this will read the first line
		//column names		
		 String line1=null;
		 List<Integer> index =new ArrayList<Integer>();
		 while((line1=br.readLine())!=null){
			 String[] colsplit =line1.split(",");
			 for(int i=0;i<colsplit.length;i++){
				for(int j=0;j<splittingcol.length;j++){
					if(splittingcol[j].equalsIgnoreCase(colsplit[i])){
						index.add(i);
					}
							
				}
			 }
			 break;
		 }
	JSONArray jsonrows = new JSONArray();
		//Taking particular columns from the text file 
		 while ((line1 = br.readLine()) != null){
		  	String[] splitrow = line1.split(",");
		  	int assign = 0;
		  	//JSONObject temp = new JSONObject();
		  	LinkedHashMap<String, Object> temp = new LinkedHashMap<String, Object>();
			 for (int i : index) {
		  			temp.put(String.valueOf(assign), splitrow[i]);
		  			assign++;
		  	 }
		  	jsonrows.add(temp);
		 }
		 //preparing path7 api call
		 jsontxtarr.put("RECORDS", jsonrows);
		 jsontxtarr.put("COLUMNS", arrtxt);
		 jsontxtarr.put("aid", migvar.aid);
		 jsontxtarr.put("tid", "0");
		 logger.debug(jsontxtarr.toString()+"\n"+index.size());
		 
		 DB2API db2 = new DB2API();
		 JSONObject outputjson = db2.readJsonFromUrl("http://"+migvar.apiIP+":45670/path7", jsontxtarr);
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
				StructType schema = db2.createSchema(columnames);
			    txtrowencrypt = migvar.sparkses.createDataFrame(arrfulldata, schema);
			    Spark2Hive rdtxt =new Spark2Hive();
			    rdtxt.hadoop(txtrowencrypt);
		 
		 
	}

}

