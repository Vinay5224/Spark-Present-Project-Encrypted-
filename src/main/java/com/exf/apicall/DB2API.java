package com.exf.apicall;


import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Reader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.bson.Document;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONException;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import com.exf.Utils.*;

import com.exf.spark.*;
import com.github.wnameless.json.flattener.JsonFlattener;
/**
* The DB2API class will checks the dbtype from properties file,
* if it's value is 'DB' then it creates a connection to the database and 
* encrypts the particular columns from the table using the API(path7) 
* and puts it into a Spark Dataset. If dbtype is 'text' then it calls 
* the Txt2API class. At last the Spark Dataset is passed to the 
* Spark2Hive class.
*
*
* @author  K. Vinay Kumar
* 
*/
public class DB2API {

	static PreparedStatement stmt = null;
	static ResultSet rs = null;
	static ResultSetMetaData rsmd = null;
	static JSONObject json = new JSONObject();
	static JSONArray jsonarray = new JSONArray();
	static Map<String, Object> flattenJson;
	public static  Dataset<Row> sentenceDataFrame2;
	static Logger logger = Logger.getLogger(DB2API.class);
	public static void main(String[] args) throws ClassNotFoundException, SQLException, IOException, JSONException, ParseException, java.text.ParseException {
		
	logger.info("Execution Started");
	ConnectionProcess();

	}

	/**
	 * Connects to the Database and selects the encrypted columns to be encrypted 
	 * and creates a json input for the API(path7).
	 * 
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws JSONException
	 * @throws ParseException
	 * @throws java.text.ParseException
	 * @throws NullPointerException
	 * @throws SQLException
	 */
	@SuppressWarnings("null")
	public static void ConnectionProcess() throws ClassNotFoundException, IOException, JSONException, ParseException, java.text.ParseException, NullPointerException, SQLException {
	Migrationvar migvariable = new Migrationvar();
	migvariable.loadProp();
		if(migvariable.storageType.toLowerCase().equalsIgnoreCase("db")){
		Class.forName("com.mysql.jdbc.Driver");
		Class.forName("org.apache.hive.jdbc.HiveDriver");
		Connection con=null;
		String dbtype = migvariable.dbType.toLowerCase();
		logger.info(dbtype+" is initialized");
		if(migvariable.dbType.toLowerCase().equalsIgnoreCase("mysql")){
			migvariable.dbType = migvariable.dbType.toLowerCase();
		
		logger.debug("Connection String");	
		 con = DriverManager.getConnection("jdbc:"+migvariable.dbType+"://"+migvariable.jdbcURL+"/"+migvariable.jdbcSourceDatabasename, migvariable.jdbcUsername,
				migvariable.jdbcPassword);
		}else{
			//it direclty goes to the hive
			String hiveallurl = migvariable.hiveMetastoreURI.split("//")[1];
			String hiveurl = hiveallurl.split(":")[0];
			logger.debug("Connection String");
		 con = DriverManager.getConnection("jdbc:hive2://"+hiveurl+":10000/"+migvariable.jdbcSourceDatabasename,"","");
		}
		
		if(migvariable.encryptColNames.endsWith(","))
			migvariable.encryptColNames = migvariable.encryptColNames.substring(0, migvariable.encryptColNames.length()-1);
		String sql = "select "+migvariable.encryptColNames+" from "+migvariable.jdbcSourceDatabasename+"."+migvariable.jdbcSourceTablename; //limit 10";
		stmt = con.prepareStatement(sql);
		rs = stmt.executeQuery();
		rsmd = rs.getMetaData();
		int rscount = rsmd.getColumnCount();
		JSONArray jsonarr2 = new JSONArray();
		while (rs.next()) {
			int assign = 0;
			JSONObject temp = new JSONObject();

			for (int i = 1; i <= rscount; i++) {
				temp.put(String.valueOf(assign), rs.getObject(i));
				assign++;

			}

			jsonarray.add(temp);

		}
		for (int j = 1; j <= rscount; j++) {
			JSONObject temp1 = new JSONObject();
			temp1.put("name", rsmd.getColumnName(j));
			temp1.put("tablename", migvariable.migrationTablename);
			jsonarr2.add(temp1);
		}
		json.put("RECORDS", jsonarray);
		json.put("COLUMNS", jsonarr2);
		json.put("aid", migvariable.aid);
		json.put("tid", "0");
		logger.debug(json);
		APICall();
		}else{
			
			//it goes to the text file 
			Txt2API txtapi = new Txt2API();
			txtapi.textfilesrc();
			
		}
		


	}
	/**
	 * This method calls the API and the API output is parsed and  
	 * writting it into the spark dataset.
	 * 	
	 */
	public static void APICall(){
		JSONObject jsonout=null;
		try{
			jsonout = readJsonFromUrl("http://"+Migrationvar.apiIP+":45670/path7", json);
		logger.debug(jsonout); 
		}catch(IOException | java.text.ParseException e){
			e.printStackTrace();
		}
	
		Document doc = Document.parse(jsonout.toString());
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
			//List<String> val = new ArrayList<String>();
			for(String keyset : keys)
				val +=doc2.get(keyset)+",";
	
			 data2 = Arrays.asList(
				        RowFactory.create(val.split(",")));
			 arrfulldata.addAll(data2);

		}

		StructType schema = createSchema(columnames);
			        sentenceDataFrame2 = Migrationvar.sparkses.createDataFrame(arrfulldata, schema);
			        Spark2Hive rdf =new Spark2Hive();
			        try {
						rdf.hadoop(sentenceDataFrame2);
					} catch (ClassNotFoundException | IOException | SQLException e) {
						e.printStackTrace();
					}
	}
	/**
	 * This method calls the API and returns the JSONObject as output. 
	 * 
	 * @param url api path
	 * @return JSONObject json
	 * @throws IOException
	 * @throws JSONException
	 * @throws java.text.ParseException
	 */
	public static JSONObject readJsonFromUrl(String url, JSONObject jsonip) throws IOException, JSONException, java.text.ParseException {
		//InputStream is = new URL(url).openStream();
		URL object = new URL(url);
		HttpURLConnection con = (HttpURLConnection) object.openConnection();
		try {
			
			con.setDoOutput(true);
			con.setDoInput(true);
			con.setRequestProperty("Content-Type", "application/json");
			con.setRequestProperty("Accept", "application/json");
			con.setRequestMethod("POST");
			 OutputStream os = con.getOutputStream();
	            os.write(jsonip.toString().getBytes());
	            os.flush();
			BufferedReader rd = new BufferedReader(new InputStreamReader(con.getInputStream()));
			String jsonText = readAll(rd);
			JSONObject json = new JSONObject(jsonText);
			return json;
		} finally {
			con.disconnect();
		}
	}
	/**
	 * This method reads the API output as a Reader and reads line by and converts
	 * to the String.
	 * @param rd Reader from the API output
	 * @return String sb
	 * @throws IOException
	 */
	public static String readAll(Reader rd) throws IOException {
		StringBuilder sb = new StringBuilder();
		int cp;
		while ((cp = rd.read()) != -1) {
			sb.append((char) cp);
		}
		return sb.toString();
	}
	/**
	 * This method reads the array and creates structType for every columns as 
	 * String
	 * 
	 * @param tableColumns all the encrypted columns 
	 * @return StructType it will creates everycolumns as a String.
	 */
	 public static StructType createSchema(List<String> tableColumns){

	        List<StructField> fields  = new ArrayList<StructField>();
	        for(String column : tableColumns){         

	                fields.add(DataTypes.createStructField(column, DataTypes.StringType, true));            

	        }
	        return DataTypes.createStructType(fields);
	    }
	  
}

