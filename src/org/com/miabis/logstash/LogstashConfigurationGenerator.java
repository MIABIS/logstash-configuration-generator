/**
 * 
 */
package org.com.miabis.logstash;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;





/**
 * @author suyama
 *
 */
public class LogstashConfigurationGenerator {

	/**
	 * @param args
	 */
	
	private static BufferedWriter writer;
	
	private static String inputPort;
	private static String entityFile;
	private static String orgId;
	private static String logstashFilesFolder;
	private static String type;
	private static String directoryDepth;
	private static String driverJarPath;
	private static String databaseName;
	private static String databaseUser;
	private static String databasePassword;

	private static final String ENTITY_MAPPING_FILE_NAME = "entitymapping.json";
	private static final String LIST_VALUES_MAPPING_FILE_NAME = "listvaluesmapping.json";
	
	private static List<String> entity = new ArrayList<String>();
	private static List<String> fileNames = new ArrayList<String>();
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		try {
			inputPort = args[0];
			entityFile = args[1];
			orgId = args[2];
			logstashFilesFolder = args[3];
			type = args[4];
			directoryDepth = args[5];
			driverJarPath = args[6];
			databaseName = args[7];
			databaseUser = args[8];
			databasePassword = args[9];
			
			
			
			
			
			
			
			JSONParser parser = new JSONParser();
			
			JSONArray entityArray = (JSONArray) parser.parse(new FileReader(entityFile));
			JSONObject joe = (JSONObject) entityArray.get(1);
			JSONArray entities = (JSONArray) joe.get("entity");
			Iterator eit = entities.iterator();
			
			while(eit.hasNext()){
				entity.add(((JSONObject)eit.next()).keySet().iterator().next().toString());
			}
			
			
			
			File folder = new File(logstashFilesFolder);
			File[] listOfFiles = folder.listFiles();

		    for (int i = 0; i < listOfFiles.length; i++) {
		      if (listOfFiles[i].isFile()) {
		        //System.out.println("File " + listOfFiles[i].getName().substring(0, listOfFiles[i].getName().lastIndexOf(".")));
		        fileNames.add(listOfFiles[i].getName());
		      }
		    }
			
			
			if(!(fileNames.contains(ENTITY_MAPPING_FILE_NAME) && fileNames.contains(LIST_VALUES_MAPPING_FILE_NAME))){
				System.err.println("Either \""+ENTITY_MAPPING_FILE_NAME+"\" or \""+ LIST_VALUES_MAPPING_FILE_NAME +"\" or both files "
						+ "are not present in the folder "+folder.toString()+
						". \nBoth files are needed to generated the logstash configuration.");
				System.exit(0);
			}
			
			
			if(!fileNames.contains(LogstashConfigurationGenerator.entity.get(0)+".json")){
				System.err.println("It seems like the main entity file \""+LogstashConfigurationGenerator.entity.get(0)+".json\" is"
						+ " missing in the folder "+folder.toString()+
						". \nWithout this file the logstash configuration cannot be generated.");
				System.exit(0);
				
			}
				
			
			File inputBeatsFile = new File(System.getProperty("user.dir")+File.separator+"generated"+File.separator+orgId+"_input-beats.conf");
			inputBeatsFile.getParentFile().mkdirs();
			writer = new BufferedWriter(new FileWriter(inputBeatsFile));
			generateInputBlockBeats(inputPort);
			writer.close();
			
			File filterFile = new File(System.getProperty("user.dir")+File.separator+"generated"+File.separator+orgId+"_filter.conf");
			filterFile.getParentFile().mkdirs();
			writer = new BufferedWriter(new FileWriter(filterFile));
			generateFilterBlock(folder, type, directoryDepth);
			writer.close();
			
			File outputDatabaseFile = new File(System.getProperty("user.dir")+File.separator+"generated"+File.separator+orgId+"_output-database.conf");
			outputDatabaseFile.getParentFile().mkdirs();
			writer = new BufferedWriter(new FileWriter(outputDatabaseFile));
			generateOutputBlockDatabase(driverJarPath, databaseName, databaseUser, databasePassword);
			writer.close();
			
			
			File inputDatabaseFile = new File(System.getProperty("user.dir")+File.separator+"generated"+File.separator+orgId+"_input-database.conf");
			inputDatabaseFile.getParentFile().mkdirs();
			writer = new BufferedWriter(new FileWriter(inputDatabaseFile));
			generateInputBlockDatabase(driverJarPath, databaseName, databaseUser, databasePassword, orgId);
			writer.close();
			
			File outputElasticFile = new File(System.getProperty("user.dir")+File.separator+"generated"+File.separator+orgId+"_output-elastic.conf");
			outputElasticFile.getParentFile().mkdirs();
			writer = new BufferedWriter(new FileWriter(outputElasticFile));
			generateOutputBlockElastic(orgId);
			writer.close();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		finally{
			try {
				writer.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}	
		}
		
	}

	private static void generateInputBlockBeats(String inputPort) {
		// TODO Auto-generated method stub
		/*input{

			beats{
				port => 5066
			}

		}*/

		
		try {
			writer.write("input {");
			writer.newLine();
			writer.write("\tbeats {");
			writer.newLine();
			writer.write("\t\tport => "+inputPort);
			writer.newLine();
			writer.write("\t}");
			writer.newLine();
			writer.write("}");
			writer.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	
	private static void generateInputBlockDatabase(String driverJarPath, String databaseName, String databaseUser, String databasePassword, String orgId) {
	
		/*input{

			  jdbc {
			  	  jdbc_driver_library => "C:/logstash-5.0.1/mysql-connector-java-5.1.40-bin.jar"
			      jdbc_connection_string => "jdbc:mysql://localhost:3306/miabis-federation"
			      jdbc_driver_class => "com.mysql.jdbc.Driver"
			      jdbc_user => "root"
			      jdbc_password => "suyesh"
			      statement => "select sample.*, sample_collection.*, study.*, contact_information.* 
			      				from 
			      					sample 
			      						left join 
			      					sample_collection 
			      						on sample.sample_collection_id=sample_collection.id
			      						left join
			      					study
			      						on sample.study_id=study.id
			      						left join 
			      					contact_information 
			      						on sample_collection.contact_information=contact_information.id
			      						left join
			      					contact_information
			      						on study.contact_information=contact_information.id"
			      type => "biobank_sample"
			      add_field => { "biobank" => "%{[fields][origin]}" } 
			  }
			  
			  	
		}*/
		
		try {
			writer.write("input {");
			writer.newLine();
			writer.write("\tjdbc {");
			writer.newLine();
			writer.write("\t\tjdbc_driver_library => \""+driverJarPath+"\"");
			writer.newLine();
			writer.write("\t\tjdbc_connection_string => \"jdbc:mysql://localhost:3306/"+databaseName+"\"");
			writer.newLine();
			writer.write("\t\tjdbc_driver_class => \"com.mysql.jdbc.Driver\"");
			writer.newLine();
			writer.write("\t\tjdbc_user => \""+databaseUser+"\"");
			writer.newLine();
			writer.write("\t\tjdbc_password => \""+databasePassword+"\"");
			writer.newLine();
			writer.write("\t\tstatement => \"select sample.*, sample_collection.*, study.*, contact_information.* "
			      				+"from " 
			      				+"	sample "
			      				+"		left join "
			      				+"	sample_collection "
			      				+"		on sample.sample_collection_id=sample_collection.id "
			      				+"		left join "
			      				+"	study "
			      				+"		on sample.study_id=study.id "
			      				+"		left join "
			      				+"	contact_information " 
			      				+"		on sample_collection.contact_information=contact_information.id "
			      				+"		left join "
			      				+"	contact_information as ci "
			      				+"		on study.contact_information=ci.id\"");
			writer.newLine();
			writer.write("\t\ttype => \""+orgId+"\"");
			writer.newLine();
			writer.write("\t}");
			writer.newLine();
			writer.write("}");
			writer.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private static void generateFilterBlock(File folder, String type, String directoryDepth){
		
		/*filter {
			if [type] == "ki_data" {
			  mutate {
		        split => { "source" => "/" }
		        add_field => { "sub_type" => "%{[source][3]}" } # TODO: find way to access last element of array (use ruby filter?). Array index starts from 1.
		      }

		      mutate {
		        join => { "source" => "/" }
		      }

		  
			  # SAMPLE
			  if [sub_type] == 'SCARAB-Samples.csv' {
			    csv {
			      columns => ["ID", "Parent sample ID", "Material type", "SAMPLE_VOLUME", "SAMPLE_UNITS", "KI_SPREC", "Sex", "T_AGE", "CLINICAL_TRIAL", "T_SAMPLE_FORM", "T_SAMPLE_STATE", "PROJECT", "Sample Collection", "Study"]
			      separator => ","
			    }
			  }
			}
		}*/
		
		
		/*filter {
			if [type] == "ki_data" {
				mutate {
					split => { "source" => "/" }
					add_field => { "sub_type" => "%{[source][3]}" }
				}

				mutate {
					join => { "source" => "/" }
				}

				if [sub_type] == "Sample.csv" {
					csv {
						columns => ["ID", "Parent sample ID", "Material type", "SAMPLE_VOLUME", "SAMPLE_UNITS", "KI_SPREC", "Sex", "Age", "CLINICAL_TRIAL", "T_SAMPLE_FORM", "T_SAMPLE_STATE", "PROJECT", "Sample Collection", "Study"]
						separator => ","
					}
				}

				if [sub_type] == "Sample Collection.csv" {
					csv {
						columns => ["ID", "Name", "Acronym", "Description", "DATE_CREATED", "DATE_COMPLETED", "NUM_SAMPLES", "GROUP_NAME", "Contact Information", ""]
						separator => ","
					}
				}

				if [sub_type] == "Study.csv" {
					csv {
						columns => ["ID", "Name", "Principal Investigator", "Description", "Study design", "KI_ANATOMIC_SYSTEMS", "Material type", "GROUP_NAME", "Contact Information", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", "", ""]
						separator => ","
					}
				}
			}
		}*/
		
		
		try {
			writer.write("filter {");
			writer.newLine();
			writer.write("\tif [type] == \""+type+"\" {");
			writer.newLine();
			writer.write("\t\tmutate {");
			writer.newLine();
			writer.write("\t\t\tsplit => { \"source\" => \"/\" }");
			writer.newLine();
			writer.write("\t\t\tadd_field => { \"sub_type\" => \"%{[source]["+directoryDepth+"]}\" }");
			writer.newLine();
			writer.write("\t\t}");
			writer.newLine();
			writer.newLine();
			writer.write("\t\tmutate {");
			writer.newLine();
			writer.write("\t\t\tjoin => { \"source\" => \"/\" }");
			writer.newLine();
			writer.write("\t\t}");
			writer.newLine();
			
			
			for(String entity : LogstashConfigurationGenerator.entity){
				
				List<String> columns = new ArrayList<String>();
				Map<String, String> columnsMapping = new HashMap<String, String>();
				
				
				if(fileNames.contains(entity+".json")){
					writer.newLine();
					writer.write("\t\tif [sub_type] == \""+entity+".csv\" {");
					writer.newLine();
					writer.write("\t\t\tcsv {");
					writer.newLine();
					writer.write("\t\t\t\tcolumns => [");

					JSONParser parser = new JSONParser();
					
					JSONArray a = (JSONArray) parser.parse(new FileReader(folder+File.separator+entity+".json"));
					JSONObject o = (JSONObject) a.get(1);
					JSONObject en = (JSONObject) ((JSONObject) o.get("entity")).get(entity);
					JSONArray attributes =  (JSONArray) en.get("attributes");
					
					
					
					for(ListIterator<String> li = attributes.listIterator(); li.hasNext();){
						columns.add(li.next());
					}
					
					JSONArray mappingArray = (JSONArray) parser.parse(new FileReader(folder+File.separator+ENTITY_MAPPING_FILE_NAME));
					JSONObject mappingObject = (JSONObject) mappingArray.get(1);
					
					JSONArray entityArray = (JSONArray)  mappingObject.get("entity");
					Iterator<JSONObject> it = entityArray.iterator();
					while(it.hasNext()){
						JSONObject jobj = it.next();
						if(jobj.containsKey(entity)){
							JSONObject attributesMappingObject = (JSONObject) ((JSONObject) jobj.get(entity)).get("attributes");
							for(Iterator<String> iterator = attributesMappingObject.keySet().iterator(); iterator.hasNext();) {
							    String key =  iterator.next();
							    String value = attributesMappingObject.get(key).toString();
							    columnsMapping.put(key, value);
							    
							}
							
							break;
						}
							
					}
					
					for(Iterator cit = columns.iterator(); cit.hasNext();){
						String value = cit.next().toString();
						String key = getKeyFromValue(value, columnsMapping);
						String comma = "";
						if(cit.hasNext()){
							comma = ", ";
						}
						
						if(key != null){
							writer.write("\""+key+"\""+comma);
						}
						else{
							writer.write("\""+value+"\""+comma);
						}
						
						if(!cit.hasNext()){
							writer.write("]");
						}
					}
					
					writer.newLine();
					writer.write("\t\t\t\tseparator => \",\"");
					writer.newLine();
					writer.write("\t\t\t}");
					
					
					
					JSONArray ma = (JSONArray) parser.parse(new FileReader(folder+File.separator+LIST_VALUES_MAPPING_FILE_NAME));
					JSONObject mo = (JSONObject) ma.get(1);
					JSONArray attrArray = (JSONArray)  mo.get("attribute");
					for(Iterator attrItr = attrArray.iterator(); attrItr.hasNext();){
						JSONObject joAttr = (JSONObject) attrItr.next();
						String attributeName = joAttr.keySet().iterator().next().toString();
						
						writer.newLine();
						writer.write("\t\t\ttranslate {");
						writer.newLine();
						writer.write("\t\t\t\tfield => \""+attributeName+"\"");
						writer.newLine();
						writer.write("\t\t\t\tdestination => \""+attributeName+"\"");
						writer.newLine();
						writer.write("\t\t\t\texact => true");
						writer.newLine();
						writer.write("\t\t\t\toverride => true");
						writer.newLine();
						writer.write("\t\t\t\tdictionary => [");
						
						
						JSONObject jolv = (JSONObject) joAttr.get(attributeName);
						Iterator lvmIterator = jolv.keySet().iterator();
						Map<String, String> valuesMapping = new HashMap<String, String>();
						while(lvmIterator.hasNext()){
							String key = lvmIterator.next().toString();
							String value = jolv.get(key).toString().trim();
							
							if(!value.isEmpty()){
								valuesMapping.put(key, value);
							}
							
						}
						
						
						for (Iterator<Map.Entry<String,String>> entries = valuesMapping.entrySet().iterator(); entries.hasNext();) {
							
							Map.Entry<String,String> entry = entries.next();
							writer.write("\""+entry.getValue()+"\", ");
							writer.write("\""+entry.getKey()+"\"");
							if(entries.hasNext()){
								writer.write(", ");
							}
							
						}
						
						writer.write("]");
						writer.newLine();
						writer.write("\t\t\t}");
					}
					
					
					
					
					
					writer.newLine();
					writer.write("\t\t}");
					writer.newLine();
				}
				
			}
			
			writer.write("\t}");
			writer.newLine();
			writer.write("}");
			writer.newLine();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	
	private static String getKeyFromValue(String value,	Map<String, String> columnsMapping) {
		// TODO Auto-generated method stub
		for (Map.Entry<String,String> entry : columnsMapping.entrySet()) {
			if (!value.trim().isEmpty() && entry.getValue().equals(value)) {
				return entry.getKey();
			}
		}
		return null;
	}

	private static void generateOutputBlockDatabase(String driverJarPath, String databaseName, String databaseUser, String databasePassword){
		/*output {

			  #stdout { codec => json }
			  stdout { codec => rubydebug }
			  
			  file {
			  	path => "C:/logstash-5.0.1/bin/output.log"
			   	codec => rubydebug
			  }


			  if [sub_type] == 'SCARAB-Samples.csv' {
			    jdbc {
			      driver_jar_path => "C:/logstash-5.0.1/mysql-connector-java-5.1.40-bin.jar"
			      driver_class => "com.mysql.jdbc.Driver"
			      connection_string => "jdbc:mysql://localhost:3306/miabis-federation"
				  username => "root"
				  password => "suyesh"
			      statement => [ "INSERT INTO SAMPLE (ID, PARENT_SAMPLE_ID, MATERIAL_TYPE, STORAGE_TEMPERATURE, SAMPLED_TIME, ANATOMICAL_SITE, SEX, AGE, AGE_UNIT, DISEASE_ONTOLOGY, DISEASE_ONTOLOGY_VERSION, DISEASE_ONTOLOGY_CODE, DISEASE_ONTOLOGY_DESCRIPTION, DISEASE_FREE_TEXT, SAMPLE_COLLECTION_ID, STUDY_ID, BIOBANK_ID) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
			       "%{[fields][origin]}_%{ID}", "Parent sample ID", "Material Type", "Storage temperature", "Sampled time", "Anatomical Site", "Sex", "Age", "Age Unit", "Disease Ontology", "Disease Ontology Version", "Disease Ontology Code", "Disease Ontology Description", "Disease Free Text", "Sample Collection", "Study", "%{[fields][origin]}"]
			    }
			  }

			  if [sub_type] == 'sample.csv' {
			    jdbc {
			      driver_jar_path => "C:/logstash-5.0.1/mysql-connector-java-5.1.40-bin.jar"
			      driver_class => "com.mysql.jdbc.Driver"
			      connection_string => "jdbc:mysql://localhost:3306/miabis-federation"
				  username => "root"
				  password => "suyesh"
			      statement => [ "INSERT INTO SAMPLE (ID, PARENT_SAMPLE_ID, MATERIAL_TYPE, STORAGE_TEMPERATURE, SAMPLED_TIME, ANATOMICAL_SITE, SEX, AGE, AGE_UNIT, DISEASE_ONTOLOGY, DISEASE_ONTOLOGY_VERSION, DISEASE_ONTOLOGY_CODE, DISEASE_ONTOLOGY_DESCRIPTION, DISEASE_FREE_TEXT, SAMPLE_COLLECTION_ID, STUDY_ID, BIOBANK_ID) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", 
			       "%{[fields][origin]}_%{ID}", "Parent sample ID", "Material Type", "Storage temperature", "Sampled time", "Anatomical Site", "Sex", "Age", "Age Unit", "Disease Ontology", "Disease Ontology Version", "Disease Ontology Code", "Disease Ontology Description", "Disease Free Text", "Sample Collection", "Study", "%{[fields][origin]}"]
			    }
			  }
			    
			}*/
		
		
		/*Modified INSERT statement for INSERT ... ON DUPLICATE KEY UPDATE Syntax
		
		[ "INSERT INTO SAMPLE 

		  (SAMPLE_ID, DISEASE, MIABIS_MATERIAL_TYPE, MATERIAL_TYPE, ANATOMICAL_SITE, 

		  SEX, DIAGNOSIS_TYPE, GENOTYPE_DATA_AVAILABLE, AGE_AT_SAMPLING, 

		  AGE_AT_DEATH, AGE_AT_DIAGNOSIS, AGE_AT_REMISSION, AFFECTED, 

		  FAMILY_MEMBERS_AVAILABLE, RELATED_SAMPLES_AVAILABLE, 

		  REGISTRY_DATA_AVAILABLE, HOSTING_BIOBANK, HOSTING_REGISTRY, 

		  PARTICIPANT_ID, DATE_OF_LAST_UPDATE) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 

		  ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE SAMPLE_ID=VALUES(SAMPLE_ID), DISEASE=VALUES(DISEASE), 

		  MIABIS_MATERIAL_TYPE=VALUES(MIABIS_MATERIAL_TYPE), MATERIAL_TYPE=VALUES

		  (MATERIAL_TYPE), ANATOMICAL_SITE=VALUES(ANATOMICAL_SITE), SEX=VALUES(SEX), 

		  DIAGNOSIS_TYPE=VALUES(DIAGNOSIS_TYPE), GENOTYPE_DATA_AVAILABLE=VALUES

		  (GENOTYPE_DATA_AVAILABLE), AGE_AT_SAMPLING=VALUES(AGE_AT_SAMPLING), 

		  AGE_AT_DEATH=VALUES(AGE_AT_DEATH), AGE_AT_DIAGNOSIS=VALUES

		  (AGE_AT_DIAGNOSIS), AGE_AT_REMISSION=VALUES(AGE_AT_REMISSION), 

		  AFFECTED=VALUES

		  (AFFECTED), FAMILY_MEMBERS_AVAILABLE=VALUES

		  (FAMILY_MEMBERS_AVAILABLE), 

		  RELATED_SAMPLES_AVAILABLE=VALUES

		  (RELATED_SAMPLES_AVAILABLE), 

		  REGISTRY_DATA_AVAILABLE=VALUES

		  (REGISTRY_DATA_AVAILABLE), 

		  HOSTING_BIOBANK=VALUES(HOSTING_BIOBANK), 

		  HOSTING_REGISTRY=VALUES(HOSTING_REGISTRY), PARTICIPANT_ID=VALUES

		  (PARTICIPANT_ID), 

		  DATE_OF_LAST_UPDATE=VALUES(DATE_OF_LAST_UPDATE)", "%{[fields][origin]}_%{Sample ID}", 

		  "Disease", "MIABIS Material Type", "Material Type", "Anatomical Site", 

		  "Sex", "Diagnosis Type", "Genotype data available", "Age at Sampling", 

		  "Age at Death", "Age at Diagnosis", "Age at Remission", "Affected", 

		  "Family members available", "Related samples available", "Registry data 

		  available", "Hosting Biobank", "Hosting Registry", "Participant ID", "Date 

		  of last update"]
*/
		
		try {
			writer.write("output {");
			writer.newLine();
			writer.write("\tstdout { codec => rubydebug }");
			writer.newLine();
			writer.newLine();
			writer.write("\tif [type] == \""+type+"\" {");
			writer.newLine();
			
			JSONParser parser = new JSONParser();
			JSONArray mappingArray = (JSONArray) parser.parse(new FileReader(entityFile));
			JSONObject mappingObject = (JSONObject) mappingArray.get(1);
			JSONArray entityArray = (JSONArray)  mappingObject.get("entity");
			Iterator entityArrayIterator = entityArray.iterator();
			
			while(entityArrayIterator.hasNext()){
				
				JSONObject o = (JSONObject) entityArrayIterator.next();
				String entityName = o.keySet().iterator().next().toString();
				JSONObject attributes = (JSONObject) o.get(entityName);
				
				if(fileNames.contains(entityName+".json")){
					writer.newLine();
					writer.write("\t\tif [sub_type] == \""+entityName+".csv\" {");
					writer.newLine();
					writer.write("\t\t\tjdbc {");
					writer.newLine();
					writer.write("\t\t\t\tdriver_jar_path => \""+driverJarPath+"\"");
					writer.newLine();
					writer.write("\t\t\t\tdriver_class => \"com.mysql.jdbc.Driver\"");
					writer.newLine();
					writer.write("\t\t\t\tconnection_string => \"jdbc:mysql://localhost:3306/"+databaseName+"\"");
					writer.newLine();
					writer.write("\t\t\t\tusername => \""+databaseUser+"\"");
					writer.newLine();
					writer.write("\t\t\t\tpassword => \""+databasePassword+"\"");
					writer.newLine();
					writer.write("\t\t\t\tstatement => [ \"INSERT INTO "+entityName.toUpperCase()+" (");
					
					JSONArray attributesArray = (JSONArray) attributes.get("attributes");
					String appendStringQuestionMark = " VALUES (";
					String appendOnDuplicateKeyUpdate = " ON DUPLICATE KEY UPDATE ";
					String appendOnDuplicateKeyUpdateColumns = "";
					String appendStringValues = " ";
					
					for(Iterator attributesArrayIterator = attributesArray.iterator(); attributesArrayIterator.hasNext();){
						String comma = "";
						String questionMark = "?";
						String column = attributesArrayIterator.next().toString();
						
						
						if(attributesArrayIterator.hasNext()){
							comma = ", ";
						}
						
						appendStringQuestionMark = appendStringQuestionMark.concat(questionMark+comma);
						appendStringValues = appendStringValues.concat("\""+column+"\""+comma);
						writer.write(column.replaceAll("\\s+", "_").toUpperCase()+comma);
						appendOnDuplicateKeyUpdateColumns = appendOnDuplicateKeyUpdateColumns.concat(column.replaceAll("\\s+", "_").toUpperCase()
								+"=VALUES("+column.replaceAll("\\s+", "_").toUpperCase()+")"+comma);
						
						if(!attributesArrayIterator.hasNext()){
							writer.write(")");
							appendStringQuestionMark = appendStringQuestionMark.concat(")");
						}
					}
					
					
					writer.write(appendStringQuestionMark);
					writer.write(appendOnDuplicateKeyUpdate);
					writer.write(appendOnDuplicateKeyUpdateColumns+"\",");
					writer.write(appendStringValues+" ]");
					
					writer.newLine();
					writer.write("\t\t\t}");
					writer.newLine();
					writer.write("\t\t}");
					writer.newLine();
				}
			}
			
			writer.write("\t}");
			writer.newLine();
			writer.write("}");
			writer.newLine();
			
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	
	private static void generateOutputBlockElastic(String orgId){
		
		/*output {

			  #stdout { codec => json }
			  stdout { codec => rubydebug }
			  
			  file {
			  	path => "C:/logstash-5.0.1/bin/output.log"
			   	codec => rubydebug
			  }
			  
			  if [type] == "biobank_sample" {
			        elasticsearch {
			            hosts => ["localhost:9200"]
			            document_id => "%{id}"
			        }
			  }
			  
		}*/
		
		try {
			writer.write("output {");
			writer.newLine();
			writer.write("\tstdout { codec => rubydebug }");
			writer.newLine();
			writer.newLine();
			writer.write("\tif [type] == \""+orgId+"\" {");
			writer.newLine();
			writer.write("\t\telasticsearch {");
			writer.newLine();
			writer.write("\t\t\thosts => [\"localhost:9200\"]");
			writer.newLine();
			writer.write("\t\t\tdocument_id => \"%{id}\"");
			writer.newLine();
			writer.write("\t\t}");
			writer.newLine();
			writer.write("\t}");
			writer.newLine();
			writer.write("}");
			writer.newLine();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
