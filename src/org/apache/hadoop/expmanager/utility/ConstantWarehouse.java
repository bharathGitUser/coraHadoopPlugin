package org.apache.hadoop.expmanager.utility;

import java.io.File;
import java.util.HashMap;
import java.util.Map.Entry;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.JobManager;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;


// This is the class that handle all the system parameters. It initializes by reading a XML file.

public class ConstantWarehouse {

	private static final Log GeneralLogger = LogFactory.getLog("JobManagerlogger");
	public enum Counter { WORDS, VALUES }


	private static final String XMLpath =  System.getenv().get("HADOOP_MAPRED_HOME")+ "/etc/hadoop/expConstant.xml";
	
	//private static final String XMLpath =  "/usr/local/hadoop/etc/hadoop/expConstant.xml";
	
	
	private static boolean isInitiated = false;

	private static HashMap<String, Integer> intWarehouse = new HashMap <String, Integer>();
	private static HashMap<String, Double> doubleWarehouse = new HashMap <String, Double>(); 
	private static HashMap<String, String> stringWarehouse = new HashMap <String, String>();
	private static HashMap<String, Boolean> booleanWarehouse = new HashMap <String, Boolean>();
	private static HashMap<String, String> descriptionWarehouse = new HashMap <String, String>(); 




	public static void readXML() {

		try {

			File fXmlFile = new File(XMLpath);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);

			//optional, but recommended
			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();

			NodeList nList = doc.getDocumentElement().getChildNodes();

			for (int i = 0; i < nList.getLength(); i++) {
				Node nNode = nList.item(i);


				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;
					String variableName = eElement.getNodeName();
					String variableType = eElement.getElementsByTagName("Type").item(0).getTextContent();
					String variableValue= eElement.getElementsByTagName("Value").item(0).getTextContent();


					if (variableType.equalsIgnoreCase("Integer")){

						//anytemp.insert_long(Integer.valueOf(variableValue));
						intWarehouse.put(variableName, Integer.valueOf(variableValue));
					}

					if (variableType.equalsIgnoreCase("Double")){
						//anytemp.insert_double(Double.valueOf(variableValue));
						doubleWarehouse.put(variableName, Double.valueOf(variableValue));
					}

					if (variableType.equalsIgnoreCase("String")){
						//anytemp.insert_string(variableValue);
						stringWarehouse.put(variableName, variableValue);
					}


					if (variableType.equalsIgnoreCase("Boolean")){
						//anytemp.insert_boolean(Boolean.valueOf(variableValue));
						booleanWarehouse.put(variableName, Boolean.valueOf(variableValue));
					}




				}
			}
			isInitiated=true;

		} catch (Exception e) {
			e.printStackTrace();
		}

	}


	private static void writeToXML() {

		try {

			DocumentBuilderFactory docFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder docBuilder = docFactory.newDocumentBuilder();

			// root elements
			Document doc = docBuilder.newDocument();
			Element rootElement = doc.createElement("Constant");
			doc.appendChild(rootElement);


			for (Entry<String, Integer> entry : intWarehouse.entrySet()) {
				String constantName = entry.getKey();
				Integer value = entry.getValue();
				String description = descriptionWarehouse.get(constantName);

				Element constantNameEntry = doc.createElement(constantName);
				rootElement.appendChild(constantNameEntry);

				Element constantTypeEntry = doc.createElement("Type");
				constantTypeEntry.appendChild(doc.createTextNode("Integer"));
				constantNameEntry.appendChild(constantTypeEntry);

				Element constantValueEntry = doc.createElement("Value");
				constantValueEntry.appendChild(doc.createTextNode(value.toString()));
				constantNameEntry.appendChild(constantValueEntry);

				if (description!= null){
					Element constantDescriptionEntry = doc.createElement("Description");
					constantDescriptionEntry.appendChild(doc.createTextNode(description));
					constantNameEntry.appendChild(constantDescriptionEntry);
				}

			}

			for (Entry<String, Double> entry : doubleWarehouse.entrySet()) {
				String constantName = entry.getKey();
				Double value = entry.getValue();
				String description = descriptionWarehouse.get(constantName);

				Element constantNameEntry = doc.createElement(constantName);
				rootElement.appendChild(constantNameEntry);

				Element constantTypeEntry = doc.createElement("Type");
				constantTypeEntry.appendChild(doc.createTextNode("Double"));
				constantNameEntry.appendChild(constantTypeEntry);

				Element constantValueEntry = doc.createElement("Value");
				constantValueEntry.appendChild(doc.createTextNode(value.toString()));
				constantNameEntry.appendChild(constantValueEntry);

				if (description!= null){
					Element constantDescriptionEntry = doc.createElement("Description");
					constantDescriptionEntry.appendChild(doc.createTextNode(description));
					constantNameEntry.appendChild(constantDescriptionEntry);
				}

			}

			for (Entry<String, String> entry : stringWarehouse.entrySet()) {
				String constantName = entry.getKey();
				String value = entry.getValue();
				String description = descriptionWarehouse.get(constantName);

				Element constantNameEntry = doc.createElement(constantName);
				rootElement.appendChild(constantNameEntry);

				Element constantTypeEntry = doc.createElement("Type");
				constantTypeEntry.appendChild(doc.createTextNode("String"));
				constantNameEntry.appendChild(constantTypeEntry);

				Element constantValueEntry = doc.createElement("Value");
				constantValueEntry.appendChild(doc.createTextNode(value));
				constantNameEntry.appendChild(constantValueEntry);

				if (description!= null){
					Element constantDescriptionEntry = doc.createElement("Description");
					constantDescriptionEntry.appendChild(doc.createTextNode(description));
					constantNameEntry.appendChild(constantDescriptionEntry);
				}

			}

			for (Entry<String, Boolean> entry : booleanWarehouse.entrySet()) {
				String constantName = entry.getKey();
				Boolean value = entry.getValue();
				String description = descriptionWarehouse.get(constantName);

				Element constantNameEntry = doc.createElement(constantName);
				rootElement.appendChild(constantNameEntry);

				Element constantTypeEntry = doc.createElement("Type");
				constantTypeEntry.appendChild(doc.createTextNode("Boolean"));
				constantNameEntry.appendChild(constantTypeEntry);

				Element constantValueEntry = doc.createElement("Value");
				constantValueEntry.appendChild(doc.createTextNode(value.toString()));
				constantNameEntry.appendChild(constantValueEntry);

				if (description!= null){
					Element constantDescriptionEntry = doc.createElement("Description");
					constantDescriptionEntry.appendChild(doc.createTextNode(description));
					constantNameEntry.appendChild(constantDescriptionEntry);
				}

			}


			// write the content into xml file
			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(XMLpath));

			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);

			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
			transformer.transform(source, result);



		} catch (ParserConfigurationException pce) {
			pce.printStackTrace();
		} catch (TransformerException tfe) {
			tfe.printStackTrace();
		}
	}



	/*
	@SuppressWarnings("unchecked")
	public static <Any> Any getConstantValue(String variable) {

		if (isInitiated){
			Integer intresult = intWarehouse.get(variable);
			Double doubleresult= doubleWarehouse.get(variable);
			String stringresult = stringWarehouse.get(variable);
			Boolean booleanresult = booleanWarehouse.get(variable);

			if (intresult != null){
				return (Any) intresult;
			}else if(doubleresult !=null){
				return (Any) doubleresult;
			}else if (stringresult != null){
				return (Any) stringresult;

			}else if (booleanresult != null){
				return (Any) booleanresult;

			}else{
				GeneralLogger.warn("Constant \"" + variable+ "\" is not found in the warehouse.");
				return null;
			}


		}else{
			GeneralLogger.error("The constant warehouse is used without initialization.");
			return null;
		}


	}

	 */

	public static Class<?> getValueClass(String variable) {

		if (!isInitiated){

			readXML();
		}/*else{
			GeneralLogger.error("The constant warehouse is used without initialization.");
			return null;
		}*/



		Integer intresult = intWarehouse.get(variable);
		Double doubleresult= doubleWarehouse.get(variable);
		String stringresult = stringWarehouse.get(variable);
		Boolean booleanresult = booleanWarehouse.get(variable);

		if (intresult != null){
			return intresult.getClass();
		}else if(doubleresult !=null){
			return doubleresult.getClass();
		}else if (stringresult != null){
			return stringresult.getClass();

		}else if (booleanresult != null){
			return booleanresult.getClass();

		}else{
			GeneralLogger.warn("Constant \"" + variable+ "\" is not found in the warehouse.");
			return null;
		}


	}





	public static int getIntegerValue(String variable) {

		if (!isInitiated){

			readXML();
		}

		/*else{
		GeneralLogger.error("The constant warehouse is used without initialization.");
		return 0;
	}
		 */

		Integer intresult = intWarehouse.get(variable);

		if (intresult != null){
			return intresult.intValue();
		}else{
			GeneralLogger.warn("Constant \"" + variable+ "\" is not found in the INTEGER warehouse.");
			return 0;
		}


	}


	public static double getDoubleValue(String variable) {

		if (!isInitiated){
			readXML();



		}

		/*else{
		GeneralLogger.error("The constant warehouse is used without initialization.");
		return 0;
	}
		 */

		Double doubleresult= doubleWarehouse.get(variable);


		if (doubleresult !=null){
			return doubleresult.doubleValue();
		}else{
			GeneralLogger.warn("Constant \"" + variable+ "\" is not found in the DOUBLE warehouse.");
			return 0;
		}

	}




	public static String getStringValue(String variable) {

		if (!isInitiated){

			readXML();


		}
		/*
	if (isInitiated){



	}else{
		GeneralLogger.error("The constant warehouse is used without initialization.");
		return null;
	}
		 */


		String stringresult = stringWarehouse.get(variable);


		if (stringresult != null){
			return stringresult;

		}else{
			GeneralLogger.warn("Constant \"" + variable+ "\" is not found in the STRING warehouse.");
			return null;
		}


	}

	public static boolean getBooleanValue(String variable) {

		if (!isInitiated){

			readXML();


		}
		/*
	else{
		GeneralLogger.error("The constant warehouse is used without initialization.");
		return false;
	}
		 */

		Boolean booleanresult = booleanWarehouse.get(variable);

		if (booleanresult != null){
			return booleanresult.booleanValue();

		}else{
			GeneralLogger.warn("Constant \"" + variable+ "\" is not found in the warehouse.");
			return false;
		}


	}





	private static void restoreXMLTemplate(){



		doubleWarehouse.put("gamma_hard", (double)5);
		descriptionWarehouse.put("gamma_hard", "Hard Deadline Gamma parameter for approximation of the logistic utility function");

		doubleWarehouse.put("gamma_soft", (double)0.5);
		descriptionWarehouse.put("gamma_soft", "Soft Deadline Gamma parameter for approximation of the logistic utility function");

		doubleWarehouse.put("gamma_no", (double)0);
		descriptionWarehouse.put("gamma_no", "Gamma parameter for deadline insensitive jobs");

		intWarehouse.put("numOfQueues", 10);
		descriptionWarehouse.put("numOfQueues", "The number of simultaneous jobs that can be processed.");

		doubleWarehouse.put("expOffset", (double)14);
		descriptionWarehouse.put("expOffset", "The offset used to prevent the objective value from overflow.");

		doubleWarehouse.put("expConstant", (double)8);
		descriptionWarehouse.put("expConstant", "The Lambda vaue used in the exp barrier function.");

		doubleWarehouse.put("priorityRange", (double)9);
		descriptionWarehouse.put("priorityRange", "The maximum value of job piority.");




		stringWarehouse.put("pathToInputFilesBase", "/user/hduser/data/");
		descriptionWarehouse.put("pathToInputFilesBase", "The location in HDFS where the input files are present. ");


		stringWarehouse.put("pathToOutputFilesBase", "/user/hduser/output/");
		descriptionWarehouse.put("pathToOutputFilesBase", "The location in HDFS where output files must be written to.");

		intWarehouse.put("maxClusters", 2);
		descriptionWarehouse.put("maxClusters", "The number of classification clusters. For jobs KMean and Classification.");


		stringWarehouse.put("strModelFile", "/usr/local/hadoop/initial_centroids");
		descriptionWarehouse.put("strModelFile", "Location of the centroids template. For jobs KMean and Classification.");

		doubleWarehouse.put("division", (double)0.5);
		descriptionWarehouse.put("division", "For jobs KMean and Classification.");

		////////////////////////////////////////////////////////////////////////////////////


		intWarehouse.put("maxClusters", 2);
		descriptionWarehouse.put("maxClusters", "The number of classification clusters. For jobs KMean and Classification.");


		intWarehouse.put("mapTaskTime", 210);
		descriptionWarehouse.put("mapTaskTime", "Time it takes to complete a Map task.");

		intWarehouse.put("mapTaskRequest", 100);
		descriptionWarehouse.put("mapTaskRequest", "Number of Map tasks that will be created for a job.");

		intWarehouse.put("redTaskRequest", 40);
		descriptionWarehouse.put("redTaskRequest", "Number of Reduce tasks that will be created for a job.");


		intWarehouse.put("totalMapSlotsInCluster", 40);
		descriptionWarehouse.put("totalMapSlotsInCluster", "Number of Map solts in the cluster.");

		intWarehouse.put("totalReduceSlotsInCluster", 40);
		descriptionWarehouse.put("totalReduceSlotsInCluster", "Number of Reduce solts in the cluster.");


		stringWarehouse.put("hadoopHome", "/usr/local/hadoop/");
		descriptionWarehouse.put("hadoopHome", "Hadoop home directory");



		stringWarehouse.put("mosekProblem", "/home/hduser/myCode/main/problem.lp");
		descriptionWarehouse.put("mosekProblem", "Problem file for Max-min fair scheduling problem.");

		stringWarehouse.put("mosekOutput", "/home/hduser/myCode/main/output.txt");
		descriptionWarehouse.put("mosekOutput", "Problem solution file for Max-min fair scheduling problem.");

		stringWarehouse.put("mosekLog", "/home/hduser/myCode/main/mosekLog.txt");
		descriptionWarehouse.put("mosekLog", "Log file of Mosek.");

		stringWarehouse.put("timelineLog", "/home/hduser/log/Timeline.log");
		descriptionWarehouse.put("timelineLog", "Timeline log file location.");

		stringWarehouse.put("jobLog", "/home/hduser/log/jobresult.log");
		descriptionWarehouse.put("jobLog", "Job log file location.");

		stringWarehouse.put("GeneralLog", "/home/hduser/log/expmanager.log");
		descriptionWarehouse.put("GeneralLog", "General log file location.");

		stringWarehouse.put("jobDescriptionFile", "/home/hduser/workspace/HadoopPlugin/jobSet.txt");
		descriptionWarehouse.put("jobDescriptionFile", "Job profile desciption file location.");


		intWarehouse.put("minimumDeadline", 210);
		descriptionWarehouse.put("minimumDeadline", "The minimum deadline that a job can have. Should be larger than one MaptaskTime.");

		stringWarehouse.put("logFileName", "/usr/local/hadoop/logs/hadoop-hduser-jobtracker-hadoop-master.log");
		descriptionWarehouse.put("logFileName", "Job profile desciption file location.");


		String jobClassPrefix = "org.apache.hadoop.jobs.";
		stringWarehouse.put("jobClasses", jobClassPrefix+"AdjList\n"+jobClassPrefix+"Classification\n" +jobClassPrefix+"HistogramMovies\n"
				+jobClassPrefix+"HistogramRatings\n"+jobClassPrefix+"InvertedIndex\n"+jobClassPrefix+"Kmeans\n"+jobClassPrefix+"SelfJoin\n"
				+jobClassPrefix+"SequenceCount\n"+jobClassPrefix+"TermVectorPerHost\n"+jobClassPrefix+"WordCount\n"+jobClassPrefix+"terasort.TeraGen\n");
		descriptionWarehouse.put("jobClasses", "Job classes that will be used.");

		writeToXML();


	}



	public static void main(String argv[]) {

		//System.out.println(XMLpath);

		//restoreXMLTemplate();

		//System.out.println("Local XML configuration file restored.");









		//		readXML();



	}



}
