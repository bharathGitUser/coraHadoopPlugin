package org.apache.hadoop.expmanager;


import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.schedulingpolicy.SchedulingPolicy;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.apache.hadoop.expmanager.utility.UtilitySimulator;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.w3c.dom.Text;


public class CapacityUpdater {
	private int numberOfQueues;
	private SchedulingPolicy schedPolicy;
	private static Log GeneralLogger = LogFactory.getLog("JobManagerlogger");
	private Log RuntimeLogger = LogFactory.getLog("AlgorithmRuntimeLogger");


	public CapacityUpdater(SchedulingPolicy policy,int numberOfQueues){
		this.schedPolicy = policy;
		this.numberOfQueues = numberOfQueues;


	}
	public boolean init(){

		boolean reboot = ReformatCapacityXML(numberOfQueues);

		if(!reboot){
			refreshHadoopQueues();
		}else{
			GeneralLogger.warn("Queues have been removed. Please reboot Hadoop.");

		}

		return reboot;
	}


	public SchedulingPolicy getSchedulingPolicy(){
		return schedPolicy;
	}


	public synchronized void addNewQueue (){
		ReformatCapacityXML(numberOfQueues+1);
		refreshHadoopQueues();


	}


	public synchronized boolean ReformatCapacityXML(int newNumOfQueues){

		String configurationFileName = ConstantWarehouse.getStringValue("hadoopConfFolder")+"capacity-scheduler.xml";
		boolean needReboot = false;
		try {

			File fXmlFile = new File(configurationFileName);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);

			//optional, but recommended
			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work


			NodeList nList = doc.getDocumentElement().getChildNodes();





			int lastQueue = -1;




			for (int i = 0; i < nList.getLength(); i++) {
				Node nNode = nList.item(i);


				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;



					String propertyName = eElement.getElementsByTagName("name").item(0).getTextContent();
					//String propertyValue= eElement.getElementsByTagName("value").item(0).getTextContent();

					if (propertyName.equals("yarn.scheduler.capacity.root.default.capacity") &&(newNumOfQueues < numberOfQueues)){
						eElement.getElementsByTagName("value").item(0).setTextContent("100.0");
					}

					if (propertyName.equals("yarn.scheduler.capacity.root.queues")){
						String valueString = "default";
						for (int j =0; j<newNumOfQueues; j++ ){

							valueString= valueString+",queue"+j;
						}
						eElement.getElementsByTagName("value").item(0).setTextContent(valueString);
					}

					if (propertyName.contains("yarn.scheduler.capacity.root.queue") && propertyName.endsWith(".capacity")){

						String temp = propertyName.split("queue")[1];
						int queueID = Integer.valueOf(temp.split(".capacity")[0]);
						if(lastQueue < queueID){
							lastQueue=queueID;
						}
						if (queueID>=newNumOfQueues){

							needReboot = true;
							nNode.getParentNode().removeChild(nNode);

						}
						else{
							if (newNumOfQueues < numberOfQueues){
								eElement.getElementsByTagName("value").item(0).setTextContent("0.0");
							}
						}

					}

				}

			}


			if (lastQueue+1< newNumOfQueues){

				for (int i =0;i <newNumOfQueues-lastQueue-1; i++){

					Element p = doc.createElement("Property");
					Element pName = doc.createElement("name");
					Text NameTag = doc.createTextNode("yarn.scheduler.capacity.root.queue"+(lastQueue+i+1)+".capacity");
					pName.appendChild(NameTag);

					Element pValue = doc.createElement("value");
					Text valueTag = doc.createTextNode("0.0");
					pValue.appendChild(valueTag);

					p.appendChild(pName);
					p.appendChild(pValue);
					nList.item(0).getParentNode().appendChild(p);
				}


			}


			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(configurationFileName));

			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);

			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
			transformer.transform(source, result);

		}  catch (Exception e) {
			e.printStackTrace();
		}

		numberOfQueues = newNumOfQueues;

		return 	needReboot;
	}









	public void updateAllocationFile(ArrayList<HadoopJobProxy> currentJobs){
		double[] queueCapacities = new double[numberOfQueues]; 

		if (currentJobs.size()>0){

			double[] weights = new double[currentJobs.size()]; 


			double beginTime = System.currentTimeMillis();
			boolean isUpdateSuccessful = schedPolicy.updateWeights(currentJobs, weights);
			double runTime =  System.currentTimeMillis() - beginTime;
			
			if (schedPolicy.getPolicyName().contains("MMF")){
				UtilitySimulator.PrintUtility(currentJobs);
				String logString = "Runtime: "+ runTime + "	Number of jobs: " + currentJobs.size();
				RuntimeLogger.info(logString);
				
			}
			
			

			
			if(!isUpdateSuccessful){
				GeneralLogger.error("The weights have not been updated properly!");
				return;
			}



			for(int i =0; i < currentJobs.size();++i){
				if(currentJobs.get(i).isMapDone()){
					int numRedSlot = (int)((weights[i]/100)*(double)ConstantWarehouse.getIntegerValue("totalReduceSlotsInCluster"));
					currentJobs.get(i).setredSlotsAssigned(numRedSlot);
					currentJobs.get(i).setmapSlotsAssigned(0);
				}else{

					int numMapSlot = (int)((weights[i]/100)*(double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster"));
					currentJobs.get(i).setmapSlotsAssigned(numMapSlot);
					currentJobs.get(i).setredSlotsAssigned(0);
				}



				int queueId = currentJobs.get(i).getQueueId();
				//	System.out.println(queueId+" "+i);

				queueCapacities[queueId] = weights[i];//the remaining capacities default to 0; 
				//System.out.println(weights[i]);
			}	
			//System.out.println("trying to write to file....");
		}
		writeCapSched(queueCapacities);

		refreshHadoopQueues();
		//System.out.println("succeeded in writing to file....");
	}



	public static synchronized void writeCapSched(double[] queueCapacities){
		String configurationFileName = ConstantWarehouse.getStringValue("hadoopConfFolder")+"capacity-scheduler.xml";

		HashMap<String, Double> CapacityMap= new HashMap<String, Double>();

		double sum=0;

		for(int i =0; i < queueCapacities.length;++i){
			DecimalFormat formater = new DecimalFormat("#.##");      
			queueCapacities[i] = Double.valueOf(formater.format(queueCapacities[i]));

			//System.out.print(" Queue "+i + ": " + queueCapacities[i]);
			sum = sum + queueCapacities[i];
			CapacityMap.put("yarn.scheduler.capacity.root.queue"+i+".capacity", queueCapacities[i]);
			if ((queueCapacities[i]>100)||(queueCapacities[i]<0)){
				GeneralLogger.error("Invalid queue capacity value: Queue "+i+" "+queueCapacities[i]);
				GeneralLogger.error("Abort updating queue capacity");
				return;
			}
		}
		//System.out.println("");

		if (sum>100){
			GeneralLogger.error("Summation of capacity values larger than 100.");
			GeneralLogger.error("Abort updating queue capacity");
			return;
		}

		double deficit = 100-sum;
		//DecimalFormat df = new DecimalFormat("#.##");      
		//deficit = Double.valueOf(df.format(deficit));
		CapacityMap.put("yarn.scheduler.capacity.root.default.capacity", deficit);

		if ((deficit>5)&&(deficit!=100)){
			GeneralLogger.warn("Capacity weight deficiency: "+deficit);
			GeneralLogger.warn("Capacity weight deficiency is too large. Optimization solution is questionable.");
		}
		

		try {

			File fXmlFile = new File(configurationFileName);
			DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
			DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
			Document doc = dBuilder.parse(fXmlFile);

			//optional, but recommended
			//read this - http://stackoverflow.com/questions/13786607/normalization-in-dom-parsing-with-java-how-does-it-work
			doc.getDocumentElement().normalize();


			NodeList nList = doc.getDocumentElement().getChildNodes();

			int QueueCounter=0;

			for (int i = 0; i < nList.getLength(); i++) {
				Node nNode = nList.item(i);


				if (nNode.getNodeType() == Node.ELEMENT_NODE) {

					Element eElement = (Element) nNode;

					String propertyName = eElement.getElementsByTagName("name").item(0).getTextContent();
					//String propertyValue= eElement.getElementsByTagName("value").item(0).getTextContent();


					if(CapacityMap.containsKey(propertyName)){
						eElement.getElementsByTagName("value").item(0).setTextContent(CapacityMap.get(propertyName).toString());

						//eElement.setAttribute(propertyName, );
						QueueCounter++;
					}



				}
			}
			if(QueueCounter<queueCapacities.length+1){
				GeneralLogger.error("Not all the queues are found in the capacityScheduler.xml.");
				GeneralLogger.error("Abort updating queue capacity");
				return;	
			}




			TransformerFactory transformerFactory = TransformerFactory.newInstance();
			Transformer transformer = transformerFactory.newTransformer();
			DOMSource source = new DOMSource(doc);
			StreamResult result = new StreamResult(new File(configurationFileName));

			// Output to console for testing
			// StreamResult result = new StreamResult(System.out);

			transformer.setOutputProperty(OutputKeys.INDENT, "yes");
			transformer.setOutputProperty("{http://xml.apache.org/xslt}indent-amount", "4");
			transformer.transform(source, result);




		}  catch (Exception e) {
			e.printStackTrace();
		}



	}


	public static void refreshHadoopQueues(){
		//the Hadoop command to refresh the queues
		try {
			//this executes the command like it would be executed on the linux terminal
			Process p = Runtime.getRuntime().exec("/usr/local/hadoop/bin/yarn rmadmin -refreshQueues");
			p.waitFor();

			
			String line;

			BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while((line = error.readLine()) != null){
				
				if (!line.contains("Connecting to ResourceManager at")){
					GeneralLogger.error(line);
				}
				
			}
			error.close();

			/*
			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((line=input.readLine()) != null){
				System.out.println(line);
			}

			input.close();
			 */



		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}




}


