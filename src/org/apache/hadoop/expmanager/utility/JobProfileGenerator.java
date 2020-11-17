package org.apache.hadoop.expmanager.utility;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.HadoopJobProxy;
import org.apache.hadoop.expmanager.JobManager;
import org.apache.hadoop.jobs.HadoopJobs;

public class JobProfileGenerator {
	static Random  randGenerator = new Random(System.currentTimeMillis());
	private static Log GeneralLogger = LogFactory.getLog("JobManagerlogger");

	/*
	public static double genDeadLine(double arrivialTime, double bound){





		double minimumTime=(int)(ConstantWarehouse.getIntegerValue("minimumDeadline")*
				((double)(ConstantWarehouse.getIntegerValue("mapTaskRequest")+ConstantWarehouse.getIntegerValue("redTaskRequest"))/(double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster")));
		double lowerbound = arrivialTime + minimumTime;
		//double upperbound = 0;
		double deadline = 0;
		if (bound<lowerbound){
			deadline =  lowerbound;
		}else{
			deadline = lowerbound + (bound-lowerbound)*randGenerator.nextDouble();
		}
		DecimalFormat dfd = new DecimalFormat("#.##");      
		deadline = Double.valueOf(dfd.format(deadline));

 /*
		double finetuneParameter=3;

		double deadlineGen = finetuneParameter*deadlineBase*(randGenerator.nextDouble())+deadlineBase*2;




		double deadlineT=arrivialTime+deadlineGen;

		DecimalFormat dfd = new DecimalFormat("#.##");      
		deadlineT = Double.valueOf(dfd.format(deadlineT));
	 */	
	//	return deadline;
	//}







	public static void genArrival(double totalJobs, double meanArrivalTime,ArrayList<Double> jobArrivalTime){
		jobArrivalTime.clear();


		double staringTime=5;

		double currentTime=staringTime;
		jobArrivalTime.add(staringTime);

		for (int i =0; i< 1; i++){
			jobArrivalTime.add(currentTime);
		}
		while(jobArrivalTime.size()<totalJobs){


			double arrivalTimeTemp = currentTime + (-meanArrivalTime *Math.log(1-randGenerator.nextDouble()));

			DecimalFormat df = new DecimalFormat("#.##");      
			arrivalTimeTemp = Double.valueOf(df.format(arrivalTimeTemp));
			jobArrivalTime.add(arrivalTimeTemp);

			currentTime=arrivalTimeTemp;
		}

	}

	/*
	public static int genDeadline(ArrayList<Double> jobArrivalTime, ArrayList<Double> jobDeadline,ArrayList<Class<?extends HadoopJobs>> jobClassArray, double meanServiceTime, double STDServiceTime){
		jobDeadline.clear();

		@SuppressWarnings("unchecked")
		ArrayList<Double> temp =  (ArrayList<Double>) jobArrivalTime.clone();

		double totalBacklock =0;

		int CurrentTime=0;

		int allowedViolation =0;



		double totalTaskperJob= (double)ConstantWarehouse.getIntegerValue("mapTaskRequest") + (double)ConstantWarehouse.getIntegerValue("redTaskRequest");

		double rate = (double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster")/ (double)ConstantWarehouse.getIntegerValue("minimumDeadline");



		while (temp.size()!=0){
			CurrentTime++;

			System.out.println(CurrentTime);
			if (CurrentTime>=temp.get(0)){
				totalBacklock= totalBacklock + totalTaskperJob;



				double minimumDeadline = totalBacklock/rate;

				double DeadlineTimeTemp = randGenerator.nextGaussian()*STDServiceTime + meanServiceTime;

						//(-meanServiceTime *Math.log(1-randGenerator.nextDouble()));

				if  (DeadlineTimeTemp<minimumDeadline){

					if (randGenerator.nextDouble()>0.1){

						DeadlineTimeTemp = minimumDeadline + randGenerator.nextDouble()*(double)ConstantWarehouse.getIntegerValue("minimumDeadline");


					}
					else{
						allowedViolation++;
					}
				}



				double DeadlineUse = Math.ceil(DeadlineTimeTemp);

				jobDeadline.add(DeadlineUse);
				temp.remove(0);
			}else{
				if (totalBacklock>0){
					totalBacklock = totalBacklock- rate;
				}
				if (totalBacklock<0){
					totalBacklock=0;
				}

			}


		}

		return allowedViolation;


	}

	 */

	public static void genDeadline(ArrayList<Double> jobDeadline,ArrayList<Class<?extends HadoopJobs>> jobClassArray, double meanServiceTime){
		jobDeadline.clear();

		double capacaity= (double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		double redTaskNum = (double)ConstantWarehouse.getIntegerValue("redTaskRequest");
		double mapTaskNum= (double)ConstantWarehouse.getIntegerValue("mapTaskRequest");
		double factor = 1;

		for (int i =0; i < jobClassArray.size(); i++){

			Class<?extends HadoopJobs> jobClass= jobClassArray.get(i);

			try {
				Object instance = jobClass.newInstance();
				double jobRunTime = (mapTaskNum/capacaity)*jobClass.cast(instance).getMapTaskTime() +
						(redTaskNum/capacaity)*jobClass.cast(instance).getRedTaskTime();

				//double DeadlineTimeTemp = (-meanServiceTime *Math.log(1-randGenerator.nextDouble())) + factor*jobRunTime;

				double DeadlineTimeTemp =  (factor+randGenerator.nextDouble())*jobRunTime;

				double DeadlineUse = Math.ceil(DeadlineTimeTemp);

				jobDeadline.add(DeadlineUse);


			} catch (InstantiationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}




		}



	}

	public static void testTimeline(ArrayList<Double> jobArrivalTime, ArrayList<Double> jobDeadline){

		@SuppressWarnings("unchecked")
		ArrayList<Double> Arrivialtemp =  (ArrayList<Double>) jobArrivalTime.clone();

		ArrayList<Double> Deadlinetemp = new ArrayList<Double>();

		for (int i =0; i < Arrivialtemp.size(); i++){
			double temp = Arrivialtemp.get(i)+jobDeadline.get(i);
			Deadlinetemp.add(temp);	
		}


		File file = new File("/home/hduser/test.txt");

		FileWriter fw;

		try {
			fw = new FileWriter(file.getAbsoluteFile());


			BufferedWriter bw = new BufferedWriter(fw);

			int onlineNUmber =0;

			int CurrentTime=0;


			while (Deadlinetemp.size()!=0){
				CurrentTime++;

				if(Arrivialtemp.size()>0){
					if (CurrentTime>Arrivialtemp.get(0)){
						onlineNUmber++;
						Arrivialtemp.remove(0);
					}
				}

				if(Deadlinetemp.size()>0){
					if (CurrentTime>Deadlinetemp.get(0)){
						onlineNUmber--;
						Deadlinetemp.remove(0);
					}
				}



				bw.write(CurrentTime+ " "+onlineNUmber +"\n");


			}


			bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}





	public static void VisualizeTimeline(String inputfile){

		ArrayList<Double> Arrivial =  new ArrayList<Double>();

		ArrayList<Double> CompletionTime =  new ArrayList<Double>();


		FileInputStream fstream;
		try {
			fstream = new FileInputStream(inputfile);

			BufferedReader br = new BufferedReader(new InputStreamReader(fstream));

			String strLine;

			br.readLine();

			//Read File Line By Line
			while ((strLine = br.readLine()) != null)   {
				// Print the content on the console

				String[] parts = strLine.split("	");
				double arrivualTime = Double.valueOf(parts[7]);
				Arrivial.add(arrivualTime);

				String[] secondLevelParts =parts[0].split(" ");
				double completionTime=Double.valueOf(secondLevelParts[secondLevelParts.length-1]);
				CompletionTime.add(completionTime+ arrivualTime);

			}

			//Close the input stream
			br.close();



		} catch (FileNotFoundException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (NumberFormatException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		File file = new File("/home/hduser/test.txt");

		FileWriter fw;

		try {
			fw = new FileWriter(file.getAbsoluteFile());


			BufferedWriter bw = new BufferedWriter(fw);

			int onlineNUmber =0;

			int CurrentTime=0;


			while (CompletionTime.size()!=0){
				CurrentTime++;

				if(Arrivial.size()>0){
					if (CurrentTime>Arrivial.get(0)){
						onlineNUmber++;
						Arrivial.remove(0);
					}
				}

				if(CompletionTime.size()>0){
					if (CurrentTime>CompletionTime.get(0)){
						onlineNUmber--;
						CompletionTime.remove(0);
					}
				}



				bw.write(CurrentTime+ " "+onlineNUmber +"\n");


			}


			bw.close();

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}




		try {
			Process p = Runtime.getRuntime().exec("/usr/bin/gnuplot /home/hduser/plo");



			String line;

			BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while((line = error.readLine()) != null){
				System.out.println(line);
			}
			error.close();

			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((line=input.readLine()) != null){
				System.out.println(line);
			}

			input.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 



	}


	/*	

	public static void genArrivalandDeadline(double totalJobs, double meanArrivalTime,ArrayList<Double> jobArrivalTime, ArrayList<Double> jobDeadline){
		jobArrivalTime.clear();
		jobDeadline.clear();


		double currentTime=5;

		int numJobGened=1;


		int CapBoundTimecounter = earliestFeasibleDeadline(numJobGened);

		jobArrivalTime.add(currentTime);
		jobDeadline.add(genDeadLine(currentTime, CapBoundTimecounter*ConstantWarehouse.getIntegerValue("mapTaskTime")));

		while(jobArrivalTime.size()<totalJobs){
			numJobGened++;
			while(ConstantWarehouse.getIntegerValue("mapTaskRequest")*numJobGened>
			CapBoundTimecounter*ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster")){
				CapBoundTimecounter++;
			}

			double arrivalTimeTemp = currentTime + (-meanArrivalTime *Math.log(1-randGenerator.nextDouble()));

			DecimalFormat df = new DecimalFormat("#.##");      
			arrivalTimeTemp = Double.valueOf(df.format(arrivalTimeTemp));
			jobArrivalTime.add(arrivalTimeTemp);


			jobDeadline.add(genDeadLine(arrivalTimeTemp,CapBoundTimecounter*ConstantWarehouse.getIntegerValue("mapTaskTime")));

			currentTime=arrivalTimeTemp;
		}

	}




	 */





	public static int feasibilityTest(ArrayList<Double> jobArrivalTime, ArrayList<Double> jobDeadline){

		int infesibilityNum=0;

		int[] eventNumCounter= new int[jobArrivalTime.size()];

		for (int i=0; i< jobArrivalTime.size();i++){			
			for(int j=0; j< jobArrivalTime.size();j++){
				if(jobDeadline.get(i)>=jobDeadline.get(j)){
					eventNumCounter[i]++;
				}
			}
		}

		for (int i=0; i< jobArrivalTime.size();i++){	
			double resourceCap=  ((int)(jobDeadline.get(i)/ConstantWarehouse.getIntegerValue("mapTaskTime")))*ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
			double resourceReq= eventNumCounter[i]*ConstantWarehouse.getIntegerValue("mapTaskRequest");

			if(resourceReq >resourceCap){
				infesibilityNum++;
				//System.err.println("Resource infesibility event "+infesibilityNum+ " at time "+jobDeadline.get(i));
			}

		}

		/*
		if(infesibilityNum>Util.feasibilityCounterLimit){
			System.err.println("Number of resource infesibility events is larger than the limit. Program terminated.");
			System.exit(0);
		}
		 */


		return infesibilityNum;

	}

	public static double verifyJobSensitivity(int jobDeadlineType, int UtilityType, double jobSensitivity){



		if (UtilityType == HadoopJobProxy.linear){




			if(jobDeadlineType == 0){


				if((jobSensitivity>-1)&&(jobSensitivity<=-0.7)){
					return jobSensitivity;
				}else{
					return -(0.7 +0.3*randGenerator.nextDouble());
				}




			}else if (jobDeadlineType == 1){


				if((jobSensitivity>=-0.3)&&(jobSensitivity<-0.1)){
					return jobSensitivity;
				}else{
					return -(0.1 +0.3*randGenerator.nextDouble());
				}



			}else{
				return 0;
			}




		}else{

			if(jobDeadlineType == 0){

				if(jobSensitivity <(ConstantWarehouse.getDoubleValue("gamma_hard")/2) ){
					double mean = ConstantWarehouse.getDoubleValue("gamma_hard");
					double std= 0.5;
					jobSensitivity = mean + std*randGenerator.nextGaussian();

					while(jobSensitivity<0){
						jobSensitivity = mean + std*randGenerator.nextGaussian();
					}
				}
				return jobSensitivity;


			}else if(jobDeadlineType == 1){

				if((jobSensitivity <=0)||(jobSensitivity>1 )){
					double mean = ConstantWarehouse.getDoubleValue("gamma_soft");
					double std= 0.005;
					jobSensitivity = mean + std*randGenerator.nextGaussian();
					while(jobSensitivity<0){
						jobSensitivity = mean + std*randGenerator.nextGaussian();
					}
				}
				return jobSensitivity;
			}
			else{
				return  ConstantWarehouse.getDoubleValue("gamma_no");
			}
		}

	}


	public static ArrayList<HadoopJobProxy> createJobs(ArrayList<Double> jobArrivalTime, ArrayList<Double> jobDeadline, ArrayList<Class<?extends HadoopJobs>> jobClassArray, boolean submitAtOrigin){


		ArrayList<HadoopJobProxy> setOfJobs= new ArrayList<HadoopJobProxy>();

		for(int i =0; i < jobArrivalTime.size(); ++i){
			//job parameters

			double typetemp=randGenerator.nextDouble();
			int jobDeadlineType=2;


			double hardDeadlineThreshold = ConstantWarehouse.getDoubleValue("HardDeadlinePercentage");
			double softDeadlineThreshold = ConstantWarehouse.getDoubleValue("SoftDeadlinePercentage");



			if(typetemp<hardDeadlineThreshold+softDeadlineThreshold){
				jobDeadlineType=1;
			}

			if(typetemp < hardDeadlineThreshold){
				jobDeadlineType = 0;  // Hard
			}

			double jobPriority=randGenerator.nextInt((int)ConstantWarehouse.getDoubleValue("priorityRange"))+1;


			double ArrivalTime;
			if (submitAtOrigin){
				ArrivalTime=5;
			}else{
				ArrivalTime=jobArrivalTime.get(i);
			}

			int utilityType= ConstantWarehouse.getIntegerValue("utilityFunction");

			double jobSensitivity= verifyJobSensitivity(jobDeadlineType, utilityType, -1000);

			HadoopJobProxy newJob = new HadoopJobProxy(	"jobTempName",
					ConstantWarehouse.getIntegerValue("mapTaskRequest"),
					ConstantWarehouse.getIntegerValue("redTaskRequest"),
					jobDeadline.get(i),
					ArrivalTime,
					jobPriority,
					jobDeadlineType,
					utilityType,
					jobSensitivity,
					jobClassArray.get(i));

			setOfJobs.add(newJob);

			System.out.println("JobID: "+i+" join time:" + ArrivalTime + " deadline "+ jobDeadline.get(i)+ " sensitivity "+jobSensitivity);
			//System.out.println(newJob);
		}

		return setOfJobs; 
	}



	public static ArrayList<HadoopJobProxy> genJobsfromData(){
		ArrayList<HadoopJobProxy> setOfJobs = new ArrayList<HadoopJobProxy>();
		try 
		{   // The name of the file which we will read from
			// Prepare to read from the file, using a Scanner object
			File file = new File(ConstantWarehouse.getStringValue("jobDescriptionFile"));
			Scanner in = new Scanner(file);

			// Read each line until end of file is reached
			while (in.hasNextLine())
			{

				String line = in.nextLine();
				Scanner lineBreaker = new Scanner(line);

				try 
				{   

					int mapTaskRequested=lineBreaker.nextInt();
					int redTaskRequested=lineBreaker.nextInt();
					double DeadlineSubmitted=lineBreaker.nextDouble();
					double timeToJoin=lineBreaker.nextDouble();
					double jobPriority=lineBreaker.nextDouble();
					int jobDeadlineType=lineBreaker.nextInt();
					int UtilityType=lineBreaker.nextInt();
					double jobDeadlineSensitivity= lineBreaker.nextDouble();
					String jobClassName =lineBreaker.next();
					String jobName=lineBreaker.next();


					jobDeadlineSensitivity= verifyJobSensitivity(jobDeadlineType, UtilityType, jobDeadlineSensitivity);

					try {
						@SuppressWarnings("unchecked")
						Class<? extends HadoopJobs> jobClass =  (Class<? extends HadoopJobs>) Class.forName(jobClassName);
						HadoopJobProxy job = new HadoopJobProxy(	jobName,
								mapTaskRequested,
								redTaskRequested,
								DeadlineSubmitted,
								timeToJoin,
								jobPriority,
								jobDeadlineType,
								UtilityType,
								jobDeadlineSensitivity,
								jobClass);

						setOfJobs.add(job);

					} catch (ClassNotFoundException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}


					// Put the Account into the ArrayList

					lineBreaker.close();
				}

				catch (InputMismatchException e)
				{

					GeneralLogger.error("Format of the job description file is incorrect. Please double-check. Program terminated.");
					System.exit(1);

				}

				catch (NoSuchElementException e)
				{
					GeneralLogger.error("Format of the job description file is incorrect. Please double-check. Program terminated.");
					System.exit(1);

				}

			}

			in.close();

		}


		catch (FileNotFoundException e)
		{
			GeneralLogger.error("Jobset.txt not found. Terminate program.");
			System.exit(1);
		}   // Make an ArrayList to store all the accounts we will make



		return setOfJobs;
	}

	public static void writeJobstoData(ArrayList<HadoopJobProxy> setOfJobs){



		try {
			// Prepare to read from the file, using a Scanner object
			File file = new File(ConstantWarehouse.getStringValue("jobDescriptionFile"));
			FileWriter fw;
			fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			for (int i=0; i<setOfJobs.size(); i++){
				String line=setOfJobs.get(i).toString()+"\r\n";
				bw.write(line);
			}
			bw.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}





	/*
	@SuppressWarnings("unchecked")
	public static ArrayList<HadoopJobProxy> genSpecificSetOfJobs(){
		ArrayList<HadoopJobProxy> setOfJobs = new ArrayList<HadoopJobProxy>();



		String[] classname = {"org.apache.hadoop.jobs.AdjList",
				"org.apache.hadoop.jobs.Classification",
				"org.apache.hadoop.jobs.HistogramMovies",
				"org.apache.hadoop.jobs.HistogramRatings",
				"org.apache.hadoop.jobs.InvertedIndex",
				"org.apache.hadoop.jobs.Kmeans",
				"org.apache.hadoop.jobs.SelfJoin",
				"org.apache.hadoop.jobs.SequenceCount",
				"org.apache.hadoop.jobs.TermVectorPerHost",
				"org.apache.hadoop.jobs.WordCount",
		"org.apache.hadoop.jobs.terasort.TeraGen"};


		try {
			for(int i =0; i< classname.length; i++){


				HadoopJobProxy newJob0= new HadoopJobProxy(	"jobTempName",
						ConstantWarehouse.getIntegerValue("mapTaskRequest"),
						ConstantWarehouse.getIntegerValue("redTaskRequest"),
						10000,
						5+ i*4000,
						ConstantWarehouse.getDoubleValue("priorityRange"),
						0,
						(Class<?extends HadoopJobs>)Class.forName(classname[i]));


				setOfJobs.add(newJob0);

			}



		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}


		return setOfJobs; 
	}


	 */




	@SuppressWarnings("unchecked")
	public static void main(String[] args) {


		int interArrivialTime= 130;
		int interServiceTime= 100;
		int totalJobs = 80;
		//ConstantWarehouse.readXML();

		ArrayList<Double> jobArrivalTime = new ArrayList<Double>();

		ArrayList<Double> jobDeadline = new ArrayList<Double>();



		//JobProfileGenerator.genArrival(factor*base, baseArrival/factor,jobArrivalTime);
		JobProfileGenerator.genArrival(totalJobs, interArrivialTime,jobArrivalTime);


		ArrayList<Class<?extends HadoopJobs>> jobClassArray = new ArrayList<Class<?extends HadoopJobs>>();
		String[] JobClasses = ConstantWarehouse.getStringValue("jobClasses").split("\n");
		for(int i =0; i<jobArrivalTime.size(); i++){
			int index=randGenerator	.nextInt(JobClasses.length);
			try {
				jobClassArray.add((Class<? extends HadoopJobs>) Class.forName(JobClasses[index]));
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}




		JobProfileGenerator.genDeadline(jobDeadline,jobClassArray, interServiceTime);

		JobProfileGenerator.testTimeline(jobArrivalTime, jobDeadline);

		ArrayList<HadoopJobProxy> temp= JobProfileGenerator.createJobs(jobArrivalTime, jobDeadline, jobClassArray, false);


		//ArrayList<HadoopJobProxy> temp= JobProfileGenerator.genSpecificSetOfJobs();


		JobProfileGenerator.writeJobstoData(temp);
		System.out.println("Job data generated and saved.");
		try {
			Process p = Runtime.getRuntime().exec("/usr/bin/gnuplot /home/hduser/plo");



			String line;

			BufferedReader error = new BufferedReader(new InputStreamReader(p.getErrorStream()));
			while((line = error.readLine()) != null){
				System.out.println(line);
			}
			error.close();

			BufferedReader input = new BufferedReader(new InputStreamReader(p.getInputStream()));
			while((line=input.readLine()) != null){
				System.out.println(line);
			}

			input.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 

	}

}
