package org.apache.hadoop.expmanager;


import java.text.DecimalFormat;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.apache.hadoop.jobs.HadoopJobs;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapred.TaskCompletionEvent.Status;


//not calling it simply job because Hadoop has its own job class.
public class HadoopJobProxy implements Comparable<HadoopJobProxy>{
	
	public static final int sigmoid = 0;
	public static final int linear = 1;
	
	private String jobName;
	private int queueID;
	private int mapTaskRequested;
	private int redTaskRequested;
	private int mapSlotsAssigned;
	private int redSlotsAssigned;
	private int numRemainingMapTasks;
	private int numRemainingRedTasks;
	private double DeadlineSubmitted;
	private double timeToJoin;
	private double jobDeadlineSensitivity; 
	private int UtilityType;

	private double jobPriority;
	private int jobDeadlineType;
	private boolean isSubmitted;
	private boolean isMapDone;
	private boolean isDone; 
	private boolean isRunning;

	private Class<?extends HadoopJobs> jobClass;
	private RunningJob RunningJobHandler;

	private double mapFinishTime;
	private double jobFinishTime;
	private double launchTime;
	private double rank;

	// Below are something not clear
	private double currentDeadline;
	private List<TaskCompletionEvent> TaskEventArray;
	//ArrayList<Double> mapTaskTimeArray;
	//ArrayList<Double> redTaskTimeArray;

	//private static double timeOrigin=0;

	private Log GeneralLogger = LogFactory.getLog("JobManagerlogger");
	static Random  randGenerator = new Random(System.currentTimeMillis());

	//these are results related to the job. populated after and during execution



	public HadoopJobProxy(	String jobName,
			int mapTaskRequested,
			int redTaskRequested,
			double DeadlineSubmitted,
			double timeToJoin,
			double jobPriority,
			int jobDeadlineType,
			int UtilityType,
			double jobDeadlineSensitivity,
			Class<?extends HadoopJobs> jobClass){

		this.jobName=jobName;
		this.queueID=-1;
		this.mapTaskRequested=mapTaskRequested;
		this.redTaskRequested=redTaskRequested;
		this.mapSlotsAssigned=0;
		this.redSlotsAssigned=0;
		this.numRemainingMapTasks=mapTaskRequested;
		this.numRemainingRedTasks=redTaskRequested;
		this.DeadlineSubmitted=DeadlineSubmitted;
		this.timeToJoin=timeToJoin;
		this.currentDeadline=DeadlineSubmitted;
		this.launchTime=0;
		this.jobPriority=jobPriority;
		this.jobDeadlineType=jobDeadlineType;
		this.isSubmitted=false;
		this.isDone=false; 
		this.isMapDone=false;
		this.isRunning=false;
		this.jobClass=jobClass;
		this.mapFinishTime=0;
		this.jobFinishTime=0;
		this.TaskEventArray = new LinkedList<TaskCompletionEvent>();
		//this.redTaskTimeArray=null;
		this.RunningJobHandler=null;
		this.rank=0;
		this.UtilityType=UtilityType;
		this.jobDeadlineSensitivity= jobDeadlineSensitivity;
		
		

	}

	
	public HadoopJobProxy(HadoopJobProxy other){

		this.jobName=other.getJobName();
		this.queueID=other.getQueueId();
		this.mapTaskRequested=other.getmapTaskRequested();
		this.redTaskRequested=other.getredTaskRequested();
		this.mapSlotsAssigned=other.getmapSlotsAssigned();
		this.redSlotsAssigned=other.getredSlotsAssigned();
		this.numRemainingMapTasks=other.getnumRemainingMapTasks();
		this.numRemainingRedTasks=other.getnumRemainingRedTasks();
		this.DeadlineSubmitted=other.getJobDeadlinewhenSubmit();
		this.timeToJoin=other.getTimetoJoin();
		this.currentDeadline=other.getCurrentDeadline();
		this.launchTime=other.getLaunchTime();
		this.jobPriority=other.getPriority();
		this.jobDeadlineType=other.getjobDeadlineType();
		this.isSubmitted=other.isSubmitted();
		this.isDone=other.isDone(); 
		this.isMapDone=other.isMapDone();
		this.isRunning=other.isRunning();
		this.jobClass=other.getjobClass();
		this.mapFinishTime=other.getMapFinishTime();
		this.jobFinishTime=other.getJobFinishTime();
	
		this.TaskEventArray =new LinkedList<TaskCompletionEvent>(other.getTaskEventArray());
		
		this.RunningJobHandler=other.getRunningJobHandler();

		this.jobDeadlineSensitivity =other.getjobDeadlineSensitivity();
		this.UtilityType= other.getUtilityType();
	

	}



	public double calcUtility(double ExtraTimetoComplete){
		//calcUtility(job.getjobDeadlineType(), job.getOriginalDeadline(), job.getLatency());
		
		double delay = ExtraTimetoComplete - currentDeadline;
		switch (UtilityType) {
		case linear:
			//linear utility 
			if(delay<0){
				return jobDeadlineSensitivity*delay + jobPriority;
			}else{
				return 0;
			}
			
			//return jobDeadlineSensitivity*delay*jobPriority;
		default:
			double utility = 1 + Math.exp(jobDeadlineSensitivity*delay);
			return jobPriority*(1/utility-1)-1;
		}
		/*
			double maxPri = ConstantWarehouse.getDoubleValue("priorityRange")+1;
			double delay = ExtraTimetoComplete - currentDeadline;

			double utility = 1 + Math.exp(gamma*delay);
			return jobPriority*(1/utility)-maxPri;
		 */

	}



	public double invertUtility(double utility){


		if(utility>jobPriority){
			GeneralLogger.error("Job "+getJobName() +" cannot achieve an utility "+utility+". Program terminates.");
			System.exit(1);
		}


		double graceRegion = 0.9995;
		double utilityUsed = utility;

		if (utility>graceRegion*jobPriority){
			utilityUsed = graceRegion*jobPriority;
		}
		if (utility<(1-graceRegion)*jobPriority){
			utilityUsed = (1-graceRegion)*jobPriority;
		}

		//System.out.println(normalizedDeadline+" "+currentDeadline+" "+Util.mapTaskTime);
		double normalUtil = (jobPriority/(utilityUsed)) -1;
		double expectedFinishingTime = Math.log(normalUtil)/jobDeadlineSensitivity + currentDeadline;


		return expectedFinishingTime;
	}




	/*


	public double calcUtility(double time){
		//calcUtility(job.getjobDeadlineType(), job.getOriginalDeadline(), job.getLatency());

		double gamma =0;
		if(jobDeadlineType == 0){
			gamma = ConstantWarehouse.getDoubleValue("gamma_hard");
		}else if(jobDeadlineType == 1)
			gamma = ConstantWarehouse.getDoubleValue("gamma_soft");
		else if(jobDeadlineType == 2){
			gamma = ConstantWarehouse.getDoubleValue("gamma_no");
		}
		int mapTaskTime = ConstantWarehouse.getIntegerValue("mapTaskTime");
		double normalizedDeadline = (double)currentDeadline/(double)mapTaskTime;
		//System.out.println(normalizedDeadline+" "+currentDeadline+" "+Util.mapTaskTime);

		double normalizedLatency = time-normalizedDeadline;
		double normalizer = 1 + Math.exp(-1*gamma*normalizedDeadline);
		double utility = 1 + Math.exp(gamma*normalizedLatency);






		return jobPriority*((normalizer/utility)-0.5);
	}


	public double invertUtility(double utility){
		//calcUtility(job.getjobDeadlineType(), job.getOriginalDeadline(), job.getLatency());

		//TODO: rewrite his in unit of time

		double gamma =0;
		if(jobDeadlineType == 0){
			gamma = ConstantWarehouse.getDoubleValue("gamma_hard");
		}else if(jobDeadlineType == 1)
			gamma = ConstantWarehouse.getDoubleValue("gamma_soft");
		else if(jobDeadlineType == 2){
			gamma = ConstantWarehouse.getDoubleValue("gamma_no");
		}
		int mapTaskTime = ConstantWarehouse.getIntegerValue("mapTaskTime");
		double normalizedDeadline = (double)currentDeadline/(double)mapTaskTime;
		//System.out.println(normalizedDeadline+" "+currentDeadline+" "+Util.mapTaskTime);


		double normalizer = 1 + Math.exp(-1*gamma*normalizedDeadline);

		return Math.floor((Math.log(((normalizer*jobPriority)/(utility+0.5*jobPriority) ) -1)/gamma) + normalizedDeadline);
	}



	 */





	/*
	public int getExpectedMapFinishingTimeSlot(){
		return expectedMapFinishingTimeSlot;
	}

	public void setExpectedMapFinishingTimeSlot(int relativeExpectedMapFinishingTimeSlot) {
		//System.out.println("Expected finishing time: "+relativeExpectedMapFinishingTimeSlot);
		int mapTaskTime = ConstantWarehouse.getIntegerValue("mapTaskTime");
		if (launchTime==0){

			this.expectedMapFinishingTimeSlot =(relativeExpectedMapFinishingTimeSlot+1)*mapTaskTime;
		}else{
			this.expectedMapFinishingTimeSlot = (int) ((System.currentTimeMillis()-launchTime)/1000) + (relativeExpectedMapFinishingTimeSlot+1)*mapTaskTime;
		}

		expectedMapFinishingTimeVector=expectedMapFinishingTimeVector+Integer.toString(expectedMapFinishingTimeSlot)+"	";
		//System.out.println(expectedMapFinishingTimeVector);

	}
	 */
	/////////////////////////////////////////////////////////////////////////////////////////////////
	
	public void setUtilityType(int UtilityType){
		this.UtilityType=UtilityType;
	}
	public int getUtilityType(){
		return UtilityType;
	}

	public void setRank(double rank){
		this.rank=rank;
	}
	public double getRank(){
		return rank;
	}
	
	public void setmapTaskRequested(int mapTaskRequested){
		this.mapTaskRequested=mapTaskRequested;
	}
	public int getmapTaskRequested(){
		return mapTaskRequested;
	}

	public void setredTaskRequested(int redTaskRequested){
		this.redTaskRequested=redTaskRequested;
	}
	public int getredTaskRequested(){
		return redTaskRequested;
	}


	public void setnumRemainingMapTasks(int numRemainingMapTasks){
		this.numRemainingMapTasks=numRemainingMapTasks;
	}
	public int getnumRemainingMapTasks(){
		return numRemainingMapTasks;
	}

	public void setnumRemainingRedTasks(int numRemainingRedTasks){
		this.numRemainingRedTasks=numRemainingRedTasks;
	}
	public int getnumRemainingRedTasks(){
		return numRemainingRedTasks;
	}

	public void setmapSlotsAssigned(int mapSlotsAssigned){
		this.mapSlotsAssigned=mapSlotsAssigned;
	}
	public int getmapSlotsAssigned(){
		return mapSlotsAssigned;
	}

	public void setredSlotsAssigned(int redSlotsAssigned){
		this.redSlotsAssigned=redSlotsAssigned;
	}
	public int getredSlotsAssigned(){
		return redSlotsAssigned;
	}

	public void setMapDone(){
		setMapFinishTime(System.currentTimeMillis());
		isMapDone=true;
	}

	public boolean isMapDone(){
		return isMapDone;
	}


	/*
	public static void setTimeOrigin(double time){
		timeOrigin=time;
	}
	 */

	public String getJobID(){
		if(isSubmitted()){

			return RunningJobHandler.getID().toString();
		}

		return "null";
	}

	public double getTimetoJoin(){
		return timeToJoin;
	}

	public void setSubmitted(){
		isSubmitted=true;
	}

	public boolean isSubmitted(){
		return isSubmitted;
	}

	public void setRunning(){
		isRunning=true;
	}

	public boolean isRunning(){
		return isRunning;
	}

	public double getLatency(){
		if(isDone){
			double executionDelay = (jobFinishTime-launchTime)/1000 - DeadlineSubmitted;

			DecimalFormat df = new DecimalFormat("#.##");      
			executionDelay = Double.valueOf(df.format(executionDelay));

			return executionDelay ;
		}else{
			return 0;
		}
	}

	public double getTotalExecutionTime(){
		if(isDone){
			double executionTime = (jobFinishTime-launchTime)/1000;

			DecimalFormat df = new DecimalFormat("#.##");      
			executionTime = Double.valueOf(df.format(executionTime));

			return executionTime ;
		}else{
			return 0;
		}
	}


	public double getJobDeadlinewhenSubmit(){
		return DeadlineSubmitted;
	}


	public void setJobDeadline(double Deadline){
		if (!isRunning){
			DeadlineSubmitted= Deadline;
			currentDeadline = DeadlineSubmitted;
		}else{
			GeneralLogger.error("Job "+ jobName +" is already started. Modifying deadline is prohibited.");
		}
	}
	
	public void setCurrentDeadline(double newDeadline){
		this.currentDeadline = newDeadline;
	}
	public double getCurrentDeadline(){
		return currentDeadline;
	}

	public double getLaunchTime() {
		return launchTime;
	}



	public void setJobFinishTime(double jobFinishTime) {
		this.jobFinishTime = jobFinishTime;
	}
	public double getJobFinishTime() {
		return jobFinishTime;
	}


	public void setMapFinishTime(double mapFinishTime) {
		this.mapFinishTime = mapFinishTime;
	}

	public double getMapFinishTime() {
		return mapFinishTime;
	}


	public int getjobDeadlineType() {
		return jobDeadlineType;
	}

	
	public double getjobDeadlineSensitivity() {
		return jobDeadlineSensitivity;
	}
	
	
	public void setjobDeadlineSensitivity(double jobDeadlineSensitivity) {
		this.jobDeadlineSensitivity = jobDeadlineSensitivity;
	}
	
	public Class<?extends HadoopJobs> getjobClass(){
		return jobClass;
	}


	public void setLaunchTime(double launchTime) {
		this.launchTime = launchTime;
	}


	public boolean isDone() {
		return isDone;
	}

	public void setDone() {
		setJobFinishTime(System.currentTimeMillis());
		currentDeadline = DeadlineSubmitted;
		this.isDone = true;
	}

	public void forceDone() {
		
		currentDeadline = DeadlineSubmitted;
		this.isDone = true;
	}

	public String getJobName() {
		return jobName;
	}

	public void setJobName(String jobName) {
		this.jobName = jobName;
	}

	public RunningJob getRunningJobHandler() {
		return RunningJobHandler;
	}

	public void setRunningJobHandler(RunningJob RunningJobHandler) {
		this.RunningJobHandler = RunningJobHandler;
	}


	public int getQueueId(){
		return queueID;
	}
	public double getPriority() {
		return jobPriority;
	}


	public void setQueueID(int QueueID) {
		this.queueID = QueueID;
	}


	public boolean equals(HadoopJobProxy that){
		if( jobName.equalsIgnoreCase(that.getJobName()))
			return true;
		else
			return false;
	}


	/*
	public void insertMapTime( double executionTime ){
		mapTaskTimeArray.add(executionTime);

	}

	public void insertRedTime( double executionTime ){
		redTaskTimeArray.add(executionTime);

	}
	 */

	public List<TaskCompletionEvent> getTaskEventArray(){
		return TaskEventArray;
	}



	public double getEstimatedMapExecTime(){

		double GCF = ConstantWarehouse.getIntegerValue("SmallestGCF");
		double mapTime=0;
		try {
			Object instance = jobClass.newInstance();
			mapTime = jobClass.cast(instance).getMapTaskTime();

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return -1;
		} 
		double totalMapTime=0;
		int totalMapTaskFinished=0;
		for (Iterator<TaskCompletionEvent> iterator = TaskEventArray.iterator(); iterator.hasNext();) {
			TaskCompletionEvent event = (TaskCompletionEvent) iterator.next();
			if(event.isMapTask() && event.getStatus().equals(Status.SUCCEEDED)){
				totalMapTime = totalMapTime + event.getTaskRunTime();
				totalMapTaskFinished++;
			}
		}

		int MeanMapTaskTime = (int) (totalMapTime+mapTime)/(totalMapTaskFinished+1);
		return MeanMapTaskTime + (GCF-(MeanMapTaskTime)%GCF);

	}


	public double getEstimatedRedExecTime(){
		double GCF = ConstantWarehouse.getIntegerValue("SmallestGCF");
		double totalRedTime=0;
		double redTime=0;
		try {
			Object instance = jobClass.newInstance();
			redTime = jobClass.cast(instance).getRedTaskTime();


		} catch (Exception e) {
			// TODO AgetEstimatedRedExecTimeuto-generated catch block
			e.printStackTrace();
			return -1;
		} 

		int totalRedTaskFinished=0;
		for (Iterator<TaskCompletionEvent> iterator = TaskEventArray.iterator(); iterator.hasNext();) {
			TaskCompletionEvent event = (TaskCompletionEvent) iterator.next();
			if(!event.isMapTask() && event.getStatus().equals(Status.SUCCEEDED)){
				totalRedTime = totalRedTime + event.getTaskRunTime();
				totalRedTaskFinished++;
			}
		}

		int MeanRedTaskTime = (int) (totalRedTime+redTime)/(totalRedTaskFinished+1);
		return MeanRedTaskTime + (GCF-(MeanRedTaskTime)%GCF);


	}


	
	public double calcUtilityHumanReadable(double ExtraTimetoComplete){
		//calcUtility(job.getjobDeadlineType(), job.getOriginalDeadline(), job.getLatency());

		
		double delay = ExtraTimetoComplete - currentDeadline;
		switch (UtilityType) {
		case linear:
			//linear utility 
			if(delay<0){
				return jobDeadlineSensitivity*delay + jobPriority;
			}else{
				return 0;
			}
		default:
			if(jobDeadlineType != 2){
				double utility = 1 + Math.exp(jobDeadlineSensitivity*delay);
				return jobPriority*(1/utility-0.5);
			}else{
				return 0;
			}
		}	
	}
	
	public String printResult(){
		String temp="";
		if (isDone){
			temp = getTotalExecutionTime()+  "	"+ getLatency() + "	"+calcUtilityHumanReadable(getTotalExecutionTime())+"	"+calcUtility(getTotalExecutionTime()) +"	"+queueID;
		}
		return temp; 
	}



	public String toString(){		
		
		String temp=mapTaskRequested+"	"+
				redTaskRequested+"	"+
				DeadlineSubmitted+"	"+
				timeToJoin+"	"+
				jobPriority+"	"+
				jobDeadlineType+"	"+
				UtilityType+ "	"+
				jobDeadlineSensitivity+ "	"+
				jobClass.getName()+"	"+
				jobName;
		return temp; 
	}


	public int compareTo(HadoopJobProxy other) {
		return Double.compare(rank,other.getRank());
	}




}
