package org.apache.hadoop.expmanager;

import java.util.*;

import org.apache.hadoop.expmanager.schedulingpolicy.SchedulingPolicy;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.apache.hadoop.expmanager.utility.JobProfileGenerator;
import org.apache.hadoop.expmanager.utility.Utility;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;



public class JobManager {
	private ArrayList<HadoopJobProxy> currentJobs;
	private ArrayList<HadoopJobProxy> completedJobSet;
	private ArrayList<HadoopJobProxy> JobProfileSet;//maintains a list of all jobs that ran for a particular experiment
	//private SchedulingPolicy PolicyToBeTested;
	private static double ProgarmStartingTime;
	private boolean allJobsDone = false;
	private QueuePoolManager queueMan;
	private CapacityUpdater CapUpdater;

	//private Log JobLogger = LogFactory.getLog("Joblogger");
	private Log TimelineLogger = LogFactory.getLog("TimelineLogger");
	private Log GeneralLogger = LogFactory.getLog("JobManager");



	public JobManager(SchedulingPolicy PolicyToBeTested){
		//ConstantWarehouse.readXML();
		CapUpdater = new CapacityUpdater(PolicyToBeTested,ConstantWarehouse.getIntegerValue("numOfQueues"));
		if (!CapUpdater.init()){
		
		//this.PolicyToBeTested = PolicyToBeTested;
		JobProfileSet= JobProfileGenerator.genJobsfromData();
		currentJobs = new ArrayList<HadoopJobProxy>();
		completedJobSet=new ArrayList<HadoopJobProxy>();
		//CapUpdater = new CapacityUpdater(PolicyToBeTested,ConstantWarehouse.getIntegerValue("numOfQueues"));
		queueMan= new QueuePoolManager(ConstantWarehouse.getIntegerValue("numOfQueues"));
		}else{
			System.exit(0);
		}
	}

	
	public String  getSchedulerName(){
		return CapUpdater.getSchedulingPolicy().getPolicyName();
	}
	
	public void  updateCapacity() {
		CapUpdater.updateAllocationFile(currentJobs);
	}
	
	public void  addNewQueue() {
		
		CapUpdater.addNewQueue();
		queueMan.addNewQueue();
		TimelineLogger.info("New queue : Queue"+(queueMan.getNumQueues()-1)+ " is added.");
	}
	
/*
	public SchedulingPolicy getSchedPolicy() {
		return PolicyToBeTested;
	}
*/
	public static double getTimeOrigin(){
		return ProgarmStartingTime;
	}



	public synchronized boolean isAllJobsDone() {
		return allJobsDone;
	}


	public synchronized void setAllJobsDone(boolean allJobsDone) {
		this.allJobsDone = allJobsDone;
	}


	public QueuePoolManager getQueueManager(){
		return queueMan;
	}

	public ArrayList<HadoopJobProxy> getCurrentJobSet(){
		return currentJobs;
	}

	public ArrayList<HadoopJobProxy> getCompletedJobSet(){
		return completedJobSet;
	}
	public ArrayList<HadoopJobProxy> getAllJobsProfileSet(){
		return JobProfileSet;
	}


	public void execExp() {

	
		// Initiate Constant Warehouse
		
		//clear contents of log file to get rid of previous experiment results
		String timelineLog =ConstantWarehouse.getStringValue("timelineLog");
		String jobLog =ConstantWarehouse.getStringValue("jobLog");
		String GeneralLog =ConstantWarehouse.getStringValue("GeneralLog");
		String runtimeLog= ConstantWarehouse.getStringValue("runtimeLog");
		String UtilityLog =ConstantWarehouse.getStringValue("UtilityLog");
		 
				
		Utility.clearContentsOfFile(timelineLog);
		Utility.clearContentsOfFile(jobLog);
		Utility.clearContentsOfFile(GeneralLog);
		Utility.clearContentsOfFile(runtimeLog);
		Utility.clearContentsOfFile(UtilityLog);
		//System.out.println("******************************************************");
		TimelineLogger.info("New trial started");
		TimelineLogger.info("Policy:"+CapUpdater.getSchedulingPolicy().getPolicyName());
		ProgarmStartingTime=System.currentTimeMillis();

		if(ConstantWarehouse.getBooleanValue("EnableDynamicDeadlineInjection")&&
				CapUpdater.getSchedulingPolicy().getPolicyName().contains("MMF")){
			GeneralLogger.warn("Dynamic deadline injection is enabled.");
		}
		// Rename the jobs
		for(int m=0; m < JobProfileSet.size();++m){
			HadoopJobProxy job = JobProfileSet.get(m);
			//frehSetOfJobs.add(job);
			String jobName = CapUpdater.getSchedulingPolicy().getPolicyName()+"-" + job.getjobClass().getSimpleName()+ "-"+m;
			job.setJobName(jobName);
		}

		//just to initialize the queue capacities
		double[] InititialqueueCapacities = new double[ConstantWarehouse.getIntegerValue("numOfQueues")];

		CapacityUpdater.writeCapSched(InititialqueueCapacities);
		CapacityUpdater.refreshHadoopQueues();	
		
		@SuppressWarnings("unused")
		JobStatusUpdateThread jobUpdateThread = new JobStatusUpdateThread(this);

		//HadoopJobProxy.setTimeOrigin(System.currentTimeMillis());


	}








	


	



	
	/*whenever a job is added or finished, this function can be called to asses
	 * how many resources the job still needs and how much time remains for its deadline.  
	 */
	
	

	

	
	/*
	public void updateResAllocVector(){


		for (Iterator<HadoopJobProxy> iterator = JobProfileSet.iterator(); iterator.hasNext();) {
			HadoopJobProxy job = (HadoopJobProxy) iterator.next();
			try {
				if(job.getRj() == null){
					//System.out.println(job.getJobName() + "not started");



					if(currentJobs.contains(job)){
						job.resAllocationOverTime = job.resAllocationOverTime+"	"+job.getAllocatedCap();//"0";
					}else{
						job.resAllocationOverTime = job.resAllocationOverTime+"	NA";//"0";
					}
				}
				else if(job.getRj().isComplete() == true){
					//System.out.println(job.getJobName() + " is complete");
					job.resAllocationOverTime = job.resAllocationOverTime+"	"+"COMP";
				}else{
					//System.out.println(job.getJobName() + " is executing");
					job.resAllocationOverTime = job.resAllocationOverTime+"	"+job.getAllocatedCap();
				}
				//System.out.println(job.resAllocationOverTime);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			//System.out.println(job.getJobName()+" "+job.resAllocationOverTime);
		}
	}
*/

	


	
	/*

	public double calcUtility(int jobType, double deadline, double latency){
		double gamma =0;
		if(jobType == 0){
			gamma =  ConstantWarehouse.getDoubleValue("gamma_hard");
		}else if(jobType == 1)
			gamma = ConstantWarehouse.getDoubleValue("gamma_soft");
		else if(jobType == 2){
			gamma = ConstantWarehouse.getDoubleValue("gamma_no");
		}

		int mapTaskTime = ConstantWarehouse.getIntegerValue("mapTaskTime");

		deadline = deadline/mapTaskTime;
		latency = latency/mapTaskTime;
		double normalizer = 1 + Math.exp(-1*gamma*deadline);
		double utility = 1 + Math.exp(gamma*latency);
		return normalizer/utility -0.5;

	}
*/


	/*
	public void obtainJobStats(){
		double hardLatency =0;
		double highPriorityLatency=0;
		double totalLatency=0;

		double totalUtility =0;
		String allocation = "";
		String expectedFinishingTime = "";
		String actualFinishingTime="";


		for(int i=0; i < allJobs.size();++i){
			HadoopJobProxy job = allJobs.get(i);
			if(job.getjobDeadlineType() == 0)
				hardLatency = hardLatency + job.getLatency();

			double piorityRange = ConstantWarehouse.getDoubleValue("piorityRange");
			if(job.getPriority() > 0.5*piorityRange)
				highPriorityLatency = highPriorityLatency + job.getLatency();

			totalLatency=totalLatency+ job.getLatency();


			double jobUtility = calcUtility(job.getjobDeadlineType(), job.getOriginalDeadline(), job.getLatency());

			DecimalFormat df = new DecimalFormat("#.##");      
			jobUtility = Double.valueOf(df.format(jobUtility));

			jobDetails = jobDetails +job +"	"+jobUtility+ "\n";
			allocation = allocation + job.resAllocationOverTime+"\n";
			expectedFinishingTime=expectedFinishingTime+job.getJobName()+"|"+ job.getExpectedMapFinishingTimeVectro()+"\n";
			actualFinishingTime=actualFinishingTime+job.getJobName()+"|"+ job.getTotalMapTime()+"\n";


			//we have the value of gamma from above
			totalUtility = totalUtility + jobUtility;
			//System.out.println(job);jps
		}
		String timeHorizon ="";
		int origin = timeLine.get(0);
		//System.out.println("ts:"+timeLine);
		for(int i=0; i < timeLine.size();++i){
			timeHorizon = timeHorizon + (timeLine.get(i)-origin)+" ";
		}
		jobDetails =jobDetails +"TimeLine:\n"+timeHorizon+"\n"+ "Allocation:\n"+allocation+ "Expected Finishing Time:\n"+expectedFinishingTime+ "Actual Finishing Time:\n"+actualFinishingTime;
		cumulativeDetails = cumulativeDetails+"  "+totalUtility+"	"+hardLatency +"	"+highPriorityLatency+"	"+totalLatency; 
	}

	 */

}
