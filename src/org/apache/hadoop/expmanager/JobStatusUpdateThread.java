package org.apache.hadoop.expmanager; 

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;
import org.apache.hadoop.expmanager.utility.JobProfileGenerator;
import org.apache.hadoop.expmanager.utility.Utility;
import org.apache.hadoop.jobs.HadoopJobs;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.mapred.TaskCompletionEvent;
import org.apache.hadoop.mapreduce.JobCounter;


public class JobStatusUpdateThread extends Thread{
	JobManager jobManHandler;
	private boolean isTerminated=false;
	private double originOfTime=0;
	private double jobSubmissionTimeOffset=0;
	private boolean enableTimeSkipping =false;
	private int CurrentJobSubmissionPointer=0;
	//	private boolean SystemChanged = false;
	private ArrayList<HadoopJobProxy> JobsWaitingToBeSubmit;
	private Log TimelineLogger = LogFactory.getLog("TimelineLogger");
	private Log JobLogger = LogFactory.getLog("Joblogger");
	private Log GeneralLogger = LogFactory.getLog("JobManagerlogger");
	private int updateAggregationTime;
	private double TaskUpdateTime;
	private static Random  randGenerator = new Random(System.currentTimeMillis());


	private static double ddiFactor =0.25;
	private static boolean updateDDIFactor=false;







	public JobStatusUpdateThread(JobManager jobManHandler) {
		this.jobManHandler = jobManHandler;
		this.originOfTime=System.currentTimeMillis();
		JobsWaitingToBeSubmit= new ArrayList<HadoopJobProxy>();
		updateAggregationTime = ConstantWarehouse.getIntegerValue("WeightUpdateAggregationTime")*1000;

		//JobLogger.info("Complete Event Format: TotalTime	Latency	Utility	QueueID	MapRequest	ReduceRequest	Deadline	TimeToJoin	Priority	DeadlineType JobType	JobName");
		start();
	}

	public void terminateThread(){
		isTerminated=true;
		TimelineLogger.info("All jobs have been executed. Program terminated.");
		if(ConstantWarehouse.getBooleanValue("EnableDynamicDeadlineInjection")&&
				jobManHandler.getSchedulerName().contains("MMF")){
			GeneralLogger.warn("Job description file is replaced at the end of program.");
			JobProfileGenerator.writeJobstoData(jobManHandler.getAllJobsProfileSet());
			Utility.StoreResult(jobManHandler.getAllJobsProfileSet());
		}
	}

	public static void increaseDDI(){
		updateDDIFactor=true;
	}



	private void HandleComletedJob(HadoopJobProxy job) {
		job.setDone();

		TimelineLogger.info("Job "+job.getJobName()+ " is completed.");
		JobLogger.info("Job Complete Event: "+ job.printResult()+ "	"+ job.toString());

		//SystemChanged=true;	
		TaskUpdateTime = System.currentTimeMillis();

		jobManHandler.getCurrentJobSet().remove(job);
		jobManHandler.getQueueManager().returnQueueToPool(job.getQueueId());
		jobManHandler.getCompletedJobSet().add(job);

		if(jobManHandler.getCompletedJobSet().size() == jobManHandler.getAllJobsProfileSet().size()){
			jobManHandler.setAllJobsDone(true);
			terminateThread();
		}

		enableTimeSkipping= true;

	}

	private void HandleMapCompleteJob(HadoopJobProxy job) {
		job.setMapDone();
		//SystemChanged=true;
		TaskUpdateTime = System.currentTimeMillis();
		//JobLogger.info("Map Complete Event: "+ job.getMapFinishTime());

	}






	public void updateUnfinishedJobs() {


		if(!isTerminated){
			for (int is=0; is< jobManHandler.getCurrentJobSet().size(); is++) {
				HadoopJobProxy job = jobManHandler.getCurrentJobSet().get(is);

				try {

					if(job.getRunningJobHandler()!=null){
						long mapTasksLaunched=job.getRunningJobHandler().getCounters().getCounter(JobCounter.TOTAL_LAUNCHED_MAPS);
						//long mapTasksLaunched=job.getRj().getCounters().getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
						int RemainingMapTasks = (int) (job.getmapTaskRequested()-mapTasksLaunched);
						if(RemainingMapTasks > 0){
							if (RemainingMapTasks!=job.getnumRemainingMapTasks()){
								//SystemChanged = true;
								TaskUpdateTime = System.currentTimeMillis();
							}

							job.setnumRemainingMapTasks(RemainingMapTasks);
						}

						long redTasksLaunched=job.getRunningJobHandler().getCounters().getCounter(JobCounter.TOTAL_LAUNCHED_REDUCES);
						//long mapTasksLaunched=job.getRj().getCounters().getCounter(JobInProgress.Counter.TOTAL_LAUNCHED_MAPS);
						int RemainingRedTasks = (int) (job.getredTaskRequested()-redTasksLaunched);
						if(RemainingRedTasks > 0){
							if (RemainingRedTasks!=job.getnumRemainingRedTasks()){
								//SystemChanged = true;
								TaskUpdateTime = System.currentTimeMillis();
							}
							job.setnumRemainingRedTasks(RemainingRedTasks);
						}
					}

					double timeElapsed = (int)((System.currentTimeMillis()-job.getLaunchTime())/1000);
					double newDeadline=0;

					if(timeElapsed<0){

						GeneralLogger.error("Current time is eariler than the launch time of jobs. Something is wrong.");
						System.exit(1);
					}
					newDeadline = job.getJobDeadlinewhenSubmit()- timeElapsed;
					job.setCurrentDeadline(newDeadline);

				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			//	System.out.println();

			/*
			if (SystemChanged){

				jobManHandler.updateCapacity();
				SystemChanged=false;
			}
			 */
		}
	}












	private void updateTaskProgressEvents(){

		if(!isTerminated){
			for (Iterator<HadoopJobProxy> iterator = jobManHandler.getCurrentJobSet().iterator(); iterator.hasNext();) {
				HadoopJobProxy job = (HadoopJobProxy) iterator.next();
				if (job.isSubmitted()){
					try {
						TaskCompletionEvent[] bunchOfEvents;
						bunchOfEvents =job.getRunningJobHandler().getTaskCompletionEvents(job.getTaskEventArray().size());
						if (bunchOfEvents != null && bunchOfEvents.length != 0) {
							job.getTaskEventArray().addAll(Arrays.asList(bunchOfEvents));
						}

					} catch (IOException e) {
						e.printStackTrace();

					}
				}
			}
		}
	}



	private void CreateJobProxy(){
		if(!isTerminated){

			double currentTime=(System.currentTimeMillis()-originOfTime)/1000 + jobSubmissionTimeOffset;
			if(CurrentJobSubmissionPointer<jobManHandler.getAllJobsProfileSet().size()){

				int capacity=  ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
				// Check the timeline and put new job into the waiting submission queue
				while((jobManHandler.getAllJobsProfileSet().get(CurrentJobSubmissionPointer).getTimetoJoin()<currentTime)
						&& (jobManHandler.getCurrentJobSet().size()*2<capacity)){

					while (jobManHandler.getCurrentJobSet().size()>= jobManHandler.getQueueManager().getNumQueues()){
						jobManHandler.addNewQueue();
					}
					int queueID = -1;
					while(queueID <0){
						queueID= jobManHandler.getQueueManager().releaseOneQueueID();
					}


					HadoopJobProxy newJob = jobManHandler.getAllJobsProfileSet().get(CurrentJobSubmissionPointer);

					newJob.setQueueID(queueID);
					newJob.setLaunchTime(System.currentTimeMillis());
					//	JobLogger.info("Create Event: "+ newJob.toString());


					jobManHandler.getCurrentJobSet().add(newJob);
					JobsWaitingToBeSubmit.add(newJob);

					////////////////////Dynamic deadline injection ////////////////////////////////////
					DynamicDeadlineInjection(newJob);

					//SystemChanged=true;
					TaskUpdateTime = System.currentTimeMillis();


					TimelineLogger.info("Recently create job: "+ newJob.getJobName());
					TimelineLogger.info("Time to join: "+ newJob.getTimetoJoin());

					CurrentJobSubmissionPointer=CurrentJobSubmissionPointer+1;

					if(CurrentJobSubmissionPointer ==jobManHandler.getAllJobsProfileSet().size()){
						break;
					}
				}
			}
		}
	}




	private void DynamicDeadlineInjection(HadoopJobProxy newJob){
		if(ConstantWarehouse.getBooleanValue("EnableDynamicDeadlineInjection")&&
				jobManHandler.getSchedulerName().contains("MMF")){
			if(newJob.getjobDeadlineType()==2){
				newJob.setJobDeadline(1);
			}else{
				double totalActiveJobs = jobManHandler.getCurrentJobSet().size();
				double MapCapa = (double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster")/totalActiveJobs;
				double RedCapa = (double)ConstantWarehouse.getIntegerValue("totalReduceSlotsInCluster")/totalActiveJobs;

				double mapTime = (newJob.getnumRemainingMapTasks()/MapCapa)*newJob.getEstimatedMapExecTime();
				double redTime = (newJob.getnumRemainingRedTasks()/RedCapa)*newJob.getEstimatedRedExecTime();
				double expectRuntime = mapTime + redTime;

				if (updateDDIFactor){
					ddiFactor = ddiFactor+0.1;
					GeneralLogger.info("DDI factor has been increased to "+ddiFactor+".");
					updateDDIFactor=false;
				}

				int deadlineUsed= (int)(expectRuntime* (0.1*randGenerator.nextGaussian() + 1+ ddiFactor));//*(double)jobManHandler.getCurrentJobSet().size()));

				while(deadlineUsed<0){
					deadlineUsed=  (int)(expectRuntime* (0.1*randGenerator.nextGaussian() + 1+ddiFactor));//*(double)jobManHandler.getCurrentJobSet().size()));
				}


				newJob.setJobDeadline((double)deadlineUsed);
			}
		}



	}


	private void SubmitJobtoHadoop(HadoopJobProxy newJob){


		Class<?extends HadoopJobs> jobClass= newJob.getjobClass();

		try {
			Object instance = jobClass.newInstance();
			RunningJob jobHandler = jobClass.cast(instance).submitAndExec(newJob.getJobName(), "queue"+ newJob.getQueueId());
			newJob.setRunningJobHandler(jobHandler);
			newJob.setRunning();
			TimelineLogger.info("Recently submited job: "+ newJob.getJobName());
		} catch (InstantiationException e) {

			e.printStackTrace();
		} catch (IllegalAccessException e) {

			e.printStackTrace();
		} 


	}



	public void run() {

		//int numJobsinRedPhase= 0;


		while(!isTerminated){
			double totalActiveJobs=0;

			for (int i =0; i< jobManHandler.getCurrentJobSet().size();i++){

				HadoopJobProxy job = jobManHandler.getCurrentJobSet().get(i);
				try {
					//	if(job.getRj().mapProgress() == 1){
					if (job.isSubmitted()){
						totalActiveJobs++;
						if(job.getRunningJobHandler().isComplete()){	
							HandleComletedJob(job);
						}else{
							if(job.getRunningJobHandler().mapProgress() == 1 && !job.isMapDone()){
								HandleMapCompleteJob(job);
							}
						}

					}
				} catch (IOException e) {
					e.printStackTrace();
				}
				/*
				if (job.isMapDone()){
					numJobsinRedPhase++;
				}
				 */
			}


			updateTaskProgressEvents();


			// offset current time so that it will skip cases in which no job is running. 
			if(( jobManHandler.getCurrentJobSet().size()==0)&&(enableTimeSkipping)){
				enableTimeSkipping=false;
				if(CurrentJobSubmissionPointer<jobManHandler.getAllJobsProfileSet().size()){
					jobSubmissionTimeOffset = jobManHandler.getAllJobsProfileSet().get(CurrentJobSubmissionPointer).getTimetoJoin()
							-10 - (System.currentTimeMillis()-originOfTime)/1000;

					GeneralLogger.info("Program time is advanced by "+ jobSubmissionTimeOffset + " seconds.");
				}

			}


			CreateJobProxy();
			updateUnfinishedJobs();

			double TimeDifference = System.currentTimeMillis() - TaskUpdateTime; 

			if ((TimeDifference >  updateAggregationTime)&&(TaskUpdateTime!=0)){

				jobManHandler.updateCapacity();
				TaskUpdateTime=0;
			}


			//int numJobsinMapPhase  = numActJobs - numJobsinRedPhase;

			int capacity=  ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");


			if((JobsWaitingToBeSubmit.size()>0) && (totalActiveJobs*2<capacity)){

				for (Iterator<HadoopJobProxy> it = JobsWaitingToBeSubmit.iterator(); it.hasNext();){
					HadoopJobProxy job = it.next();
					//System.out.println("job starts at:"+job.getLaunchTime());
					if (job.getmapSlotsAssigned()>0){
						//	JobLogger.info("New job submitted "+job.getJobName()+" with allocated capa: "+job.getAllocatedCap());
						SubmitJobtoHadoop(job);
						job.setSubmitted();
						it.remove();

					}
				}
			}

			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}



	}


}
