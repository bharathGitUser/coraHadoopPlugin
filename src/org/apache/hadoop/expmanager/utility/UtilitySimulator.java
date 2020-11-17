package org.apache.hadoop.expmanager.utility;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.HadoopJobProxy;
import org.apache.hadoop.expmanager.JobManager;
import org.apache.hadoop.expmanager.JobStatusUpdateThread;
import org.apache.hadoop.expmanager.schedulingpolicy.MaxMinSchedulingPolicy;

public class UtilitySimulator {

	private static Log UtilityLogger= LogFactory.getLog("UtilityLogger");
	
	private static Log GeneralLogger = LogFactory.getLog("JobManagerlogger");

	
	public static void getEDFUtility( ArrayList<HadoopJobProxy> jobSet, HashMap<String, Double> resultArray){
		double TaskTimeGCF = MaxMinSchedulingPolicy.getTaskTimeGCF();
		
		double capacity = (double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		DecimalFormat dfd = new DecimalFormat("#.##");      
		
		for (int i=0; i< jobSet.size();i++){
			jobSet.get(i).setRank(jobSet.get(i).getCurrentDeadline());	
		}

		Collections.sort(jobSet);

		double minEDFUtility =Double.MAX_VALUE;
		double totalEDFUtility= 0;
		double totalDelay=0;
		
		double minEDFUtilityHumanReadable =Double.MAX_VALUE;
		double totalEDFUtilityHumanReadable= 0;

		
		for (int i=0; i< jobSet.size();i++){
			int MapMultiplication = (int) (jobSet.get(i).getEstimatedMapExecTime()/TaskTimeGCF);
			int RedMultiplication = (int) (jobSet.get(i).getEstimatedRedExecTime()/TaskTimeGCF);
			double totalRequest= jobSet.get(i).getnumRemainingMapTasks()*MapMultiplication + jobSet.get(i).getnumRemainingRedTasks()*RedMultiplication;
			double delay = Math.ceil((totalRequest/capacity))*TaskTimeGCF;
			totalDelay = totalDelay+delay;
			double utilityTempHumanReadable = jobSet.get(i).calcUtilityHumanReadable(totalDelay);
			totalEDFUtilityHumanReadable= totalEDFUtilityHumanReadable+utilityTempHumanReadable;
			if (utilityTempHumanReadable<minEDFUtilityHumanReadable){
				minEDFUtilityHumanReadable = utilityTempHumanReadable;
			}

			double utilityTemp = jobSet.get(i).calcUtility(totalDelay);
			totalEDFUtility= totalEDFUtility+utilityTemp;
			if (utilityTemp<minEDFUtility){
				minEDFUtility = utilityTemp;
			}
			
			
		}


		minEDFUtility = Double.valueOf(dfd.format(minEDFUtility));
		totalEDFUtility = Double.valueOf(dfd.format(totalEDFUtility));

		
		minEDFUtilityHumanReadable = Double.valueOf(dfd.format(minEDFUtilityHumanReadable));
		totalEDFUtilityHumanReadable = Double.valueOf(dfd.format(totalEDFUtilityHumanReadable));

		
		
		resultArray.put("EDFMin", minEDFUtility);
		resultArray.put("EDFTotal", totalEDFUtility);
		
		resultArray.put("EDFMinHumanReadable", minEDFUtilityHumanReadable);
		resultArray.put("EDFTotalHumanReadable", totalEDFUtilityHumanReadable);
		
		

	}

	
	
	public static void getFIFOUtility( ArrayList<HadoopJobProxy> JobSet, HashMap<String, Double> resultArray){

		double TaskTimeGCF = MaxMinSchedulingPolicy.getTaskTimeGCF();
		double capacity = (double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		DecimalFormat dfd = new DecimalFormat("#.##");      
		
		///////////// FIFO////////////////////////////////////////////////
		for (int i=0; i< JobSet.size();i++){
			JobSet.get(i).setRank(JobSet.get(i).getTimetoJoin());
		}

		Collections.sort(JobSet);

		double minFIFOUtility =Double.MAX_VALUE;
		double totalFIFOUtility= 0;
		
		double minFIFOUtilityHumanReadable =Double.MAX_VALUE;
		double totalFIFOUtilityHumanReadable= 0;
		
		double totalDelay=0;
		for (int i=0; i< JobSet.size();i++){
			int MapMultiplication = (int) (JobSet.get(i).getEstimatedMapExecTime()/TaskTimeGCF);
			int RedMultiplication = (int) (JobSet.get(i).getEstimatedRedExecTime()/TaskTimeGCF);
			double totalRequest= JobSet.get(i).getnumRemainingMapTasks()*MapMultiplication + JobSet.get(i).getnumRemainingRedTasks()*RedMultiplication;
			double delay = Math.ceil((totalRequest/capacity))*TaskTimeGCF;
			totalDelay = totalDelay+delay;
			double utilityTemp = JobSet.get(i).calcUtility(totalDelay);
			totalFIFOUtility= totalFIFOUtility+utilityTemp;
			if (utilityTemp<minFIFOUtility){
				minFIFOUtility = utilityTemp;
			}

			double utilityTempHumanReadable = JobSet.get(i).calcUtilityHumanReadable(totalDelay);
			totalFIFOUtilityHumanReadable= totalFIFOUtilityHumanReadable+utilityTempHumanReadable;
			if (utilityTempHumanReadable<minFIFOUtilityHumanReadable){
				minFIFOUtilityHumanReadable = utilityTempHumanReadable;
			}
			

		}

		minFIFOUtility = Double.valueOf(dfd.format(minFIFOUtility));
		totalFIFOUtility = Double.valueOf(dfd.format(totalFIFOUtility));

		resultArray.put("FIFOMin", minFIFOUtility);
		resultArray.put("FIFOTotal", totalFIFOUtility);
		
		minFIFOUtilityHumanReadable = Double.valueOf(dfd.format(minFIFOUtilityHumanReadable));
		totalFIFOUtilityHumanReadable = Double.valueOf(dfd.format(totalFIFOUtilityHumanReadable));

		resultArray.put("FIFOMinHumanReadable", minFIFOUtilityHumanReadable);
		resultArray.put("FIFOTotalHumanReadable", totalFIFOUtilityHumanReadable);
		
		
		
	}
	
	
	
	public static void getRRHUtility( ArrayList<HadoopJobProxy> JobSet, HashMap<String, Double> resultArray){

		double TaskTimeGCF = MaxMinSchedulingPolicy.getTaskTimeGCF();
		double capacity = (double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");

		DecimalFormat dfd = new DecimalFormat("#.##");      
		
		

		///////////// RRH ////////////////////////////////////////////////

		double alpha= 0.3;
		double discountRate = 9;

		for (int i =0; i < JobSet.size(); i++){

			double yield = JobSet.get(i).calcUtilityHumanReadable(0);

			/*
			double MapRemainTime =  (currentJobs.get(i).getEstimatedMapExecTime()) *(double)currentJobs.get(i).getnumRemainingMapTasks() / (double)capacity;
			double RedRemainTime =  (currentJobs.get(i).getEstimatedRedExecTime()) *(double)currentJobs.get(i).getnumRemainingRedTasks() / (double)capacity;
			 */
			double MapRemainTime =  (JobSet.get(i).getEstimatedMapExecTime()) *Math.ceil((double)JobSet.get(i).getnumRemainingMapTasks() / (double)capacity);
			double RedRemainTime =  (JobSet.get(i).getEstimatedRedExecTime()) *Math.ceil((double)JobSet.get(i).getnumRemainingRedTasks() / (double)capacity);

			double RemainProcTime = MapRemainTime + RedRemainTime;
			double PV = yield /(1+(discountRate*RemainProcTime));


			double expireTime = JobSet.get(i).invertUtility(0);
			
			if (JobSet.get(i).getUtilityType()==HadoopJobProxy.linear){
				expireTime = Double.MAX_VALUE;
			}
			
			
			double cost=0;

			for (int j =0; j < JobSet.size(); j++){

				if (i!=j){
		
					double decayj = -JobSet.get(j).getPriority()*JobSet.get(j).getjobDeadlineSensitivity()/4;
					if (JobSet.get(j).getUtilityType()==HadoopJobProxy.linear){
						decayj = JobSet.get(j).getjobDeadlineSensitivity();
					}
					cost = cost + decayj*Math.min(RemainProcTime, expireTime);
				}
			}

			double reward = (alpha*PV + (1-alpha)*cost)/RemainProcTime;

			
			JobSet.get(i).setRank(reward);


		}

		Collections.sort(JobSet);


		double minRRHUtility =Double.MAX_VALUE;
		double totalRRHUtility= 0;
		
		double minRRHUtilityHumanReadable =Double.MAX_VALUE;
		double totalRRHUtilityHumanReadable= 0;
		
		double totalDelay=0;
		for (int i=0; i< JobSet.size();i++){
			int MapMultiplication = (int) (JobSet.get(i).getEstimatedMapExecTime()/TaskTimeGCF);
			int RedMultiplication = (int) (JobSet.get(i).getEstimatedRedExecTime()/TaskTimeGCF);
			double totalRequest= JobSet.get(i).getnumRemainingMapTasks()*MapMultiplication + JobSet.get(i).getnumRemainingRedTasks()*RedMultiplication;
			double delay = Math.ceil((totalRequest/capacity))*TaskTimeGCF;
			totalDelay = totalDelay+delay;
			double utilityTemp = JobSet.get(i).calcUtility(totalDelay);
			totalRRHUtility= totalRRHUtility+utilityTemp;
			if (utilityTemp<minRRHUtility){
				minRRHUtility = utilityTemp;
			}
			
			
			double utilityTempHumanReadable = JobSet.get(i).calcUtilityHumanReadable(totalDelay);
			totalRRHUtilityHumanReadable= totalRRHUtilityHumanReadable+utilityTempHumanReadable;
			if (utilityTempHumanReadable<minRRHUtilityHumanReadable){
				minRRHUtilityHumanReadable = utilityTempHumanReadable;
			}
		}


		minRRHUtility = Double.valueOf(dfd.format(minRRHUtility));
		totalRRHUtility = Double.valueOf(dfd.format(totalRRHUtility));


		resultArray.put("RRHMin", minRRHUtility);
		resultArray.put("RRHTotal", totalRRHUtility);
		
		
		minRRHUtilityHumanReadable = Double.valueOf(dfd.format(minRRHUtilityHumanReadable));
		totalRRHUtilityHumanReadable = Double.valueOf(dfd.format(totalRRHUtilityHumanReadable));


		resultArray.put("RRHMinHumanReadable", minRRHUtilityHumanReadable);
		resultArray.put("RRHTotalHumanReadable", totalRRHUtilityHumanReadable);
		
	

	}
	
	
	
	
	
	
	
	public static void getMaxMinUtility(ArrayList<HadoopJobProxy> currentJobs, HashMap<String, Integer> finishingTime, HashMap<String, Double> resultArray){

		double TaskTimeGCF = MaxMinSchedulingPolicy.getTaskTimeGCF();

		double totalUtility =  0;
		double minutility = Double.MAX_VALUE;
		
		double totalUtilityHumanReadable =  0;
		double minutilityHumanReadable  = Double.MAX_VALUE;
		
		
		for(int e=0; e<currentJobs.size(); e++){

		
			double temp = currentJobs.get(e).calcUtility(finishingTime.get(currentJobs.get(e).getJobName())*  TaskTimeGCF );
			totalUtility = totalUtility+ temp;
			if (temp < minutility){
				minutility = temp;
			}
			double tempHumanReadable = currentJobs.get(e).calcUtilityHumanReadable(finishingTime.get(currentJobs.get(e).getJobName())*  TaskTimeGCF );
			totalUtilityHumanReadable = totalUtilityHumanReadable+ tempHumanReadable;
			if (tempHumanReadable < minutilityHumanReadable){
				minutilityHumanReadable = tempHumanReadable;
			}

		}

		DecimalFormat dfd = new DecimalFormat("#.##");      
		minutility = Double.valueOf(dfd.format(minutility));
		totalUtility = Double.valueOf(dfd.format(totalUtility));
		
		resultArray.put("MMFMin", minutility);
		resultArray.put("MMFTotal", totalUtility);
		
		minutilityHumanReadable = Double.valueOf(dfd.format(minutilityHumanReadable));
		totalUtilityHumanReadable = Double.valueOf(dfd.format(totalUtilityHumanReadable));
		
		resultArray.put("MMFMinHumanReadable", minutilityHumanReadable);
		resultArray.put("MMFTotalHumanReadable", totalUtilityHumanReadable);
		
		
		
		/*
		if (minutilityHumanReadable<0){
		
			JobStatusUpdateThread.increaseDDI();
			
		}
		*/


	}
	


public static void getFairUtility( ArrayList<HadoopJobProxy> JobSet, HashMap<String, Double> resultArray){

		double totalPriority=0;
		double capacity = (double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		double TaskTimeGCF = MaxMinSchedulingPolicy.getTaskTimeGCF();
		
		DecimalFormat dfd = new DecimalFormat("#.##");      

		double minFairUtility =Double.MAX_VALUE;
		double totalFairUtility= 0;
		
		double minFairUtilityHumanReadable =Double.MAX_VALUE;
		double totalFairUtilityHumanReadable= 0;
		
		
		double totalDelay=0;
		
		HashMap<HadoopJobProxy,Double> remainigTask= new HashMap<HadoopJobProxy,Double>();
		for (int i=0; i< JobSet.size();i++){
			int MapMultiplication = (int) (JobSet.get(i).getEstimatedMapExecTime()/TaskTimeGCF);
			int RedMultiplication = (int) (JobSet.get(i).getEstimatedRedExecTime()/TaskTimeGCF);
			double totalRequest= JobSet.get(i).getnumRemainingMapTasks()*MapMultiplication + JobSet.get(i).getnumRemainingRedTasks()*RedMultiplication;
			remainigTask.put(JobSet.get(i), totalRequest);
			totalPriority=totalPriority+JobSet.get(i).getPriority();
		}
		
		while (JobSet.size()!=0){
			double minTime=Double.MAX_VALUE;
			int minIndex=0;
			
			for (int i=0; i< JobSet.size();i++){
				double remainTasksTemp= remainigTask.get(JobSet.get(i));
				double weightedCapacity = Math.floor((capacity*JobSet.get(i).getPriority()/totalPriority));
				double delay = Math.ceil((remainTasksTemp/weightedCapacity))*TaskTimeGCF;
				if (delay<minTime){
					minTime= delay;
					minIndex = i;
				}	
			}
			
			totalDelay= totalDelay+ minTime;
			double utilityTemp = JobSet.get(minIndex).calcUtility(totalDelay);
			totalFairUtility= totalFairUtility+utilityTemp;
			if (utilityTemp<minFairUtility){
				minFairUtility = utilityTemp;
			}
			
			double utilityTempHumanReadable = JobSet.get(minIndex).calcUtilityHumanReadable(totalDelay);
			totalFairUtilityHumanReadable= totalFairUtilityHumanReadable+utilityTempHumanReadable;
			if (utilityTempHumanReadable<minFairUtilityHumanReadable){
				minFairUtilityHumanReadable = utilityTempHumanReadable;
			}
			
			
			for (int i=0; i< JobSet.size();i++){
				double remainTasksTemp= remainigTask.get(JobSet.get(i));
				double weightedCapacity = Math.floor((capacity*JobSet.get(i).getPriority()/totalPriority));
				remainTasksTemp = Math.ceil(remainTasksTemp- minTime*weightedCapacity);
				remainigTask.put(JobSet.get(i), remainTasksTemp);
			}
			
			totalPriority= totalPriority- JobSet.get(minIndex).getPriority();
			remainigTask.remove(JobSet.get(minIndex));
			JobSet.remove(minIndex);

		}

		minFairUtility = Double.valueOf(dfd.format(minFairUtility));
		totalFairUtility = Double.valueOf(dfd.format(totalFairUtility));

		resultArray.put("FairMin", minFairUtility);
		resultArray.put("FairTotal", totalFairUtility);
		
		minFairUtilityHumanReadable = Double.valueOf(dfd.format(minFairUtilityHumanReadable));
		totalFairUtilityHumanReadable = Double.valueOf(dfd.format(totalFairUtilityHumanReadable));

		resultArray.put("FairMinHumanReadable", minFairUtilityHumanReadable);
		resultArray.put("FairTotalHumanReadable", totalFairUtilityHumanReadable);
		
		
	}



	
	
	public static void PrintUtility( ArrayList<HadoopJobProxy> CurrentJobSet){
		
		if(CurrentJobSet.size()>1){
			
			ArrayList<HadoopJobProxy> JobSet = new ArrayList<HadoopJobProxy>();
			for (int i =0; i < CurrentJobSet.size(); i ++){
				HadoopJobProxy temp = new HadoopJobProxy(CurrentJobSet.get(i));
				JobSet.add(temp);
			}
			
			HashMap<String, Double> resultArray = new HashMap<String, Double>();
			HashMap<String, Integer> ExpectedFinishingTimeSlot = MaxMinSchedulingPolicy.getexpectedFinishingTimeSlot();

			getMaxMinUtility( JobSet,ExpectedFinishingTimeSlot,resultArray);
			getEDFUtility(JobSet, resultArray);
			
			getFIFOUtility(JobSet, resultArray);
			getRRHUtility(JobSet, resultArray);
			
			
			// Fair scheudling simulation must be at the last position since it will remove all the job proxy object.
			getFairUtility(JobSet, resultArray);
			

			UtilityLogger.info("	" +resultArray.get("MMFMinHumanReadable")+ "	" + resultArray.get("MMFTotalHumanReadable")+ "	"+ resultArray.get("EDFMinHumanReadable") + "	" + resultArray.get("EDFTotalHumanReadable")
					+"	"+resultArray.get("FairMinHumanReadable")+"	"+resultArray.get("FairTotalHumanReadable")+"	"+resultArray.get("RRHMinHumanReadable")+"	"+resultArray.get("RRHTotalHumanReadable")
					+"	"+ resultArray.get("FIFOMinHumanReadable")+"	"+resultArray.get("FIFOTotalHumanReadable")+"	|	"+resultArray.get("MMFMin")+ "	" + resultArray.get("MMFTotal")+ "	"+ resultArray.get("EDFMin") + "	" + resultArray.get("EDFTotal")
					+"	"+resultArray.get("FairMin")+"	"+resultArray.get("FairTotal")+"	"+resultArray.get("RRHMin")+"	"+resultArray.get("RRHTotal")
					+"	"+ resultArray.get("FIFOMin")+"	"+resultArray.get("FIFOTotal")	+"\n");
		}

	}

}
