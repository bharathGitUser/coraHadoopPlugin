package org.apache.hadoop.expmanager.schedulingpolicy;

import java.util.ArrayList;

import org.apache.hadoop.expmanager.HadoopJobProxy;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;

public class RiskRewardHeuristic implements SchedulingPolicy{

	private double discountRate = 9;
	private int capacity = ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");

	private String CurrentJobName="";





	@Override
	public boolean updateWeights(ArrayList<HadoopJobProxy> currentJobs,	double[] weights) {
		boolean Recalculate=true;
		int ScheduleIndex=0;
		for (int i =0; i < currentJobs.size(); i++){
			if (currentJobs.get(i).getJobName().equals(CurrentJobName)){

				Recalculate=false;
				ScheduleIndex= i;

			}
		}


		if (Recalculate){

			//double [] reward = new double [currentJobs.size()];
			double alpha= 0.3;
			double largestReward=Double.MIN_VALUE;
			int SelectedIndex=0;

			for (int i =0; i < currentJobs.size(); i++){

				double yield = currentJobs.get(i).calcUtility(0);
				/*
			double MapRemainTime =  (currentJobs.get(i).getEstimatedMapExecTime()) *(double)currentJobs.get(i).getnumRemainingMapTasks() / (double)capacity;
			double RedRemainTime =  (currentJobs.get(i).getEstimatedRedExecTime()) *(double)currentJobs.get(i).getnumRemainingRedTasks() / (double)capacity;
				 */
				double MapRemainTime =  (currentJobs.get(i).getEstimatedMapExecTime()) *Math.ceil((double)currentJobs.get(i).getnumRemainingMapTasks() / (double)capacity);
				double RedRemainTime =  (currentJobs.get(i).getEstimatedRedExecTime()) *Math.ceil((double)currentJobs.get(i).getnumRemainingRedTasks() / (double)capacity);


				double RemainProcTime = MapRemainTime + RedRemainTime;
				double PV = yield /(1+(discountRate*RemainProcTime));

				
				double expireTime = currentJobs.get(i).invertUtility(0);
				
				if (currentJobs.get(i).getUtilityType()==HadoopJobProxy.linear){
					expireTime = Double.MAX_VALUE;
				}
				
				

				double cost=0;

				for (int j =0; j < currentJobs.size(); j++){

					if (i!=j){
						double decayj = -currentJobs.get(j).getPriority()*currentJobs.get(j).getjobDeadlineSensitivity()/4;
						if (currentJobs.get(j).getUtilityType()==HadoopJobProxy.linear){
							decayj = currentJobs.get(j).getjobDeadlineSensitivity();
						}
						cost = cost + decayj*Math.min(RemainProcTime, expireTime);
					}
				}

				double reward = (alpha*PV + (1-alpha)*cost)/RemainProcTime;

				if (reward>largestReward){
					largestReward = reward;
					SelectedIndex = i;
				}


			}


			weights[SelectedIndex]=100;
			CurrentJobName = currentJobs.get(SelectedIndex).getJobName();
		}else{
			weights[ScheduleIndex]=100;

		}
		return true;
	}

	@Override
	public String getPolicyName() {

		return "RRH";
	}

}
