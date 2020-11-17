package org.apache.hadoop.expmanager.schedulingpolicy;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Random;

import org.apache.hadoop.expmanager.HadoopJobProxy;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;

//simply assigns equal weights to all jobs such that their sum is 99
public class SimpleSchedPolicies implements SchedulingPolicy{
	public static final int FAIR = 1;
	public static final int RANDOM = 2;
	public static final int EDF = 3;
	public static final int EQU = 4;
	public static final int FIFO =5;
	private int schedPolicy =0; 

	private boolean UpdateLock=false;

	private boolean enableDebug = false;

	public SimpleSchedPolicies(int schedPolicy){
		this.schedPolicy = schedPolicy;
	}
	public String getPolicyName(){
		switch(schedPolicy){
		case FAIR:
			return "Fair";
		case RANDOM:
			return "Rand";
		case EDF:
			return "EDF";
		case EQU:
			return "EQU";
		case FIFO:
			return "FIFO";
		}
		return "ERROR";
	}


	public void lockUpdate(){
		UpdateLock =true;
	}

	public void unlockUpdate(){
		UpdateLock =false;
	}


	public boolean isUpdateLocked(){
		return UpdateLock;
	}

	public boolean updateWeights(ArrayList<HadoopJobProxy> currentJobs, double[] weights) {

		double totalsum=0;
		switch(schedPolicy){
		case FIFO:
			weights[0]=100;
			break;
		case EQU:

			if(enableDebug){
				double numRunningJob=0;

				for(int i=0; i < currentJobs.size();++i){
					if(currentJobs.get(i).isRunning()){
						numRunningJob++;
					}
				}


				if (numRunningJob==0){
					unlockUpdate();
				}else{
					lockUpdate();
				}
			}


			if(!isUpdateLocked()){


				for(int i =0 ; i < currentJobs.size(); ++i){

					DecimalFormat df = new DecimalFormat("#.##");      

					weights[i] = 100/(double)currentJobs.size();

					weights[i] = Double.valueOf(df.format(weights[i]));

					totalsum=totalsum+weights[i];


					//System.out.println("Weight: "+weights[i]);
				}
			}else{
				for(int i =0 ; i < currentJobs.size(); ++i){
					weights[i]=(double)(currentJobs.get(i).getmapSlotsAssigned()+ currentJobs.get(i).getredSlotsAssigned())*100/
							(double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");

				}
			}

			break;
		case FAIR: //Fair Scheduling





			if(enableDebug){
				double numRunningJob=0;

				for(int i=0; i < currentJobs.size();++i){
					if(currentJobs.get(i).isRunning()){
						numRunningJob++;
					}
				}


				if (numRunningJob==0){
					unlockUpdate();
				}else{
					lockUpdate();
				}
			}


			if(!isUpdateLocked()){
				double totalPriority = 0;
				for(int i=0; i < currentJobs.size();++i){
					totalPriority = totalPriority + currentJobs.get(i).getPriority();
					weights[i]=5;

				}
				for(int i =0 ; i < currentJobs.size(); ++i){
					double priority = currentJobs.get(i).getPriority();
					int temp = (int)((100.0-currentJobs.size()*5)*priority/totalPriority);
					weights[i] = weights[i]+ temp;
					//DecimalFormat df = new DecimalFormat("#.##");      
					//weights[i] = Double.valueOf(df.format(weights[i]));
					//totalsum=totalsum+weights[i];

				}

			}else{
				for(int i =0 ; i < currentJobs.size(); ++i){
					weights[i]=(double)(currentJobs.get(i).getmapSlotsAssigned()+ currentJobs.get(i).getredSlotsAssigned())*100/
							(double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");

				}
			}



			break;
		case RANDOM: //Random Scheduling
			//double[] temp = Util.genNumbersThatSumToInput(weights.length,99); 

			if(enableDebug){
				double numRunningJob=0;

				for(int i=0; i < currentJobs.size();++i){
					if(currentJobs.get(i).isRunning()){
						numRunningJob++;
					}
				}


				if (numRunningJob==0){
					unlockUpdate();
				}else{
					lockUpdate();
				}
			}

			if(!isUpdateLocked()){
				double sum=0;
				Random random = new Random(new Random().nextInt());
				for(int i=0; i < weights.length;++i){
					weights[i] = random.nextDouble()*100;
					sum=sum+weights[i];


				}
				for(int i =0; i < weights.length; ++i){
					weights[i]=weights[i]/sum;
					DecimalFormat df = new DecimalFormat("#.##");      
					weights[i] = Double.valueOf(df.format(weights[i]));
					totalsum=totalsum+weights[i];

				}

			}else{
				for(int i =0 ; i < currentJobs.size(); ++i){
					weights[i]=(double)(currentJobs.get(i).getmapSlotsAssigned()+ currentJobs.get(i).getredSlotsAssigned())*100/
							(double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");

				}
			}

			break;



		case EDF://Earliest Deadline First Scheduling
			HadoopJobProxy jobWithEarliestDeadline = currentJobs.get(0);
			int associatedIndex = 0;
			//System.out.println(currentJobs.get(0).getJobName()+ "	helo "+ currentJobs.get(0).getCurrentDeadline());
			for(int i =1; i < currentJobs.size(); ++i){

				//System.out.println(currentJobs.get(i).getJobName()+ "	helo "+ currentJobs.get(i).getCurrentDeadline());


				if(currentJobs.get(i).getJobDeadlinewhenSubmit()+currentJobs.get(i).getTimetoJoin()
						< jobWithEarliestDeadline.getJobDeadlinewhenSubmit()+ jobWithEarliestDeadline.getTimetoJoin()){
					jobWithEarliestDeadline = currentJobs.get(i);
					associatedIndex = i;
				} 
			}

			for(int i =0; i < weights.length; ++i){
				if(i == associatedIndex)
					weights[i] = 100;
				else
					weights[i] = 0;

			}
			break;	
		}


		while(totalsum>100){
			for(int i =0 ; i < currentJobs.size(); ++i){
				if(weights[i]>0){
					weights[i]=weights[i]-0.01;
					totalsum=totalsum-0.01;
				}
			}
		}








		return true; 
	}
}