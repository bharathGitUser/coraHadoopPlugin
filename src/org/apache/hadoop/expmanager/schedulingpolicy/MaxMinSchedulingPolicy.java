package org.apache.hadoop.expmanager.schedulingpolicy;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.HadoopJobProxy;
import org.apache.hadoop.expmanager.JobManager;
import org.apache.hadoop.expmanager.schedulingpolicy.SchedulingPolicy;
import org.apache.hadoop.expmanager.utility.ConstantWarehouse;




public class MaxMinSchedulingPolicy implements SchedulingPolicy{

	private int currentIteration;
	private static double TaskTimeGCF=(double)ConstantWarehouse.getIntegerValue("SmallestGCF"); 
	private int numTimeSlots;
	private Log GeneralLogger = LogFactory.getLog("JobManagerlogger");
	private Log TimelineLogger = LogFactory.getLog("TimelineLogger");
	private static HashMap<String, Integer> expectedFinishingTimeSlot = new HashMap<String, Integer>();


	private static String outputPath = ConstantWarehouse.getStringValue("MosekLogPath");
	private static boolean enableDebugInfo = false;


	public MaxMinSchedulingPolicy(){
		currentIteration=0;
		if(!outputPath.endsWith("/")){
			outputPath=outputPath+"/";
		}

		File file = new File(outputPath);

		boolean FolderCreationResult = false;

		if (!file.exists()) {
			FolderCreationResult = file.mkdirs();
			if (!FolderCreationResult){
				GeneralLogger.error("Failed to create Mosek log directory");

			}
		}


		if (ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster")!=ConstantWarehouse.getIntegerValue("totalReduceSlotsInCluster")){
			GeneralLogger.error("The current version of the MMF scheduler requires the ReduceSlot number to be the same as the MapSlot number. Please check your system setting.");
			System.exit(1);

		}
		

	}






	private static double gcd(double a, double b)
	{
		while (b > 0)
		{
			double temp = b;
			b = a % b; // % is remainder
			a = temp;
		}
		return a;
	}

	private static double gcd(Double[] listtemp)
	{
		double result = listtemp[0];
		for(int i = 1; i < listtemp.length; i++) result = gcd(result, listtemp[i]);
		return result;
	}



	public static double getGreatestCommonFactorTaskTime(ArrayList<HadoopJobProxy> currentJobs){



		ArrayList<Double> temp = new ArrayList<Double>();
		for (Iterator<HadoopJobProxy> iterator = currentJobs.iterator(); iterator.hasNext();) {
			HadoopJobProxy job = (HadoopJobProxy) iterator.next();

			temp.add(job.getEstimatedMapExecTime());
			temp.add(job.getEstimatedRedExecTime());
		}
		Double[] listtemp = new Double[temp.size()];
		listtemp = temp.toArray(listtemp);
		return gcd(listtemp);
	}

	public static double getTaskTimeGCF(){

		return TaskTimeGCF;
	}

	
	
	public static HashMap<String, Integer> getexpectedFinishingTimeSlot(){

		return expectedFinishingTimeSlot;
	}
	
	
	

	public int estimateNumOfTimeSlots(ArrayList<HadoopJobProxy> currentJobs){

		//int maxDeadline=0;

		double EquivalentMapTasks=0;
		double EquivalentRedTasks=0;


		double totalMapRequest=0;
		double totalRedRequest=0;

		for (Iterator<HadoopJobProxy> iterator = currentJobs.iterator(); iterator.hasNext();) {
			HadoopJobProxy job = (HadoopJobProxy) iterator.next();
			int MapMultiplication = (int) (job.getEstimatedMapExecTime()/TaskTimeGCF);
			int RedMultiplication = (int) (job.getEstimatedRedExecTime()/TaskTimeGCF);
			EquivalentMapTasks=(double)(job.getmapTaskRequested()*MapMultiplication);
			EquivalentRedTasks= (double)(job.getredTaskRequested()*RedMultiplication);
			totalMapRequest = totalMapRequest+ EquivalentMapTasks;
			totalRedRequest = totalRedRequest + EquivalentRedTasks;
		}
		int re=(int) Math.ceil( totalMapRequest/(double)ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster") + totalRedRequest/(double)ConstantWarehouse.getIntegerValue("totalReduceSlotsInCluster"));

		//return 5;
		return re;

	}







	public double[] reOptimize(ArrayList<HadoopJobProxy> currentJobs){

		//////////////////////   Scale of problem ///////////////////////////
		//int numTimeSlots=estimateNumOfTimeSlots(currentJobs);
		int numJobs=currentJobs.size();
		int numVar=4*numTimeSlots* numJobs;
		int numCon=numJobs+numTimeSlots + 2*numTimeSlots* numJobs;

		int startPosLambda=numTimeSlots* numJobs;



		/////////////////////////////////////////////////////////////////////

		currentIteration++;


		mosek.Env
		env = null;
		mosek.Task
		task = null;




		try{	 

			env  = new mosek.Env ();
			task = new mosek.Task (env, numCon, numVar);


			if(ConstantWarehouse.getBooleanValue("EnableMosekDebugOutput")){

				task.set_Stream(mosek.Env.streamtype.log, 
						new mosek.Stream()  
				{ public void stream(String msg) { System.out.print(msg); }}); 
			}

			task.appendcons(numCon);
			task.appendvars(numVar);


			///////////////////////////////////////////////////////////
			// put objective coefficient

			//////////Calculate offset ///////////////

			double SmallestUtil =0;
			for(int j=0; j<currentJobs.size(); ++j){
				double Util = currentJobs.get(j).calcUtility(numTimeSlots*TaskTimeGCF);
				if (Util<SmallestUtil){
					SmallestUtil= Util;
				}
			}
			
			double MaxObjCoeff= Math.exp(-Math.log(startPosLambda)*SmallestUtil);
			
			double offSet = 1;

			while (MaxObjCoeff*offSet>1000000000){
				offSet = offSet/10;
			}
			////////////////////////////////////////////////
			
			for(int j=0; j<startPosLambda; ++j){
				int LamZero=startPosLambda+3*j;
				int jobID=(j/numTimeSlots);
				int timeID=j-(jobID)*numTimeSlots+1;
				double jobUtil=currentJobs.get(jobID).calcUtility(timeID*TaskTimeGCF);
				//double temp = -Math.log(startPosLambda);
				double ObjCoeff= Math.exp(-Math.log(startPosLambda)*jobUtil);//*100;//+ 0.001*(double)timeID/(double)currentJobs.get(jobID).getCurrentDeadline();


				


				task.putcj(LamZero,offSet);
				task.putcj(LamZero+1,ObjCoeff*offSet);
				task.putcj(LamZero+2,ObjCoeff*offSet);

			}





			///////////////////////////////////////////////////////////////// 
			// Put constraint bounds.
			// 1. Capacity Constraint
			int constraintIndexOffset=0;
			for(int i=0; i<numTimeSlots; i++){
				task.putbound(mosek.Env.accmode.con,i,mosek.Env.boundkey.up,0,ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster"));
			}

			// 2. Demand Constraint
			constraintIndexOffset=numTimeSlots;



			for(int i=0; i<numJobs; i++){
				//double totalRequest=0;

				int MapMultiplication = (int) (currentJobs.get(i).getEstimatedMapExecTime()/TaskTimeGCF);
				int RedMultiplication = (int) (currentJobs.get(i).getEstimatedRedExecTime()/TaskTimeGCF);
				double totalRequest=currentJobs.get(i).getnumRemainingMapTasks()*MapMultiplication + currentJobs.get(i).getnumRemainingRedTasks()*RedMultiplication;
				//totalRequest=totalRequest+currentJobs.get(i).getmapTaskRequested()*MapMultiplication + currentJobs.get(i).getredTaskRequested()*RedMultiplication;
				
				
				
				
				
				
				task.putbound(mosek.Env.accmode.con,constraintIndexOffset+i,mosek.Env.boundkey.fx,totalRequest,totalRequest);
			}

			// 3. Lambda X Constraint
			constraintIndexOffset=numTimeSlots+numJobs;
			for(int i=0; i<startPosLambda; i++){
				task.putbound(mosek.Env.accmode.con, constraintIndexOffset+i,mosek.Env.boundkey.fx,0,0);
			}

			// 4. Lambda sum Constraint
			constraintIndexOffset=numTimeSlots+numJobs + startPosLambda;
			for(int i=0; i<startPosLambda; i++){
				task.putbound(mosek.Env.accmode.con, constraintIndexOffset+i,mosek.Env.boundkey.fx,1,1);
			}



			///////////////////////////////////////////////////////////////// 
			// Put variable bounds.
			for(int i=0; i<startPosLambda; i++){
				int jobIndex=(i/numTimeSlots);
				int timeIndex=i-(jobIndex)*numTimeSlots;

				task.putbound(mosek.Env.accmode.var,i,mosek.Env.boundkey.ra,0,ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster"));
				task.putvarname(i,"x"+jobIndex+"_"+timeIndex);

			}

			int variableIndexOffset=startPosLambda;
			for(int i=0; i<3*startPosLambda; i++){
				int jobIndex=(i/(3*numTimeSlots));
				int timeIndex=(i-3*jobIndex*numTimeSlots)/3;
				int typeIndex=i-3*jobIndex*numTimeSlots-3*timeIndex;

				task.putbound(mosek.Env.accmode.var,variableIndexOffset+i,mosek.Env.boundkey.lo,0,1);
				task.putvarname(i+startPosLambda,"L"+jobIndex+"_"+timeIndex+"_"+typeIndex);
				//System.out.println(i+startPosLambda);
				//System.out.println("l"+jobIndex+timeIndex+typeIndex);
			}


			///////////////////////////////////////////////////////////////// 
			// Put constraint matrix.

			for(int j=0; j<numVar; j++){
				int[] coeffIndexSet = null;
				double[]coefficientSet = null;
				if(j<startPosLambda){
					// constraint coefficients for Xit
					int jobIndex=(j/numTimeSlots);
					int timeIndex=j-(jobIndex)*numTimeSlots;

					int CapacityConstraintIndex=timeIndex;
					int DemandConstraintIndex=numTimeSlots+jobIndex;
					int LambdaConstraintIndex=numTimeSlots+numJobs + jobIndex*numTimeSlots + timeIndex;

					coeffIndexSet=new int[3];
					coefficientSet=new double[3];

					coeffIndexSet[0]=CapacityConstraintIndex;
					coeffIndexSet[1]=DemandConstraintIndex;
					coeffIndexSet[2]=LambdaConstraintIndex;

					coefficientSet[0]=1;
					coefficientSet[1]=1;
					coefficientSet[2]=-1;


				}else{
					int LambdaIndex=j-startPosLambda;
					int jobIndex=(LambdaIndex/(3*numTimeSlots));
					int timeIndex=(LambdaIndex-3*jobIndex*numTimeSlots)/3;
					int typeIndex=LambdaIndex-3*jobIndex*numTimeSlots-3*timeIndex;

					int LambdaXConstraintIndex=numTimeSlots+numJobs + jobIndex*numTimeSlots + timeIndex;
					int LambdaSumConstraintIndex=numTimeSlots+numJobs + numJobs*numTimeSlots + jobIndex*numTimeSlots + timeIndex;


					switch (typeIndex) {
					case 0:
						coeffIndexSet=new int[1];
						coefficientSet=new double[1];
						coeffIndexSet[0]=LambdaSumConstraintIndex;
						coefficientSet[0]=1;

						break;
					case 1:

						coeffIndexSet=new int[2];
						coefficientSet=new double[2];
						coeffIndexSet[0]=LambdaXConstraintIndex;
						coefficientSet[0]=1;
						coeffIndexSet[1]=LambdaSumConstraintIndex;
						coefficientSet[1]=1;

						break;
					case 2:
						coeffIndexSet=new int[2];
						coefficientSet=new double[2];
						coeffIndexSet[0]=LambdaXConstraintIndex;
						coefficientSet[0]=ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
						coeffIndexSet[1]=LambdaSumConstraintIndex;
						coefficientSet[1]=1;
						break;

					default: 
						break;
					}       
				}

				if((coeffIndexSet!=null)&&(coefficientSet!=null)){
					task.putacol(j,coeffIndexSet,coefficientSet);
					//System.out.println(task.getvarname(j));
				}

			}


			///////////////////////////////////////////////////////////////// 
			/* A minimization problem */ 
			task.putobjsense(mosek.Env.objsense.minimize);
			@SuppressWarnings("unused")
			mosek.Env.rescode termcode;

			task.putintparam(mosek.Env.iparam.optimizer,mosek.Env.optimizertype.primal_simplex.value);


			///////////////////////////////////////////////////////////////// 
			//optimization


			termcode = task.optimize();                
			task.solutionsummary(mosek.Env.streamtype.log);

			mosek.Env.solsta solsta[] = new mosek.Env.solsta[1];

			/* Get status information about the solution */ 
			task.getsolsta(mosek.Env.soltype.bas,solsta);


			switch(solsta[0])
			{

			case dual_infeas_cer:
				GeneralLogger.error("MMF solution is dual infeasible. Program terminated.");
				System.exit(1);
				break;
			case prim_infeas_cer:
				GeneralLogger.error("MMF solution is primal infeasible. Program terminated.");
				System.exit(1);
				break;
			case near_dual_infeas_cer:
				GeneralLogger.error("MMF solution is near dual infeasible. Program terminated.");
				System.exit(1);
				break;
			case near_prim_infeas_cer:  
				GeneralLogger.error("MMF solution is near primal infeasible. Program terminated.");
				System.exit(1);
				break;
			case unknown:
				GeneralLogger.error("MMF solution returns unknown status. Program terminated.");
				System.exit(1);
				break;
			default:
				break;
			}
			//printResidualCapacity(task);

			
			printProblem(task,currentJobs);
			printSolution(task,currentJobs);
			printCurrentJobStatus(currentJobs);

			double[] solution=new double[numVar];
			task.getxx(mosek.Env.soltype.bas,solution);
			UpdateExpectedJobFinishingTimeSlot(solution,currentJobs);
		
			if (enableDebugInfo){
				printFinishingTime(currentJobs);
			}
			return solution;
		} 
		catch (mosek.Exception e)
		/* Catch both Error and Warning */
		{
			GeneralLogger.error("Mosek returns an error.");

			GeneralLogger.error(e.getMessage ());
			throw e;
		}
		finally
		{          
			if (task != null) task.dispose ();
			if (env  != null)  env.dispose ();
		}




	}













	public boolean updateWeights(ArrayList<HadoopJobProxy> currentJobs, double[] weights) {
		TaskTimeGCF = getGreatestCommonFactorTaskTime(currentJobs);
		numTimeSlots = estimateNumOfTimeSlots(currentJobs);
		double capacity = ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		if(currentJobs.size()>1){

			int jobSize=currentJobs.size();
			//int numTimeSlots=estimateNumOfTimeSlots(currentJobs);

			/////////////////////////// Discretized /////////////////////////////////////////////////////////////



			double sum=0;
			int smallestTaskSLotNum = Integer.MAX_VALUE;
			for(int i =0 ; i < jobSize; ++i){
				if (currentJobs.get(i).isMapDone()){
					int slotNum= (int) (currentJobs.get(i).getEstimatedRedExecTime()/TaskTimeGCF);
					if (smallestTaskSLotNum>slotNum){
						smallestTaskSLotNum=slotNum;
					}


				}else{
					int slotNum= (int) (currentJobs.get(i).getEstimatedMapExecTime()/TaskTimeGCF);
					if (smallestTaskSLotNum>slotNum){
						smallestTaskSLotNum=slotNum;
					}

				}
				weights[i]=0;
			}


			int[] AllocationVector= getAllocationVector(reOptimize(currentJobs),currentJobs, smallestTaskSLotNum);


			String logString ="Allocation Vector: ";
			for(int i =0 ; i < jobSize; ++i){

				weights[i]= ((double)AllocationVector[i]*100)/capacity;
				sum= sum+ weights[i];
				logString= logString+" "+currentJobs.get(i).getJobName()+" "+weights[i];
			}

			logString = logString + " TimeSlot " + numTimeSlots + " GCF "+ TaskTimeGCF;

			TimelineLogger.info(logString);

			if(sum>100){
				GeneralLogger.error("Max-Min solutions sum up larger than 100.");
				return false;
			}

			return true;
		}else{
			weights[0]=100;
			return true;

		}


	}








	public void UpdateExpectedJobFinishingTimeSlot(double[] optimalSolution, ArrayList<HadoopJobProxy> currentJobs){

		expectedFinishingTimeSlot.clear();
	

		for(int i=0; i<currentJobs.size(); i++){
			int time=0;
			for(int j=0; j<numTimeSlots; j++){
				int index= i*numTimeSlots+j;
				if(Math.round(optimalSolution[index])>0){
					time=j;	
				}
			}		
			expectedFinishingTimeSlot.put(currentJobs.get(i).getJobName(), time);
		}

	}



	/*
	public int[] getAllocationVector(double[] optimalSolution, ArrayList<HadoopJobProxy> currentJobs, int numSlots){


		//int numTimeSlots=estimateNumOfTimeSlots(currentJobs);
		int numJobs=currentJobs.size();

		int[] Allocation= new int[numJobs];


		int totalCap=0;
		int maxCap=0; 
		int maxJobIndex=-1;
		int remainNonzero = ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		for(int i=0; i<numJobs; i++){
			double valueTemp=0;
			for (int j = 0; j<numSlots; j++){
				valueTemp = valueTemp+ Math.round(optimalSolution[i*numTimeSlots + j]);
			}
			valueTemp= valueTemp/(double)numSlots;
			Allocation[i] = (int)(valueTemp - (valueTemp%1));

			// 5% rule of Hadoop capacity scheduler requires that the minimum number of slots in a queue is 2. 
			if (Allocation[i]<2){
				Allocation[i] = 2;
				remainNonzero=remainNonzero-2;
			}

			totalCap = totalCap+ Allocation[i];
			if (maxCap<Allocation[i]){
				maxCap= Allocation[i];
				maxJobIndex= i;
			}
		}

		int remainingCap= ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster") - totalCap;

		Allocation[maxJobIndex] = Allocation[maxJobIndex]+ remainingCap;

		return Allocation;
	}
	 */



	public int[] getAllocationVector(double[] optimalSolution, ArrayList<HadoopJobProxy> currentJobs, int numSlots){


		//int numTimeSlots=estimateNumOfTimeSlots(currentJobs);
		int numJobs=currentJobs.size();

		int[] Allocation= new int[numJobs];
		double[] AllocationTemp= new double[numJobs];

		double totalDominantCap=0;
		double remainNonzero =ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");

		for(int i=0; i<numJobs; i++){
			double valueTemp=0;
			for (int j = 0; j<numSlots; j++){
				valueTemp = valueTemp+ Math.round(optimalSolution[i*numTimeSlots + j]);
			}
			AllocationTemp[i]= valueTemp/(double)numSlots;


			// 5% rule of Hadoop capacity scheduler requires that the minimum number of slots in a queue is 2. 
			if (AllocationTemp[i]<2){
				AllocationTemp[i] = 2;
				remainNonzero=remainNonzero-2;
			}else{
				totalDominantCap = totalDominantCap+AllocationTemp[i];
			}

		}


		int maxCap=0;
		int maxJobIndex=-1;
		int totalCap=0;


		for(int i=0; i<numJobs; i++){


			if (AllocationTemp[i]>2){
				double temp = (AllocationTemp[i]*remainNonzero/totalDominantCap);
				Allocation[i] = (int)(temp-temp%1);
				if (Allocation[i]<2){
					Allocation[i]=2;
				}

			}else{
				Allocation[i]=(int)AllocationTemp[i];
			}
			totalCap = totalCap+Allocation[i];


			if (maxCap<Allocation[i]){
				maxCap= Allocation[i];
				maxJobIndex= i;
			}
		}




		int remainingCap= ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster") - totalCap;

		Allocation[maxJobIndex] = Allocation[maxJobIndex]+ remainingCap;

		return Allocation;
	}

	public int[] getResidualCapacity(double[] optimalSolution, ArrayList<HadoopJobProxy> currentJobs){


		//	int numTimeSlots=estimateNumOfTimeSlots(currentJobs);
		int numJobs=currentJobs.size();
		int[]ResidualCapa= new int[numTimeSlots];


		for(int j=0; j<numTimeSlots; j++){
			ResidualCapa[j]=ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
			for(int i=0; i<numJobs; i++){
				int index= i*numTimeSlots+j;
				//System.out.println(ResidualCapa[j]);
				ResidualCapa[j]=ResidualCapa[j]-(int)Math.round(optimalSolution[index]);

			}

		}

		return ResidualCapa;
	}





	


	
	
	
	
	
	
	
	
	
	
	/*
	public void getEDFUtility( ArrayList<HadoopJobProxy> currentJobs, double[] edf){

		ArrayList<HadoopJobProxy> jobTemp = new ArrayList<HadoopJobProxy>();

		for (int i=0; i< currentJobs.size();i++){
			HadoopJobProxy temp = new HadoopJobProxy(currentJobs.get(i));
			temp.setRank(temp.getCurrentDeadline());
			jobTemp.add(temp);
		}




		Collections.sort(jobTemp);

		edf[1]=0;
		int capacity = ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		double time=0;

		double minimumUtility =Double.MAX_VALUE;

		for (int i=0; i< jobTemp.size();i++){
			double MapRemainTime =  (jobTemp.get(i).getEstimatedMapExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingMapTasks() / (double)capacity);
			double RedRemainTime =  (jobTemp.get(i).getEstimatedRedExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingRedTasks() / (double)capacity);
			double RemainProcTime = MapRemainTime + RedRemainTime;
			time=time+ RemainProcTime;
			double utilityTemp = jobTemp.get(i).calcUtility(time);
			edf[1] = edf[1]+utilityTemp;
			if (utilityTemp<minimumUtility){
				minimumUtility = utilityTemp;
			}

		}

		edf[0]= minimumUtility;


		DecimalFormat dfd = new DecimalFormat("#.##");      
		edf[0] = Double.valueOf(dfd.format(edf[0]));
		edf[1] = Double.valueOf(dfd.format(edf[1]));
	}


	public void getFIFOUtility( ArrayList<HadoopJobProxy> currentJobs, double[] FIFO){

		ArrayList<HadoopJobProxy> jobTemp = new ArrayList<HadoopJobProxy>();

		for (int i=0; i< currentJobs.size();i++){
			HadoopJobProxy temp = new HadoopJobProxy(currentJobs.get(i));
			temp.setRank(temp.getTimetoJoin());
			jobTemp.add(temp);
		}




		Collections.sort(jobTemp);

		FIFO[1]=0;
		int capacity = ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");
		double time=0;

		double minimumUtility =Double.MAX_VALUE;

		for (int i=0; i< jobTemp.size();i++){
			double MapRemainTime =  (jobTemp.get(i).getEstimatedMapExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingMapTasks() / (double)capacity);
			double RedRemainTime =  (jobTemp.get(i).getEstimatedRedExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingRedTasks() / (double)capacity);
			double RemainProcTime = MapRemainTime + RedRemainTime;
			time=time+ RemainProcTime;
			double utilityTemp = jobTemp.get(i).calcUtility(time);
			FIFO[1] = FIFO[1]+utilityTemp;
			if (utilityTemp<minimumUtility){
				minimumUtility = utilityTemp;
			}

		}

		FIFO[0]= minimumUtility;


		DecimalFormat dfd = new DecimalFormat("#.##");      
		FIFO[0] = Double.valueOf(dfd.format(FIFO[0]));
		FIFO[1] = Double.valueOf(dfd.format(FIFO[1]));
	}


	public void getFairUtility( ArrayList<HadoopJobProxy> currentJobs,  double[] fair){

		@SuppressWarnings("unchecked")
		ArrayList<HadoopJobProxy> jobTemp = new ArrayList<HadoopJobProxy>();
		double totalPriority=0;
		for (int i=0; i< currentJobs.size();i++){
			HadoopJobProxy temp = new HadoopJobProxy(currentJobs.get(i));
			jobTemp.add(temp);
			totalPriority= totalPriority+ currentJobs.get(i).getPriority();
		}


		int capacity = ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");

		double minimumUtility =Double.MAX_VALUE;
		fair[1]=0;
		double currentTime= 0;

		while(jobTemp.size()!=0){


			double minimumTime = Double.MAX_VALUE;
			int minimumIndex = 0;
			for (int i=0; i< jobTemp.size();i++){
				
				//double MapRemainTime =  (jobTemp.get(i).getEstimatedMapExecTime()) *(double)jobTemp.get(i).getnumRemainingMapTasks() /
				//		((double)jobTemp.get(i).getPriority()*(double)capacity);
				//double RedRemainTime =  (jobTemp.get(i).getEstimatedRedExecTime()) *(double)jobTemp.get(i).getnumRemainingRedTasks() /
				//		((double)jobTemp.get(i).getPriority()*(double)capacity);
				 
				double MapRemainTime =  (jobTemp.get(i).getEstimatedMapExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingMapTasks() /
						((double)jobTemp.get(i).getPriority()*(double)capacity/totalPriority));
				double RedRemainTime =  (jobTemp.get(i).getEstimatedRedExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingRedTasks() /
						((double)jobTemp.get(i).getPriority()*(double)capacity/totalPriority));


				if (MapRemainTime+RedRemainTime < minimumTime){
					minimumTime= MapRemainTime+RedRemainTime ;
					minimumIndex= i;
				}
			}

			double utilityTemp = jobTemp.get(minimumIndex).calcUtility(currentTime + minimumTime);
			fair[1] = fair[1]+ utilityTemp;
			if (utilityTemp<minimumUtility){
				minimumUtility= utilityTemp;
			}

			currentTime = currentTime+ minimumTime;

			for (int i=0; i< jobTemp.size();i++){
				double remainMap = jobTemp.get(i).getnumRemainingMapTasks() - ((double)jobTemp.get(i).getPriority()*(double)capacity/totalPriority)*minimumTime/jobTemp.get(i).getEstimatedMapExecTime();
				double remainRed =  jobTemp.get(i).getnumRemainingRedTasks() - ((double)jobTemp.get(i).getPriority()*(double)capacity/totalPriority)*minimumTime/jobTemp.get(i).getEstimatedRedExecTime();

				if (remainMap< 0){
					remainMap=0;
				}
				if (remainRed< 0){
					remainRed=0;
				}
				jobTemp.get(i).setnumRemainingMapTasks((int)remainMap);
				jobTemp.get(i).setnumRemainingRedTasks((int)remainRed);
			}
			jobTemp.remove(minimumIndex);

		}

		fair[0] =  minimumUtility;


		DecimalFormat dfd = new DecimalFormat("#.##");      
		fair[0] = Double.valueOf(dfd.format(fair[0]));
		fair[1] = Double.valueOf(dfd.format(fair[1]));
	}






	public void getRiskRewardUtility( ArrayList<HadoopJobProxy> currentJobs, double[] rru){


		ArrayList<HadoopJobProxy> jobTemp = new ArrayList<HadoopJobProxy>();
		for (int i=0; i< currentJobs.size();i++){
			HadoopJobProxy temp = new HadoopJobProxy(currentJobs.get(i));
			jobTemp.add(temp);

		}



		int capacity = ConstantWarehouse.getIntegerValue("totalMapSlotsInCluster");


		double alpha= 0.3;
		double discountRate = 9;

		for (int i =0; i < jobTemp.size(); i++){

			double yield = jobTemp.get(i).calcUtility(0);

			
			//double MapRemainTime =  (currentJobs.get(i).getEstimatedMapExecTime()) *(double)currentJobs.get(i).getnumRemainingMapTasks() / (double)capacity;
			//double RedRemainTime =  (currentJobs.get(i).getEstimatedRedExecTime()) *(double)currentJobs.get(i).getnumRemainingRedTasks() / (double)capacity;
			 
			double MapRemainTime =  (jobTemp.get(i).getEstimatedMapExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingMapTasks() / (double)capacity);
			double RedRemainTime =  (jobTemp.get(i).getEstimatedRedExecTime()) *Math.ceil((double)jobTemp.get(i).getnumRemainingRedTasks() / (double)capacity);




			double RemainProcTime = MapRemainTime + RedRemainTime;
			double PV = yield /(1+(discountRate*RemainProcTime));

			double expireTime = jobTemp.get(i).invertUtility(0);
			//double decay = currentJobs.get(i).getPriority()*currentJobs.get(i).getGamma()/4;

			double cost=0;

			for (int j =0; j < jobTemp.size(); j++){

				if (i!=j){
					double decayj = jobTemp.get(j).getPriority()*jobTemp.get(j).getGamma()/4;
					cost = cost + decayj*Math.min(RemainProcTime, expireTime);
				}
			}

			double reward = (alpha*PV + (1-alpha)*cost)/RemainProcTime;

			jobTemp.get(i).setRank(reward);


		}



		Collections.sort(jobTemp);

		rru[1]= 0;

		double time=0;

		double minimumUtility =Double.MAX_VALUE;

		for (int i=0; i< jobTemp.size();i++){
			double MapRemainTime =  (jobTemp.get(i).getEstimatedMapExecTime()) *(double)jobTemp.get(i).getnumRemainingMapTasks() / (double)capacity;
			double RedRemainTime =  (jobTemp.get(i).getEstimatedRedExecTime()) *(double)jobTemp.get(i).getnumRemainingRedTasks() / (double)capacity;
			double RemainProcTime = MapRemainTime + RedRemainTime;
			time=time+ RemainProcTime;
			double utilityTemp = jobTemp.get(i).calcUtility(time);
			rru[1] = rru[1] + utilityTemp;
			if (utilityTemp<minimumUtility){
				minimumUtility = utilityTemp;
			}

		}

		rru[0] =  minimumUtility;
		DecimalFormat dfd = new DecimalFormat("#.##");      
		rru[0] = Double.valueOf(dfd.format(rru[0]));
		rru[1] = Double.valueOf(dfd.format(rru[1]));
	}

*/





	public void printFinishingTime( ArrayList<HadoopJobProxy> currentJobs){

		for(int e=0; e<currentJobs.size(); e++){
			System.out.println("Job "+e+" expected finishing time: "+expectedFinishingTimeSlot.get(currentJobs.get(e).getJobName())*TaskTimeGCF
					+" requsted deadline: "+ currentJobs.get(e).getCurrentDeadline());
			//currentJobs.get(e).setExpectedMapFinishingTimeSlot(finishingTime[e]);
		}
	}

	public void printResidualCapacity(mosek.Task task,  ArrayList<HadoopJobProxy> currentJobs){

		//int numTimeSlots=estimateNumOfTimeSlots(currentJobs);
		int numJobs=currentJobs.size();
		int numVar=4*numTimeSlots* numJobs;


		double[] xx=new double[numVar];
		task.getxx(mosek.Env.soltype.bas,xx);

		int[] ResidualCapa = getResidualCapacity(xx, currentJobs);

		for(int e=0; e<numTimeSlots; e++){
			System.out.println("Residual Capacity at time "+e+": "+ResidualCapa[e]);
		}
	}

	public void printProblem(mosek.Task task, ArrayList<HadoopJobProxy> currentJobs){
		//int numTimeSlots=estimateNumOfTimeSlots(currentJobs);
		int numJobs=currentJobs.size();
		int numVar=4*numTimeSlots* numJobs;
		int numCon=numJobs+numTimeSlots + 2*numTimeSlots* numJobs;
		int startPosLambda=numTimeSlots* numJobs;

		try {
			FileWriter fstream = new FileWriter(outputPath+"Problem"+currentIteration+".txt");
			BufferedWriter out = new BufferedWriter(fstream);
			out.write("Obj:");
			double[] ObjCoeff=new double[numVar];
			task.getc(ObjCoeff);




			for(int j=0; j<numVar; j++){

				if(j>=startPosLambda){
					int LambdaIndex=j-startPosLambda;
					int jobIndex=(LambdaIndex/(3*numTimeSlots));
					int timeIndex=(LambdaIndex-3*jobIndex*numTimeSlots)/3;
					int typeIndex=LambdaIndex-3*jobIndex*numTimeSlots-3*timeIndex;


					out.write("Job "+jobIndex+" time "+timeIndex+" type "+typeIndex+": "+Double.toString(ObjCoeff[j])+"	\r\n");

					/*
					if(typeIndex==1){
						out.write("Job "+jobIndex+" time "+timeIndex+": "+Double.toString(ObjCoeff[j])+"	\r\n");
					}
					 */
				}
			}


			for(int i=0; i<numCon; i++){
				for(int j=0; j<numVar; j++){
					double cof=task.getaij(i, j);

					if(cof!=0){
						out.write(Double.toString(cof)+" "+task.getvarname(j)+ " ");
					}

				}
				mosek.Env.boundkey[] target= new mosek.Env.boundkey[1];
				double[] upperb=new double[1];
				double[] lowerb=new double[1];
				task.getbound(mosek.Env.accmode.con,i,target,lowerb,upperb);

				if(target[0]==mosek.Env.boundkey.fx){
					out.write("= "+lowerb[0]+"\r\n");
				}

				if(target[0]==mosek.Env.boundkey.lo){
					out.write(">= "+lowerb[0]+"\r\n");
				}

				if(target[0]==mosek.Env.boundkey.up){
					out.write("<= "+upperb[0]+"\r\n");
				}

			}

			out.close();
			fstream.close();

			if (enableDebugInfo){
				System.out.println("Print problem into file Problem"+currentIteration+".txt\r\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}


	}



	public void printCurrentJobStatus( ArrayList<HadoopJobProxy> currentJobs){


		try {
			FileWriter fstream = new FileWriter(outputPath+"Status"+currentIteration+".txt");
			BufferedWriter out = new BufferedWriter(fstream);

			int time=(int)((JobManager.getTimeOrigin()-System.currentTimeMillis())/1000);
			out.write("Time stamp: "+time+" seconds.\r\n");
			for(int j=0; j<currentJobs.size(); j++){
				String content="Job "+j+" status:\r\n=========================================\r\n";
				HadoopJobProxy tmp=currentJobs.get(j);
				content=content+"Job name: "+ tmp.getJobName()+"\r\n";
				switch (tmp.getjobDeadlineType()){
				case 0:
					content=content+"Job deadline type: hard\r\n";
					break;
				case 1:
					content=content+"Job deadline type: soft\r\n";
					break;
				case 2:
					content=content+"Job deadline type: insensitive\r\n";
					break;
				default:
					break;

				}

				content=content+"Job priority: "+ tmp.getPriority()+"\r\n";
				content=content+"Current allocated capa: Map "+ tmp.getmapSlotsAssigned()+" Reduce " +tmp.getredSlotsAssigned() +"\r\n";


				content=content+"Current resource request: Map "+ tmp.getmapTaskRequested()+ " Reduce "+ tmp.getredTaskRequested()+"\r\n";
				content=content+"Original deadline: "+ tmp.getJobDeadlinewhenSubmit()+"\r\n";
				content=content+"Current deadline: "+ tmp.getCurrentDeadline()+"\r\n";
				content=content+"Time to join: "+ tmp.getTimetoJoin()+"\r\n";
				content=content+"Queue assigned to: "+ tmp.getQueueId()+"\r\n";
				content=content+"\r\n";


				out.write(content);
			}




			out.close();
			fstream.close();

			if(enableDebugInfo){
				System.out.println("Status has been stored into file Status"+currentIteration+".txt\r\n");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}


	}










	public void printSolution(mosek.Task task , ArrayList<HadoopJobProxy> currentJobs){
		//int numTimeSlots=estimateNumOfTimeSlots(currentJobs);
		int numJobs=currentJobs.size();
		int numVar=4*numTimeSlots* numJobs;
		int xrange=numTimeSlots* numJobs;


		double[] xx=new double[numVar];
		task.getxx(mosek.Env.soltype.bas,xx);

		try {
			FileWriter fstream = new FileWriter(outputPath+"Solution"+currentIteration+".txt");
			BufferedWriter out = new BufferedWriter(fstream);





			out.write("Optimal objective value: "+task.getprimalobj(mosek.Env.soltype.bas)+"\r\n");










			//out.write("Objective value MMF: "+ getMaxMinUtility(task, currentJobs)+ "\r\n");


			double[] temp=new double[numVar];
			task.getc(temp); 
			for(int j = 0; j < numVar; ++j){
				if(j<xrange){
					out.write (task.getvarname(j) +" "+ Math.round(xx[j])+ "\r\n");
				}else{
					out.write (task.getvarname(j) +" "+ xx[j]+ "			corresponding objective: "+ temp[j]  +"\r\n");
				}

			}







			out.close();
			fstream.close();

			if(enableDebugInfo){
				System.out.println("Optimal solution has been written to file.");
			}

		} catch (IOException e) {
			e.printStackTrace();
		}
	}









	@Override
	public String getPolicyName() {
		// TODO Auto-generated method stub
		return "MMF";
	}

	/*
	public static void main(String[] args) {
		MaxMinSchedulingPolicy test= new MaxMinSchedulingPolicy();
		test.getResourceAllocationVector();
	}
	 */
}

