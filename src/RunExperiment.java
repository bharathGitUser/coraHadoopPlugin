

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.expmanager.JobManager;
import org.apache.hadoop.expmanager.schedulingpolicy.MaxMinSchedulingPolicy;
import org.apache.hadoop.expmanager.schedulingpolicy.RiskRewardHeuristic;
import org.apache.hadoop.expmanager.schedulingpolicy.SchedulingPolicy;
import org.apache.hadoop.expmanager.schedulingpolicy.SimpleSchedPolicies;
import org.apache.hadoop.expmanager.schedulingpolicy.MaxMinBisectionSchedulingPolicy;
import org.apache.hadoop.expmanager.utility.Utility;
import org.apache.log4j.FileAppender;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;


public class RunExperiment {





	public static void main(String[] args) {

		int CurrentIteraction=20;
		int numInterToRun=100;

		//MaxMinSchedulingPolicy policy =new MaxMinSchedulingPolicy();
		//SimpleSchedPolicies policy = new SimpleSchedPolicies(SimpleSchedPolicies.EDF);
		//SimpleSchedPolicies policy = new SimpleSchedPolicies(SimpleSchedPolicies.EQU);
		//SimpleSchedPolicies policy = new SimpleSchedPolicies(SimpleSchedPolicies.FAIR);
		//SimpleSchedPolicies policy = new SimpleSchedPolicies(SimpleSchedPolicies.RANDOM);

		while(CurrentIteraction<=numInterToRun){


			Utility.replaceJobSetFile(CurrentIteraction);

			
			
			
			ArrayList<SchedulingPolicy> schedPoliciesToTest = new ArrayList<SchedulingPolicy>();


			
			

			//MaxMinSchedulingPolicy MainPolicy =new MaxMinSchedulingPolicy();
			RiskRewardHeuristic policy1 = new RiskRewardHeuristic();

			//SimpleSchedPolicies policy2 = new SimpleSchedPolicies(SimpleSchedPolicies.EDF);
			//SimpleSchedPolicies policy3 = new SimpleSchedPolicies(SimpleSchedPolicies.FAIR);
			//SimpleSchedPolicies policy4 = new SimpleSchedPolicies(SimpleSchedPolicies.FIFO);
			
			//schedPoliciesToTest.add(MainPolicy);
			schedPoliciesToTest.add(policy1);
			//schedPoliciesToTest.add(policy2);


			//schedPoliciesToTest.add(policy3);
			//schedPoliciesToTest.add(policy4);


			for (int i=0;i < schedPoliciesToTest.size(); i++){
				
				
				

				Utility.switchLogFile("TimelineLogger", "/home/hduser/log/Timeline-"+  schedPoliciesToTest.get(i).getPolicyName() +"-" +CurrentIteraction  +".log");
				Utility.switchLogFile("Joblogger", "/home/hduser/log/Result-"+  schedPoliciesToTest.get(i).getPolicyName() +"-"   +CurrentIteraction  +".log");
				Utility.switchLogFile("JobManagerlogger", "/home/hduser/log/General-"+  schedPoliciesToTest.get(i).getPolicyName()  +"-"  +CurrentIteraction  +".log");
				Utility.switchLogFile("AlgorithmRuntimeLogger", "/home/hduser/log/Runtime-"+  schedPoliciesToTest.get(i).getPolicyName()  +"-"  +CurrentIteraction  +".log");
				Utility.switchLogFile("UtilityLogger","/home/hduser/log/Utility-"+  schedPoliciesToTest.get(i).getPolicyName()  +"-"  +CurrentIteraction  +".log");
				
				
				JobManager JM = new JobManager(schedPoliciesToTest.get(i));
				JM.execExp();

				while(!JM.isAllJobsDone()){

					try {
						Thread.sleep(10000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}

				//String folder= "";

				//folder=folder+ CurrentIteraction+ "/"+schedPoliciesToTest.get(i).getPolicyName();
				//copyFiles(folder);

				Utility.restartCluster();

			}


			CurrentIteraction = CurrentIteraction+20;
			//CurrentIteraction++;


		}



	}


}
