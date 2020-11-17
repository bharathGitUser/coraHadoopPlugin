package org.apache.hadoop.jobs;


import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.mapred.RunningJob;
import org.apache.hadoop.util.Tool;

public abstract class HadoopJobs extends Configured implements Tool{
	

	abstract public int getMapTaskTime();
	abstract public int getRedTaskTime();
	
	abstract public RunningJob submitAndExec(String JobName, String QueueID);

}
