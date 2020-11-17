package org.apache.hadoop.expmanager.schedulingpolicy;

import java.util.ArrayList;

import org.apache.hadoop.expmanager.HadoopJobProxy;

public interface SchedulingPolicy {
	public boolean updateWeights(ArrayList<HadoopJobProxy> currentJobs, double[] weights);
	public String getPolicyName();

	
}
