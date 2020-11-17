package org.apache.hadoop.expmanager;

import java.util.LinkedList;


public class QueuePoolManager {
	LinkedList<Integer> queuePool;
	int currentQueueSize;
	
	
	public QueuePoolManager(int NumOFQueues){
		queuePool = new LinkedList<Integer>();
		currentQueueSize = NumOFQueues;
		for(int i =0; i < NumOFQueues; ++i){
			queuePool.add(i);
		}
	}
	
	public synchronized int releaseOneQueueID(){
		//System.out.println(queuePool);
		if(queuePool.size() !=0)
			return queuePool.pop();
		else
			return -1; 
			
	}
	
	public synchronized void addNewQueue(){
		queuePool.add(currentQueueSize);
		currentQueueSize++;
		
	}
	
	
	public synchronized void returnQueueToPool(int QueueID){
		queuePool.add(QueueID);
	}

	public synchronized int getNumQueues(){
		return currentQueueSize;
	}
	
}
