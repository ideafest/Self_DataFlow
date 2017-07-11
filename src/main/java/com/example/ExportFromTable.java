package com.example;

import com.google.cloud.WaitForOption;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Table;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class ExportFromTable {
	
	static Table table;
	
	public ExportFromTable(Table table) {
		this.table = table;
	}
	
	private static Job extractTable(String format, String gcURL){
		Job job = table.extract(format, gcURL);
		try{
			Job completedJob = job.waitFor(WaitForOption.checkEvery(1, TimeUnit.SECONDS),
					WaitForOption.timeout(3, TimeUnit.MINUTES));
			if(completedJob != null && completedJob.getStatus().getError() != null){
				System.out.println("Job successful");
			}
			else{
				System.out.println("Error");
			}
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (TimeoutException e) {
			e.printStackTrace();
		}
		return job;
	}
	
	
	public static void main(String[] args) {
		String format = "CSV";
		String gcsURL = "zimetrics:Learning.shakespeare_copy";
		
		Job doneJob = extractTable(format, gcsURL);
		
		System.out.println(doneJob);
	}
}
