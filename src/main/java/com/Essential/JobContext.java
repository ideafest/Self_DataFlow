package com.Essential;

import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.Serializable;

public class JobContext implements Serializable{
	
	private JobOptions jobOptions;
	private Pipeline pipeline;
	
	
	public JobContext() {
		this.jobOptions = null;
		this.pipeline = null;
	}
	
	public JobOptions getJobOptions() {
		return jobOptions;
	}
	
	
	public Pipeline getPipeline() {
		return pipeline;
	}
	
	public void setPipeline(Pipeline pipeline) {
		this.pipeline = pipeline;
	}
	
	public DataflowPipelineOptions createOptions(){
		
		DataflowPipelineOptions options = PipelineOptionsFactory
				.create().as(DataflowPipelineOptions.class);
		
		options.setRunner(jobOptions.getRunner());
		options.setProject(jobOptions.getProject());
		options.setStagingLocation(jobOptions.getStagingLocation());
		options.setTempLocation(jobOptions.getTempLocation());
		options.setNumWorkers(jobOptions.getNumWorkers());
		
		return options;
	}
	
	public Pipeline createAndInitPipeline(){
		DataflowPipelineOptions options = createOptions();
		Pipeline pipeline = Pipeline.create(options);
		return pipeline;
	}
	
}
