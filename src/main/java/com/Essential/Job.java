package com.Essential;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.values.PCollection;

public class Job {
	
	protected JobContext jobContext;
	
	public DataflowPipelineOptions createOptions(){
	
		JobOptions jobOptions = jobContext.getJobOptions();
		
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
	public JobContext getJobContext()
	{
		return jobContext;
	}
	
	public void setJobContext(JobContext context)
	{
		this.jobContext = context;
	}
	
}
