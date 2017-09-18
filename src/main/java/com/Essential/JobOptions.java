package com.Essential;

import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.Description;

public interface JobOptions extends DataflowPipelineOptions {
	
	@Description("Get the Input table Name")
	String getInputTable();
	void setInputTable(String inputTable);
	
	@Description("Get the Output table Name")
	String getOutputTable();
	void setOutputTable(String outputTable);
	
	@Description("Get the startTime for the time slice")
	String getStartTime();
	void setStartTime(String startTime);
	
	@Description("Get the endTime for the time slice")
	String getEndTime();
	void setEndTime(String endTime);
	
	@Description("Get the output GS destination")
	String getOutput();
	void setOutput(String output);
	
	@Description("Set the temp Locations")
	String getTempLocation();
	void setTempLocation(String tempLocation);
	
	@Description("Set the Staging Location")
	String getStagingLocation();
	void setStagingLocation(String stagingLocation);
}
