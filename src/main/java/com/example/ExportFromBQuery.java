package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

public class ExportFromBQuery {
	
	private static final String source = "zimetrics:Learning.shakespeare_copy";
	
	static class ExtractData extends DoFn<TableRow, String>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			context.output((String)row.get("word"));
		}
	}
	
	private interface BQOptions extends PipelineOptions{
		@Description("Source")
		@Default.String(source)
		String getInput();
		void setInput(String value);
		
		@Description("Destination")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args) {
		BQOptions options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BQOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply(BigQueryIO.Read.named("Reader").from(options.getInput()))
				.apply(ParDo.of(new ExtractData()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
	
	
}
