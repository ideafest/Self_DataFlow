package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

public class ExportFromBQ {
	
	private static final String source = "zimetrics:Learning.shakespeare_copy";
	
	
	static class ExtractData extends DoFn<TableRow, String> {
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			context.output((String)row.get("word"));
		}
	}
	
	static class Writer extends DoFn<String, File>{
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String element = context.element();
			File file = new File("/home/zimetrics/Documents/query.txt");
			try{
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				writer.write(context.element());
				writer.newLine();
			}
			catch (Exception e){}
		}
	}
	
	private interface BQOptions extends PipelineOptions {
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
				.apply(ParDo.of(new Writer()));
		
		pipeline.run();
	}
	
}
