package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class BQThings {
	
	
	private interface Options extends PipelineOptions{
		@Description("Input table Path")
		@Default.String("vantage-167009:Xtaas.dnclist")
		String getInput();
		void setInput(String input);
	
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply(BigQueryIO.Read.from(options.getInput()))
				.apply(ParDo.of(new DoFn<TableRow, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						context.output(context.element().toPrettyString());
					}
				}))
				.apply(TextIO.Write.to(options.getOutput()));
				
		pipeline.run();
		
	}
	
	
}
