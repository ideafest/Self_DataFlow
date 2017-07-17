package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.*;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.List;


public class SideFilter {
	
	private static final String mainTable = "zimetrics:Learning.weather_stations_copy";
	private static final String sideTable = "zimetrics:Learning.weather_filter";
	
	static class ExtractFromTable extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
		}
	}
	
	static void filterTable(PCollection<TableRow> table1, PCollection<TableRow> table2){
		PCollection<TableRow> extractTable1 = table1.apply(ParDo.of(new ExtractFromTable()));
		
		//TODO
	}
	
	static interface Options extends PipelineOptions{
		@Description("Table to read from")
		@Default.String(mainTable)
		String getInput();
		void setInput(String value);
		
		@Description("BigQuery table to write to")
		@Validation.Required
		String getOutput();
		void setOutput(String value);
	}
	
	public static void main(String[] args) {
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(BigQueryOptions.getDefaultInstance().getService());
		Table table = bigQuerySnippets.getTable("Learning", "weather_stations_copy");
		List<Field> fieldList = table.getDefinition().getSchema().getFields();
		
		Options pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		PCollection<TableRow> table1 = pipeline.apply(BigQueryIO.Read.named("Reader_1").from(pipelineOptions.getInput()));
		PCollection<TableRow> table2 = pipeline.apply(BigQueryIO.Read.named("Side_Reader").from(sideTable));
		filterTable(table1, table2);
		
	}
	
}
