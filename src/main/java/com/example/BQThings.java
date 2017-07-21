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
	
	private static class ExtractFromThatTable extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
		}
	}
	
//	private static class GetThemFilteredValues extends DoFn<>
	
	private static class GetThemFiltered extends DoFn<TableRow, String>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String field = (String) row.get("input_field_name");
			
		}
	}
	
	
	public static class ExtractFromTableRow extends PTransform<PCollection<TableRow>, PCollection<String>>{
		
		@Override
		public PCollection<String> apply(PCollection<TableRow> tableRow){
			PCollection<TableRow> getThatRow = tableRow.apply(ParDo.of(new ExtractFromThatTable()));
			PCollection<String> result = getThatRow.apply(ParDo.of(new GetThemFiltered()));
			return result;
		}
	}

	public static List<Field> getThemFields(){
		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
		Table table = bigQuerySnippets.getTable("Learning","weather_filter");
		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
		
		return fieldSchemas;
	}
	
	private static interface Options extends PipelineOptions{
		@Description("Input table Path")
		@Default.String("zimetrics:Learning.weather_stations_copy")
		String getInput();
		void setInput(String input);
//
//		@Description("Side input table path")
//		@Default.String("zimetrics:Learning.weather_filter")
//		String getSideInput();
//		void setSideInput(String sideInput);
		
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		pipeline.apply(BigQueryIO.Read.from(options.getInput()))
				.apply(new ExtractFromTableRow())
				.apply(TextIO.Write.to(options.getOutput()));
				
		pipeline.run();
		
	}
	
	@Test
	public void test1(){
		for(Field field : getThemFields()){
			System.out.println(field.getName());
		}
	}
	
}
