package com.example;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Default;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

public class BQThings_v2 {
	
	private static final String tableName = "zimetrics:Learning.shakespeare_copy";
	private static List<String> fieldNames;
	
	private static void getTableFields() {
		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
		Table table = bigQuerySnippets.getTable("Learning", "weather_stations_copy");
		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
		fieldNames = new ArrayList<>();
		for(int i = 0;i<4;i++){
			fieldNames.add(fieldSchemas.get(i).getName());
		}
	}
	
	static class ExtractTableData extends DoFn<TableRow, String>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			getTableFields();
			TableRow row = context.element();
			String str = "";
			for (String field : fieldNames) {
				str += row.get(field) + ", ";
			}
			context.output(str);
		}
	}
	
	
	private interface BQOptions extends PipelineOptions {
		@Description("Source")
		@Default.String(tableName)
		String getInput();
		void setInput(String value);
	}
	
	public static void main(String[] args) {
		
		BQOptions bqOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(BQOptions.class);
		Pipeline pipeline = Pipeline.create(bqOptions);
		
		pipeline.apply(BigQueryIO.Read.named("Reader").from(bqOptions.getInput()))
				.apply(ParDo.of(new ExtractTableData()))
				.apply(TextIO.Write.named("Writing").to("gs://learning001/output2/table"));
		
		pipeline.run();
	
	}

}
