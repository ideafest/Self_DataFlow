package com.Practice.Basic;

import com.Essential.Joins;
import com.example.BigQuerySnippets;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.io.TextIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.ArrayList;
import java.util.List;

public class LeftOuterJoinTest {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			context.output(context.element().toPrettyString());
			
		}
	}
	
	
	private static List<Field> getThemFields(String datasetName, String tableName){
		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
		Table table = bigQuerySnippets.getTable(datasetName,tableName);
		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
		
		return fieldSchemas;
	}
	
	private static void setTheTableSchema(List<TableFieldSchema> fieldSchemaList, String tablePrefix, String datasetName, String tableName){
		
		for(Field field : getThemFields(datasetName, tableName)){
			fieldSchemaList.add(new TableFieldSchema().setName(tablePrefix + field.getName()).setType(field.getType().getValue().toString()));
		}
	}
	
	private static class ExtractFromTemp1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("id");
			context.output(KV.of(key, element));
		}
	}
	
	private static class ExtractFromTemp2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String key = (String) element.get("id");
			context.output(KV.of(key, element));
		}
	}
	
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		Joins joins = new Joins();
		
		PCollection<KV<String, TableRow>> temp1PCollection = pipeline
				.apply(BigQueryIO.Read.from("vantage-167009:Learning.Test1"))
				.apply(ParDo.of(new ExtractFromTemp1()));
		
		PCollection<KV<String, TableRow>> temp2PCollection = pipeline
				.apply(BigQueryIO.Read.from("vantage-167009:Learning.Test2"))
				.apply(ParDo.of(new ExtractFromTemp2()));
		
		
		PCollection<TableRow> stringPCollection = joins.leftOuterJoin1(temp1PCollection,temp2PCollection,
				"A_","B_");
		
		stringPCollection.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.to(options.getOutput()));
		
		pipeline.run();
	}
	
}
