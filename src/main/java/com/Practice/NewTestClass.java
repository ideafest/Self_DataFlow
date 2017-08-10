package com.Practice;

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

import java.util.List;

public class NewTestClass {
	
	private static final String table1Name = "vantage-167009:Learning.Check1";
	private static final String table2Name = "vantage-167009:Learning.Check2";
	
	private static List<Field> getThemFields(String datasetName, String tableName){
		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
		Table table = bigQuerySnippets.getTable(datasetName,tableName);
		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
		
		return fieldSchemas;
	}
	
	public static void setTheSchema(List<TableFieldSchema> fieldSchemaList, String datasetName, String tableName){
		for(Field field : getThemFields(datasetName, tableName)){
			fieldSchemaList.add(new TableFieldSchema().setName(field.getName()).setType(field.getType().getValue().toString()));
		}
	}
	
	public static void setTheTableRow(TableRow finalRow, TableRow sourceRow, String datasetName, String tableName){
		for(Field field : getThemFields(datasetName, tableName)){
			finalRow.set(field.getName(), sourceRow.get(field.getName()));
		}
	}
	
	private static class ReadFromTable extends DoFn<TableRow, KV<String, String>> {
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("campaignid");
			String restOfFields = (String) row.get("agentid");
//			for(Field field : getThemFields("Learning","Check1")){
//				restOfFields += row.get(field.getName()) +", ";
//			}
			context.output(KV.of(id, restOfFields));
		}
	}
	
	private static class ReadFromTable2 extends DoFn<TableRow, KV<String, String>> {
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("campaignid");
			String restOfFields = "";
			for(Field field : getThemFields("Learning","Check2")){
				restOfFields += row.get(field.getName()) +", ";
			}
			context.output(KV.of(id, restOfFields));
		}
	}
	
	static PCollection<String> joinTables(PCollection<TableRow> rowPCollection1, PCollection<TableRow> rowPCollection2) {
		
		PCollection<KV<String, String>> pCollection1 = rowPCollection1.apply(ParDo.of(new ReadFromTable()));
		PCollection<KV<String, String>> pCollection2 = rowPCollection2.apply(ParDo.of(new ReadFromTable2()));
		
		final TupleTag<String> tupleTag1 = new TupleTag<>();
		final TupleTag<String> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> gbkResultPCollection = KeyedPCollectionTuple
				.of(tupleTag1, pCollection1)
				.and(tupleTag2, pCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<KV<String, String>> resultPCollection = gbkResultPCollection
				.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, KV<String, String>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
					
						KV<String, CoGbkResult> coGbkResultKV = context.element();
						String id = coGbkResultKV.getKey();
						String table1Details = coGbkResultKV.getValue().getOnly(tupleTag1);
						for(String table2Details : context.element().getValue().getAll(tupleTag2)){
							context.output(KV.of(id, "Table1Details: "+table1Details +", Table2Details: "+table2Details));
						}
					}
				}));
		
		PCollection<String> formattedPCollection = resultPCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, String>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						context.output("Id: " + context.element().getKey() + ", TableDetails -> "+ context.element().getValue());
					}
				}));
		
		return formattedPCollection;
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
		
		PCollection<TableRow> pCollection1 = pipeline.apply(BigQueryIO.Read.named("Reader1").from(table1Name));
		PCollection<TableRow> pCollection2 = pipeline.apply(BigQueryIO.Read.named("Reader2").from(table2Name));
		
		PCollection<String> resultPCollection = joinTables(pCollection1, pCollection2);
		
		resultPCollection.apply(TextIO.Write.to(options.getOutput()));
		
		pipeline.run();
		
	}
}
