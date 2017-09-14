package com.Practice.Basic;

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
	
	private static PCollection<String> joinOperations(PCollection<KV<String, TableRow>> kvpCollection1,
	                                                  List<Field> fieldMetaDataList1,
	                                   PCollection<KV<String, TableRow>> kvpCollection2){
		
		TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> gbkResultPCollection = KeyedPCollectionTuple
		.of(tupleTag1, kvpCollection1)
		.and(tupleTag2, kvpCollection2)
		.apply(CoGroupByKey.create());
		
		PCollection<String> resultPCollection = gbkResultPCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, CoGbkResult>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterator1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterator2 = element.getValue().getAll(tupleTag2);
						
						String details1, details2;

//						if (rowIterator1.spliterator().getExactSizeIfKnown() == 0) {
							details1 = "null";
//							details1 = "T1D : " + rowIterator1.iterator().next().keySet().toString();
//						} else {
//							details1 = "T1D : " + rowIterator1.iterator().next().keySet().toString();
//						}
//
//						if (rowIterator2.spliterator().getExactSizeIfKnown() == 0) {
							details2 = "null";
//							details2 = "T2D : " + rowIterator2.iterator().next().keySet().toString();
//
//						} else {
//							details2 = "T2D : " + rowIterator2.iterator().next().keySet().toString();
//						}
//
//						context.output(details1 + ", " + details2);
//
						TableRow freshRow;

						if(rowIterator1.spliterator().getExactSizeIfKnown() != 0) {
							for (TableRow tableRow1 : rowIterator1) {
								if(rowIterator2.spliterator().getExactSizeIfKnown() != 0) {
									for (TableRow tableRow2 : rowIterator2) {
										freshRow = new TableRow();
										for (String field : tableRow1.keySet()) {
											freshRow.set("A_" + field, tableRow1.get(field));
										}
										
										for (String field : tableRow2.keySet()) {
											freshRow.set("B_" + field, tableRow2.get(field));
										}
										context.output(freshRow.toPrettyString());
									}
								}
								else{
									freshRow = new TableRow();
									for (String field : tableRow1.keySet()) {
										freshRow.set("A_" + field, tableRow1.get(field));
									}
									
									for (Field field : fieldMetaDataList1) {
										freshRow.set("B_" + field.getName(), "null");
									}
									context.output(freshRow.toPrettyString());
									
								}
							
//						TableRow freshRow;
//
//						for(TableRow tableRow1 : rowIterator1){
//
//							for(TableRow tableRow2 : rowIterator2){
//
//								freshRow = new TableRow();
//
//								for(String field : tableRow1.keySet()){
//									if(tableRow1.get(field) == null){
//										freshRow.set("A_"+field, "null");
//									}else{
//										freshRow.set("A_"+field, tableRow1.get(field));
//									}
//								}
//
//								for(String field : tableRow2.keySet()){
//									if(tableRow2.get(field) == null){
//										freshRow.set("B_"+field, "null");
//									}else{
//										freshRow.set("B_"+field, tableRow2.get(field));
//									}
//								}
//
//								context.output(freshRow.toPrettyString());
						
						}
						}
					}
				}));
		return resultPCollection;
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
		
		List<Field> fieldMetaDataList1 = getThemFields("Learning","Test2");
		
		PCollection<KV<String, TableRow>> temp1PCollection = pipeline
				.apply(BigQueryIO.Read.from("vantage-167009:Learning.Test1"))
				.apply(ParDo.of(new ExtractFromTemp1()));
		
		PCollection<KV<String, TableRow>> temp2PCollection = pipeline
				.apply(BigQueryIO.Read.from("vantage-167009:Learning.Test2"))
				.apply(ParDo.of(new ExtractFromTemp2()));
		
		
		PCollection<String> stringPCollection = joinOperations(temp1PCollection, fieldMetaDataList1, temp2PCollection);
		
		stringPCollection.apply(TextIO.Write.to(options.getOutput()));
		
		pipeline.run();
	}
	
}
