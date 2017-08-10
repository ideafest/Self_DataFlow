package com.Practice.Basic;

import com.example.BigQuerySnippets;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
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
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class BasicTest2 {
	
	private static final String table1Name = "vantage-167009:Learning.Table1";
	private static final String table2Name = "vantage-167009:Learning.Table2";
	
	static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("campaignid");
			context.output(KV.of(id, row));
		}
	}
	
	static PCollection<String> combineTableDetails(PCollection<TableRow> stringPCollection1, PCollection<TableRow> stringPCollection2){
		
		PCollection<KV<String, TableRow>> kvpCollection1 = stringPCollection1.apply(ParDo.named("FormatData1").of(new BasicTest1.ReadFromTable1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = stringPCollection2.apply(ParDo.named("FormatData2").of(new BasicTest1.ReadFromTable2()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		
		PCollection<String> resultPCollection = pCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, CoGbkResult>, String>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						TableRow newRow = new TableRow();
						
						Iterator<TableRow> rowIterator1 = element.getValue().getAll(tupleTag1).iterator();
						Iterator<TableRow> rowIterator2 = element.getValue().getAll(tupleTag2).iterator();
						
						List<FieldMetaData> fieldMetaDataList1 = getTableSchemaDetails("Learning","Table1");
						List<FieldMetaData> fieldMetaDataList2 = getTableSchemaDetails("Learning","Table2");
						
						
						while(rowIterator1.hasNext() && rowIterator2.hasNext()){
							
							TableRow row1Details = rowIterator1.next();
							TableRow row2Details = rowIterator2.next();
							
							for(FieldMetaData metaData : fieldMetaDataList1){
								newRow.set(metaData.getName(), row1Details.get(metaData.getName()));
							}
							
							for(FieldMetaData metaData : fieldMetaDataList2){
								newRow.set(metaData.getName(), row2Details.get(metaData.getName()));
							}
						}
						context.output(newRow.toString());
						
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
		
//		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
//		fieldSchemaList.add(new TableFieldSchema().setName("campaignid").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("createddate").setType("TIMESTAMP"));
//		fieldSchemaList.add(new TableFieldSchema().setName("updateddate").setType("TIMESTAMP"));
//		fieldSchemaList.add(new TableFieldSchema().setName("prospecthandleduration").setType("INTEGER"));
//		fieldSchemaList.add(new TableFieldSchema().setName("DNI").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("campaignid").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("call_date").setType("DATE"));
//		fieldSchemaList.add(new TableFieldSchema().setName("record_datetime").setType("TIMESTAMP"));
//		fieldSchemaList.add(new TableFieldSchema().setName("status").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("dispositionstatus").setType("STRING"));
//		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
		BasicTest1.Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(BasicTest1.Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		
		PCollection<TableRow> rowPCollection1 = pipeline.apply(BigQueryIO.Read.named("Reader1").from(table1Name));
		PCollection<TableRow> rowPCollection2 = pipeline.apply(BigQueryIO.Read.named("Reader2").from(table2Name));
		
		PCollection<String> pCollection = combineTableDetails(rowPCollection1, rowPCollection2);
		
		pCollection.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
		
	}
	
	@Test
	public void test1(){
		List<FieldMetaData> fieldMetaDataList = new ArrayList<>();
		for(Field field: getThemFields("Learning","Table2")){
			fieldMetaDataList.add(new FieldMetaData(field.getName(), field.getType().getValue().toString()));
		}
		
		for(FieldMetaData metaData : fieldMetaDataList){
			System.out.println(metaData.toString());
		}
	}
	
	@Test
	public void test2(){
		String tableName = "vantage-167009:Learning.Table1";
		String[] tokens = tableName.split("[.:]");
		for(String str : tokens){
			System.out.println(str);
		}
	}
	private static List<Field> getThemFields(String datasetName, String tableName){
		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
		Table table = bigQuerySnippets.getTable(datasetName,tableName);
		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
		
		return fieldSchemas;
	}
	
	private static List<FieldMetaData> getTableSchemaDetails(String datasetName, String tableName){
		List<FieldMetaData> fieldMetaDataList = new ArrayList<>();
		for(Field field: getThemFields("Learning","Table1")){
			fieldMetaDataList.add(new FieldMetaData(field.getName(), field.getType().getValue().toString()));
		}
		return fieldMetaDataList;
	}
	
}
