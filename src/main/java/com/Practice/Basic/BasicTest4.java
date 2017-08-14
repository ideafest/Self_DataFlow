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
import java.util.List;

public class BasicTest4 {
	
	private static final String table1Name = "vantage-167009:Learning.Test1";
	private static final String table2Name = "vantage-167009:Learning.Test2";
	private static final String table3Name = "vantage-167009:Learning.Test3";
	
	static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("id");
			context.output(KV.of(id, row));
		}
	}
	
	static PCollection<TableRow> combineTableDetails(PCollection<TableRow> stringPCollection1, PCollection<TableRow> stringPCollection2
			, PCollection<TableRow> stringPCollection3, String table1Prefix, String table2Prefix, String table1, String table2){
		
		PCollection<KV<String, TableRow>> kvpCollection1 = stringPCollection1.apply(ParDo.named("FormatData1").of(new ReadFromTable1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = stringPCollection2.apply(ParDo.named("FormatData2").of(new ReadFromTable1()));
		PCollection<KV<String, TableRow>> kvpCollection3 = stringPCollection3.apply(ParDo.named("FormatData3").of(new ReadFromTable1()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag3 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.and(tupleTag3, kvpCollection3)
				.apply(CoGroupByKey.create());
		
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						Iterable<TableRow> rowIterable3 = element.getValue().getAll(tupleTag3);
						
						List<Field> fieldMetaDataList1 = getThemFields("Learning",table1);
						List<Field> fieldMetaDataList2 = getThemFields("Learning",table2);
						List<Field> fieldMetaDataList3 = getThemFields("Learning","Test3");
						
						TableRow tableRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								for(TableRow tableRow3 : rowIterable3){
									tableRow = new TableRow();
									
									for(Field field: fieldMetaDataList1) {
										tableRow.set(table1Prefix + field.getName(), tableRow1.get(field.getName()));
									}
									for(Field field : fieldMetaDataList2){
										tableRow.set(table2Prefix + field.getName(), tableRow2.get(field.getName()));
									}
									for(Field field : fieldMetaDataList3){
										tableRow.set("C_" + field.getName(), tableRow3.get(field.getName()));
									}
									context.output(tableRow);
								}
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
		
		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();

		setTheTableSchema(fieldSchemaList, "A_","Learning", "Test1");
		setTheTableSchema(fieldSchemaList, "B_","Learning", "Test2");
		setTheTableSchema(fieldSchemaList, "C_", "Learning", "Test3");
		
		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		
		PCollection<TableRow> rowPCollection1 = pipeline.apply(BigQueryIO.Read.named("Reader1").from(table1Name));
		PCollection<TableRow> rowPCollection2 = pipeline.apply(BigQueryIO.Read.named("Reader2").from(table2Name));
		PCollection<TableRow> rowPCollection3 = pipeline.apply(BigQueryIO.Read.named("Reader3").from(table3Name));

//		PCollection<TableRow> pCollection = combineTableDetails(rowPCollection1, rowPCollection2);
		
		PCollection<TableRow>  pCollection = combineTableDetails(rowPCollection1, rowPCollection2, rowPCollection3,"A_", "B_"
				, "Test1", "Test2");
//		PCollection<TableRow>  pCollection2 = combineTableDetails(pCollection, rowPCollection3, "B_", "C_"
//					, "Test2", "Test3");
		
		pCollection.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
				.withSchema(tableSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		pipeline.run();
		
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
	
}
