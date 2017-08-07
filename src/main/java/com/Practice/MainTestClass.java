package com.Practice;

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

import java.util.ArrayList;
import java.util.List;

public class MainTestClass {
	
	private static final String table1Name = "vantage-167009:Learning.Check1";
	private static final String table2Name = "vantage-167009:Learning.Query_Output";
	
	private static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>> {
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("campaignid");
			context.output(KV.of(id, row));
		}
	}
	
	private static class ReadFromTable2 extends DoFn<TableRow, KV<String, TableRow>> {
		
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("campaignid");
			context.output(KV.of(id, row));
		}
	}
	
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
	
	static PCollection<TableRow> joinTables(PCollection<TableRow> pCollection1, PCollection<TableRow> pCollection2){
		PCollection<KV<String, TableRow>> kvpCollection1 = pCollection1.apply(ParDo.of(new ReadFromTable1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = pCollection2.apply(ParDo.of(new ReadFromTable2()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> kvpCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = kvpCollection
				.apply(ParDo.of(new DoFn<KV<String, CoGbkResult>, TableRow>(){
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> coGbkResultKV = context.element();
						TableRow row = new TableRow();
						for(TableRow row1 : coGbkResultKV.getValue().getAll(tupleTag1)){
							setTheTableRow(row, row1, "Learning","Check1");
						}
//						TableRow row1 = coGbkResultKV.getValue().getOnly(tupleTag1);
//						TableRow row2 = coGbkResultKV.getValue().getOnly(tupleTag2);
						for(TableRow row2 : coGbkResultKV.getValue().getAll(tupleTag2)){
							setTheTableRow(row, row2, "Learning", "Query_Output");
						}
						
//						setTheTableRow(row, row1, "Learning","PC_PCI");
//						setTheTableRow(row, row2, "Xtaas", "master_status");
						
						context.output(row);
					}
				}));
		
		return resultPCollection;
	}
	
	interface Options extends PipelineOptions{
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		
		
		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
		setTheSchema(fieldSchemaList, "Learning","Check1");
		setTheSchema(fieldSchemaList, "Learning", "Query_Output");
		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
		PCollection<TableRow> pCollection1 = pipeline.apply(BigQueryIO.Read.from(table1Name));
		PCollection<TableRow> pCollection2 = pipeline.apply(BigQueryIO.Read.from(table2Name));
		
		PCollection<TableRow> rowPCollection = joinTables(pCollection1, pCollection2);
		
		rowPCollection.apply(BigQueryIO.Write.to(options.getOutput())
		.withSchema(tableSchema)
		.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
		.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		pipeline.run();
		
	}
}

