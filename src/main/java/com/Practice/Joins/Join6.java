package com.Practice.Joins;

import com.Practice.Basic.Queries;
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
import com.google.cloud.dataflow.sdk.transforms.*;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class Join6 {
	
	private static List<Field> fieldTrackerList = new ArrayList<>();
	static Logger logger = LoggerFactory.getLogger(Join6.class);
	
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
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			context.output(context.element().toPrettyString());
			
		}
	}
	
	private static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id") + tableRow.get("index");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromJoin1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("B__id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromTable3 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromJoin2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("C_campaignId");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromTable4 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	static PCollection<TableRow> combineTableDetails(PCollection<TableRow> stringPCollection1, PCollection<TableRow> stringPCollection2
			, PCollection<TableRow> stringPCollection3, PCollection<TableRow> stringPCollection4,
			                                         List<Field> fieldMetaDataList1, List<Field> fieldMetaDataList2, List<Field> fieldMetaDataList3, List<Field> fieldMetaDataList4,
			                                         String table1Prefix, String table2Prefix, String table3Prefix, String table4Prefix){
		
		PCollection<KV<String, TableRow>> kvpCollection1 = stringPCollection1.apply(ParDo.named("FormatData1").of(new ReadFromTable1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = stringPCollection2.apply(ParDo.named("FormatData2").of(new ReadFromTable1()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						TableRow tableRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								tableRow = new TableRow();
								
								for(Field field: fieldMetaDataList1){
									tableRow.set(table1Prefix + field.getName(), tableRow1.get(field.getName()));
									fieldTrackerList.add(Field.of(table1Prefix + field.getName(), field.getType()));
								}
								
								for(Field field : fieldMetaDataList2){
									tableRow.set(table2Prefix + field.getName(), tableRow2.get(field.getName()));
									fieldTrackerList.add(Field.of(table2Prefix + field.getName(), field.getType()));
								}
								context.output(tableRow);
							}
						}
					}
				}));
		
		PCollection<KV<String, TableRow>> kvpCollection3 = resultPCollection.apply(ParDo.named("FormatData3").of(new ReadFromJoin1()));
		PCollection<KV<String, TableRow>> kvpCollection4 = stringPCollection3.apply(ParDo.named("FormatData4").of(new ReadFromTable3()));
		
		final TupleTag<TableRow> tupleTag3 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag4 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection2 = KeyedPCollectionTuple
				.of(tupleTag3, kvpCollection3)
				.and(tupleTag4, kvpCollection4)
				.apply(CoGroupByKey.create());
		
		
		PCollection<TableRow> resultPCollection2 = pCollection2.apply(ParDo.named("Process2")
				.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag3);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag4);
						
						TableRow tableRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								tableRow = new TableRow();
								
								for(Field field : fieldTrackerList){
									tableRow.set(field.getName(), tableRow1.get(field.getName()));
								}
								
								for(Field field : fieldMetaDataList3){
									tableRow.set(table3Prefix + field.getName(), tableRow2.get(field.getName()));
									fieldTrackerList.add(Field.of(table3Prefix + field.getName(), field.getType()));
								}
								context.output(tableRow);
								
							}
							
						}
					}
				}));
		
		PCollection<KV<String, TableRow>> kvpCollection5 = resultPCollection2.apply(ParDo.named("FormatData5").of(new ReadFromJoin2()));
		PCollection<KV<String, TableRow>> kvpCollection6 = stringPCollection4.apply(ParDo.named("FormatData6").of(new ReadFromTable4()));
		
		final TupleTag<TableRow> tupleTag5 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag6 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection3 = KeyedPCollectionTuple
				.of(tupleTag5, kvpCollection5)
				.and(tupleTag6, kvpCollection6)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection3 = pCollection3.apply(ParDo.named("Process3")
				.of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag5);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag6);
						
						TableRow tableRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								tableRow = new TableRow();
								
								for(Field field : fieldTrackerList){
									tableRow.set(field.getName(), tableRow1.get(field.getName()));
								}
								
								for(Field field : fieldMetaDataList4){
									tableRow.set(table4Prefix + field.getName(), tableRow2.get(field.getName()));
									fieldTrackerList.add(Field.of(table4Prefix + field.getName(), field.getType()));
								}
								context.output(tableRow);
							}
						}
					}
				}));
		
		PCollection<TableRow> fileredPCollection = resultPCollection3.apply(ParDo.named("Filter").of(new DoFn<TableRow, TableRow>() {
			public void processElement(ProcessContext context) throws Exception {
				
				TableRow tableRow = context.element();
				String condition = (String) tableRow.get("A_sectionName");
				logger.info("NOTICE : " + condition);
				if(!(condition.equals("Customer Satisfaction"))){
					context.output(tableRow);
				}
			}
		}));
		
		return fileredPCollection;
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		Queries queries = new Queries();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<TableRow> source1Table = pipeline.apply(BigQueryIO.Read.named("Source1Reader").from(queries.feedbackResponseList));
		PCollection<TableRow> source2Table = pipeline.apply(BigQueryIO.Read.named("Source2Reader").from(queries.responseAttributes));
		PCollection<TableRow> source3Table = pipeline.apply(BigQueryIO.Read.named("Source3Reader").from(queries.pciProspectCall));
		PCollection<TableRow> source4Table = pipeline.apply(BigQueryIO.Read.named("Source4Reader").fromQuery(queries.CMPGN));

		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
		setTheTableSchema(fieldSchemaList, "A_","Xtaas", "pci_feedbackResponseList");
		setTheTableSchema(fieldSchemaList, "B_","Xtaas", "pci_responseAttributes");
		setTheTableSchema(fieldSchemaList, "C_","Xtaas", "pci_prospectcall");
		setTheTableSchema(fieldSchemaList, "D_","Xtaas", "CMPGN");
		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);

		List<Field> fieldMetaDataList1 = getThemFields("Xtaas","pci_feedbackResponseList");
		List<Field> fieldMetaDataList2 = getThemFields("Xtaas","pci_responseAttributes");
		List<Field> fieldMetaDataList3 = getThemFields("Xtaas","pci_prospectcall");
		List<Field> fieldMetaDataList4 = getThemFields("Xtaas", "CMPGN");

		PCollection<TableRow> rowPCollection = combineTableDetails(source1Table, source2Table, source3Table, source4Table,
				fieldMetaDataList1, fieldMetaDataList2, fieldMetaDataList3, fieldMetaDataList4,
				"A_", "B_", "C_", "D_");


		rowPCollection.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
				.withSchema(tableSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));

//		source1Table.apply(ParDo.named("Filter").of(new FilteringData()))
//				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
	
}
