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

import java.util.ArrayList;
import java.util.List;

public class BasicTest7New {
	
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
	
	private static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id") + tableRow.get("index");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromJoinResult1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("B__id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class FinalFieldTableRow extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			TableRow element = context.element();
			TableRow tableRow = new TableRow();
			
			tableRow.set("_id", element.get("A__id"));
			tableRow.set("campaignId", element.get("C_campaignId"));
			tableRow.set("agentId", element.get("C_agentId"));
			tableRow.set("sectionName", element.get("A_sectionName"));
			tableRow.set("feedback", element.get("B_feedback"));
			
			context.output(tableRow);
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
	
	private static class ReadFromJoinResult2 extends DoFn<TableRow, KV<String, TableRow>>{
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
	
	private static class Filter extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			
			String filtered = (String) tableRow.get("A_sectionName");
			boolean aIsDeleted = (boolean) tableRow.get("A_isdeleted");
			boolean aIsDirty = (boolean) tableRow.get("A_isdirty");
			boolean bIsDeleted = (boolean) tableRow.get("B_isdeleted");
			boolean bIsDirty = (boolean) tableRow.get("B_isdirty");
			boolean cIsDeleted = (boolean) tableRow.get("C_isdeleted");
			boolean cIsDirty = (boolean) tableRow.get("C_isdirty");
			boolean dIsDeleted = (boolean) tableRow.get("D_isdeleted");
			boolean dIsDirty = (boolean) tableRow.get("D_isdirty");
			
			if(!(filtered.equals("Customer Satisfaction")) &&
					!(aIsDeleted) && !(aIsDirty) &&
					!(bIsDeleted) && !(bIsDirty) &&
					!(cIsDeleted) && !(cIsDirty) &&
					!(dIsDeleted) && !(dIsDirty)){
				context.output(tableRow);
			}
		}
	}
	
	static PCollection<TableRow> combineTableDetails(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
			, List<Field> fieldMetaDataList1, List<Field> fieldMetaDataList2, String table1Prefix, String table2Prefix){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result1").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
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
								}
								
								for(Field field : fieldMetaDataList2){
									tableRow.set(table2Prefix + field.getName(), tableRow2.get(field.getName()));
								}
								context.output(tableRow);
							}
						}
					}
				}));
		
		
		return resultPCollection;
	}
	
	static PCollection<TableRow> combineTableDetails2(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
			,List<Field> fieldMetaDataList2, String table2Prefix){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result2").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								for(Field field : fieldMetaDataList2){
									tableRow1.set(table2Prefix + field.getName(), tableRow2.get(field.getName()));
								}
								context.output(tableRow1);
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
		
		Queries queries = new Queries();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<KV<String, TableRow>> source1Table = pipeline
				.apply(BigQueryIO.Read.named("Reader1").from(queries.pciFeedbackResponseList))
				.apply(ParDo.named("FormatData1").of(new ReadFromTable1()));
		
		PCollection<KV<String, TableRow>> source2Table = pipeline
				.apply(BigQueryIO.Read.named("Reader2").from(queries.pciResponseAttributes))
				.apply(ParDo.named("FormatData2").of(new ReadFromTable1()));
		
		List<Field> fieldMetaDataList1 = getThemFields("Xtaas","pci_feedbackResponseList");
		List<Field> fieldMetaDataList2 = getThemFields("Xtaas","pci_responseAttributes");
		List<Field> fieldMetaDataList3 = getThemFields("Xtaas","pci_prospectcall");
		List<Field> fieldMetaDataList4 = getThemFields("Xtaas", "CMPGN");
		
		
		
		PCollection<TableRow> rowPCollection = combineTableDetails(source1Table, source2Table,
				fieldMetaDataList1, fieldMetaDataList2, "A_", "B_");
		
		PCollection<KV<String, TableRow>> joinResult1 = rowPCollection
					.apply(ParDo.named("FormatData3").of(new ReadFromJoinResult1()));
		
		PCollection<KV<String, TableRow>> source3Table = pipeline
				.apply(BigQueryIO.Read.named("Reader4").from(queries.pciProspectCall))
				.apply(ParDo.named("FormatData4").of(new ReadFromTable3()));
		
		PCollection<TableRow> rowPCollection2 = combineTableDetails2(joinResult1, source3Table,
				fieldMetaDataList3, "C_");
		
		
		PCollection<KV<String, TableRow>> joinResult2 = rowPCollection2
				.apply(ParDo.named("FormatData5").of(new ReadFromJoinResult2()));

		PCollection<KV<String, TableRow>> source4Table = pipeline
				.apply(BigQueryIO.Read.named("Reader6").fromQuery(queries.CMPGN))
				.apply(ParDo.named("FormatData6").of(new ReadFromTable4()));

		PCollection<TableRow> rowPCollection3 = combineTableDetails2(joinResult2, source4Table,
				fieldMetaDataList4, "D_");
		
		
		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
		setTheTableSchema(fieldSchemaList, "A_","Xtaas", "pci_feedbackResponseList");
		setTheTableSchema(fieldSchemaList, "B_","Xtaas", "pci_responseAttributes");
		setTheTableSchema(fieldSchemaList, "C_","Xtaas", "pci_prospectcall");
		setTheTableSchema(fieldSchemaList, "D_","Xtaas", "CMPGN");
		
//		fieldSchemaList.add(new TableFieldSchema().setName("_id").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("campaignId").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("agentId").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("sectionName").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("feedback").setType("STRING"));
		
		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
		rowPCollection3.apply(ParDo.named("Filter").of(new Filter()))
				//.apply(ParDo.of(new FinalFieldTableRow()))
				.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
						.withSchema(tableSchema)
						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		
//		rowPCollection3//.apply(ParDo.of(new Filter()))
//				.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
	
}
