package com.Practice.Basic;

import com.Essential.Queries;
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

public class BasicTest9 {
	
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
	
	private static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
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
			String id = (String) tableRow.get("C__id");
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
	
	private static class ReadFromJoinResult3 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("B_campaignid");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromTable5 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}

	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			context.output(context.element().toPrettyString());
			
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
	
	static PCollection<TableRow> multiCombine(PCollection<KV<String, TableRow>> stringPCollection1,
	                                          PCollection<KV<String, TableRow>> stringPCollection2,
	                                          PCollection<KV<String, TableRow>> stringPCollection3,
	                                          PCollection<KV<String, TableRow>> stringPCollection4,
	                                          List<Field> fieldMetaDataList1, List<Field> fieldMetaDataList2,
	                                          List<Field> fieldMetaDataList3, List<Field> fieldMetaDataList4,
	                                          String table1Prefix, String table2Prefix,
	                                          String table3Prefix, String table4Prefix){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag3 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag4 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag5 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.and(tupleTag3, stringPCollection3)
				.and(tupleTag4, stringPCollection4)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result2").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						Iterable<TableRow> rowIterable3 = element.getValue().getAll(tupleTag3);
						Iterable<TableRow> rowIterable4 = element.getValue().getAll(tupleTag4);
						
						TableRow tableRow;
						
						for(TableRow tableRow1 : rowIterable1){
							for(TableRow tableRow2 : rowIterable2){
								for(TableRow tableRow3 : rowIterable3){
									for (TableRow tableRow4 : rowIterable4){
										tableRow = new TableRow();
										for(Field field : fieldMetaDataList1){
											tableRow.set(table1Prefix + field.getName(), tableRow1.get(field.getName()));
										}
										for(Field field : fieldMetaDataList2){
											tableRow.set(table2Prefix + field.getName(), tableRow2.get(field.getName()));
										}
										for(Field field : fieldMetaDataList3){
											tableRow.set(table3Prefix + field.getName(), tableRow3.get(field.getName()));
										}
										for(Field field : fieldMetaDataList4){
											tableRow.set(table4Prefix + field.getName(), tableRow4.get(field.getName()));
										}
										context.output(tableRow);
									}
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
		
		Queries queries = new Queries();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		List<Field> fieldMetaDataList1 = getThemFields("Learning", "Temp_ProspectCallLog");
		List<Field> fieldMetaDataList2 = getThemFields("Learning", "Temp_ProspectCall");
		List<Field> fieldMetaDataList3 = getThemFields("Learning", "Temp_Prospect");
		List<Field> fieldMetaDataList4 = getThemFields("Learning", "Temp_Answers");
		List<Field> fieldMetaDataList5 = getThemFields("Learning", "Temp_CMPGN");
		

		PCollection<KV<String, TableRow>> source1Table = pipeline
				.apply(BigQueryIO.Read.named("Reader1").from(queries.temp_prospectCallLog))
				.apply(ParDo.named("FormatData1").of(new ReadFromTable1()));
		
		PCollection<KV<String, TableRow>> source2Table = pipeline
				.apply(BigQueryIO.Read.named("Reader2").from(queries.temp_prospectCall))
				.apply(ParDo.named("FormatData2").of(new ReadFromTable1()));
		
		PCollection<KV<String, TableRow>> source3Table = pipeline
				.apply(BigQueryIO.Read.named("Reader3").fromQuery(queries.prospect))
				.apply(ParDo.named("FormatData3").of(new ReadFromTable3()));
		
		PCollection<KV<String, TableRow>> source4Table = pipeline
				.apply(BigQueryIO.Read.named("Reader4").fromQuery(queries.answers))
				.apply(ParDo.named("FormatData4").of(new ReadFromTable4()));
		
		PCollection<KV<String, TableRow>> source5Table = pipeline
				.apply(BigQueryIO.Read.named("Reader5").fromQuery(queries.CMPGN))
				.apply(ParDo.named("FormatData5").of(new ReadFromTable5()));
		
		PCollection<TableRow> finalResult = multiCombine(source1Table, source2Table, source3Table, source4Table,
				fieldMetaDataList1, fieldMetaDataList2, fieldMetaDataList3, fieldMetaDataList4,
				"A_", "B_", "C_", "D_");
				
		PCollection<KV<String, TableRow>> join1Table = finalResult.apply(ParDo.of(new ReadFromJoinResult3()));

		PCollection<TableRow> newFinalResult = combineTableDetails2(join1Table, source5Table, fieldMetaDataList5, "E_");

//		PCollection<TableRow> rowPCollection = combineTableDetails(source1Table, source2Table,
//				fieldMetaDataList1, fieldMetaDataList2, "A_", "B_");
//
//		PCollection<KV<String, TableRow>> joinResult1 = rowPCollection
//				.apply(ParDo.named("FormatData3").of(new ReadFromJoinResult1()));
		

		
//		PCollection<TableRow> rowPCollection2 = combineTableDetails2(joinResult1, source3Table,
//				fieldMetaDataList3, "C_");
//
//
//		PCollection<KV<String, TableRow>> joinResult2 = rowPCollection2
//				.apply(ParDo.named("FormatData5").of(new ReadFromJoinResult2()));
		

//		PCollection<TableRow> rowPCollection3 = combineTableDetails2(joinResult2, source4Table,
//				fieldMetaDataList4, "D_");
//
//		PCollection<KV<String, TableRow>> joinResult3 = rowPCollection3
//				.apply(ParDo.named("FormatData6").of(new ReadFromJoinResult3()));
//

//
//		PCollection<TableRow> rowPCollection4 = combineTableDetails2(joinResult3, source5Table,
//				fieldMetaDataList5, "E_");

//		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
////		setTheTableSchema(fieldSchemaList, "A_","Xtaas", "pci_feedbackResponseList");
////		setTheTableSchema(fieldSchemaList, "B_","Xtaas", "pci_responseAttributes");
////		setTheTableSchema(fieldSchemaList, "C_","Xtaas", "pci_prospectcall");
////		setTheTableSchema(fieldSchemaList, "D_","Xtaas", "CMPGN");
//
//		fieldSchemaList.add(new TableFieldSchema().setName("_id").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("campaignId").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("agentId").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("CallClosing").setType("FLOAT"));
//		fieldSchemaList.add(new TableFieldSchema().setName("Salesmanship").setType("FLOAT"));
//		fieldSchemaList.add(new TableFieldSchema().setName("ClientOfferAndSend").setType("FLOAT"));
//		fieldSchemaList.add(new TableFieldSchema().setName("Introduction").setType("FLOAT"));
//		fieldSchemaList.add(new TableFieldSchema().setName("PhoneEtiquette").setType("FLOAT"));
//		fieldSchemaList.add(new TableFieldSchema().setName("LeadValidation").setType("FLOAT"));
//
//
//		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
//		PCollection<TableRow> itsAPCollection = rowPCollection4.apply(ParDo.of(new Filter()))
//				.apply(ParDo.of(new FinalFieldTableRow()));
//
//		PCollection<TableRow> pCollection = operations(itsAPCollection);
		
//		pCollection.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
//				.withSchema(tableSchema)
//				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		
		
		
		newFinalResult.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
	
}
