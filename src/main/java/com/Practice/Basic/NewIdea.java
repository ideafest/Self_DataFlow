//package com.Practice.Basic;
//
//import com.example.BigQuerySnippets;
//import com.google.api.services.bigquery.model.TableFieldSchema;
//import com.google.api.services.bigquery.model.TableRow;
//import com.google.api.services.bigquery.model.TableSchema;
//import com.google.cloud.bigquery.BigQuery;
//import com.google.cloud.bigquery.BigQueryOptions;
//import com.google.cloud.bigquery.Field;
//import com.google.cloud.bigquery.Table;
//import com.google.cloud.dataflow.sdk.Pipeline;
//import com.google.cloud.dataflow.sdk.io.BigQueryIO;
//import com.google.cloud.dataflow.sdk.options.Description;
//import com.google.cloud.dataflow.sdk.options.PipelineOptions;
//import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
//import com.google.cloud.dataflow.sdk.options.Validation;
//import com.google.cloud.dataflow.sdk.transforms.DoFn;
//import com.google.cloud.dataflow.sdk.transforms.ParDo;
//import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
//import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
//import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
//import com.google.cloud.dataflow.sdk.values.KV;
//import com.google.cloud.dataflow.sdk.values.PCollection;
//import com.google.cloud.dataflow.sdk.values.TupleTag;
//
//import java.util.ArrayList;
//import java.util.List;
//
//public class NewIdea {
//
//	private static List<Field> fieldTrackerList = new ArrayList<>();
//
//	private static List<Field> getThemFields(String datasetName, String tableName){
//		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
//		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
//		Table table = bigQuerySnippets.getTable(datasetName,tableName);
//		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
//
//		return fieldSchemas;
//	}
//
//	private static void setTheTableSchema(List<TableFieldSchema> fieldSchemaList, String tablePrefix, String datasetName, String tableName){
//
//		for(Field field : getThemFields(datasetName, tableName)){
//			fieldSchemaList.add(new TableFieldSchema().setName(tablePrefix + field.getName()).setType(field.getType().getValue().toString()));
//		}
//	}
//
//	private static class ConvertToString extends DoFn<TableRow, String> {
//		@Override
//		public void processElement(ProcessContext context) throws Exception {
//
//			context.output(context.element().toPrettyString());
//
//		}
//	}
//
//	private static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>>{
//		@Override
//		public void processElement(ProcessContext context) throws Exception {
//			TableRow tableRow = context.element();
//			TableRow outputRow = new TableRow();
//			outputRow.set("_id", tableRow.get("_id"));
//			outputRow.set("sectionName", tableRow.get("sectionName"));
//			outputRow.set("isdeleted", tableRow.get("isdeleted"));
//			outputRow.set("isdirty", tableRow.get("isdirty"));
//			String id = (String) tableRow.get("_id") + tableRow.get("index");
//			context.output(KV.of(id, outputRow));
//		}
//	}
//
//	private static class ReadFromTable2 extends DoFn<TableRow, KV<String, TableRow>>{
//		@Override
//		public void processElement(ProcessContext context) throws Exception {
//			TableRow tableRow = context.element();
//			TableRow outputRow = new TableRow();
//			outputRow.set("_id", tableRow.get("_id"));
//			outputRow.set("isdeleted", tableRow.get("isdeleted"));
//			outputRow.set("isdirty", tableRow.get("isdirty"));
//			String id = (String) tableRow.get("_id") + tableRow.get("index");
//			context.output(KV.of(id, outputRow));
//		}
//	}
//
//
//	private static class ReadFromTable3 extends DoFn<TableRow, KV<String, TableRow>>{
//		@Override
//		public void processElement(ProcessContext context) throws Exception {
//			TableRow tableRow = context.element();
//			TableRow outputRow = new TableRow();
//			outputRow.set("_id", tableRow.get("_id"));
//			outputRow.set("campaignId", tableRow.get("campaignId"));
//			outputRow.set("isdeleted", tableRow.get("isdeleted"));
//			outputRow.set("isdirty", tableRow.get("isdirty"));
//			String id = (String) tableRow.get("_id") + tableRow.get("index");
//			context.output(KV.of(id, outputRow));
//		}
//	}
//
//
//	private static class ReadFromTable4 extends DoFn<TableRow, KV<String, TableRow>>{
//		@Override
//		public void processElement(ProcessContext context) throws Exception {
//			TableRow tableRow = context.element();
//			TableRow outputRow = new TableRow();
//			outputRow.set("_id", tableRow.get("_id"));
//			outputRow.set("isdeleted", tableRow.get("isdeleted"));
//			outputRow.set("isdirty", tableRow.get("isdirty"));
//			String id = (String) tableRow.get("_id") + tableRow.get("index");
//			context.output(KV.of(id, outputRow));
//		}
//	}
//
//
//	private static List<Field> createFieldList1() {
//		List<Field> fieldList = new ArrayList<>();
//		fieldList.add(Field.of("_id"), "STRING");
//		return fieldList;
//	}
//	private static List<Field> createFieldList2() {
//		List<Field> fieldList = new ArrayList<>();
//
//		return fieldList;
//	}
//	private static List<Field> createFieldList3() {
//		List<Field> fieldList = new ArrayList<>();
//
//		return fieldList;
//	}
//	private static List<Field> createFieldList4() {
//		List<Field> fieldList = new ArrayList<>();
//
//		return fieldList;
//	}
//
//
//	interface Options extends PipelineOptions {
//		@Description("Output path for String")
//		@Validation.Required
//		String getOutput();
//		void setOutput(String output);
//	}
//
//	public static void main(String[] args) {
//		Queries queries = new Queries();
//		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		Pipeline pipeline = Pipeline.create(options);
//
//		PCollection<TableRow> source1Table = pipeline.apply(BigQueryIO.Read.named("Source1Reader").from(queries.pciFeedbackResponseList));
//		PCollection<TableRow> source2Table = pipeline.apply(BigQueryIO.Read.named("Source2Reader").from(queries.pciResponseAttributes));
//		PCollection<TableRow> source3Table = pipeline.apply(BigQueryIO.Read.named("Source3Reader").from(queries.pciProspectCall));
//		PCollection<TableRow> source4Table = pipeline.apply(BigQueryIO.Read.named("Source4Reader").fromQuery(queries.CMPGN));
//
//		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
//		fieldSchemaList.add(new TableFieldSchema().setName("_id").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("campaignid").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("agentid").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("sectionname").setType("STRING"));
//		fieldSchemaList.add(new TableFieldSchema().setName("feedback").setType("STRING"));
//		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
//
//		List<Field> fieldList1 = createFieldList1();
//
//	}
//
//	private static List<Field> createFieldList1() {
//		List<Field> fieldList = new ArrayList<>();
//
//		return fieldList;
//	}
//	private static List<Field> createFieldList2() {
//		List<Field> fieldList = new ArrayList<>();
//
//		return fieldList;
//	}
//	private static List<Field> createFieldList3() {
//		List<Field> fieldList = new ArrayList<>();
//
//		return fieldList;
//	}
//	private static List<Field> createFieldList4() {
//		List<Field> fieldList = new ArrayList<>();
//
//		return fieldList;
//	}
//
//}
