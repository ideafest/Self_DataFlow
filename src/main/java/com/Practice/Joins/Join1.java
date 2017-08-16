package com.Practice.Joins;

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

public class Join1 {
	
	//PC_PCI(P) with master_status (D) {P.status = D.code}
	
	private static final String table1Name = "vantage-167009:Xtaas.pci_feedbackResponseList";
	private static final String table2Name = "vantage-167009:Xtaas.pci_responseAttributes";
	private static final String table3Name = "vantage-167009:Xtaas.qafeedbackformattributes";
	
	
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
	
	static class ExtractFromPC_FeedbackResponseList extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String id = (String) context.element().get("_id") + (String) context.element().get("index");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class ExtractFromResponse_Attributes extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String id = (String) context.element().get("_id") + (String) context.element().get("index");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class ExtractFromResponse_Attributes_For1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String id = (String) context.element().get("B__id");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static class ExtractFromResponse_Attributes_For2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String id = (String) context.element().get("_id");
			context.output(KV.of(id, context.element()));
		}
	}
	
	static PCollection<TableRow> combineTableDetails(PCollection<TableRow> stringPCollection1, PCollection<TableRow> stringPCollection2
			, List<Field> fieldMetaDataList1, List<Field> fieldMetaDataList2, String table1Prefix, String table2Prefix){
		
		PCollection<KV<String, TableRow>> kvpCollection1 = stringPCollection1.apply(ParDo.named("FormatData1").of(new ExtractFromPC_FeedbackResponseList()));
		PCollection<KV<String, TableRow>> kvpCollection2 = stringPCollection2.apply(ParDo.named("FormatData2").of(new ExtractFromResponse_Attributes()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.<String>create());
		
		
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
	
	static PCollection<TableRow> combineTableDetails2(PCollection<TableRow> stringPCollection1, PCollection<TableRow> stringPCollection2
			, List<Field> fieldMetaDataList1, List<Field> fieldMetaDataList2, String table1Prefix, String table2Prefix){
		
		PCollection<KV<String, TableRow>> kvpCollection1 = stringPCollection1.apply(ParDo.named("FormatData1").of(new ExtractFromResponse_Attributes_For1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = stringPCollection2.apply(ParDo.named("FormatData2").of(new ExtractFromResponse_Attributes_For2()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.<String>create());
		
		
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
	
	interface Options extends PipelineOptions {
		@Description("Output path for Table")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
		
		setTheTableSchema(fieldSchemaList, "A_","Xtaas", "pci_feedbackResponseList");
		setTheTableSchema(fieldSchemaList, "B_","Xtaas","pci_responseAttributes");
		setTheTableSchema(fieldSchemaList, "C_", "Xtaas", "qafeedbackformattributes");
		
		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
		List<Field> fieldMetaDataList1 = getThemFields("Xtaas","pci_feedbackResponseList");
		List<Field> fieldMetaDataList2 = getThemFields("Xtaas","pci_responseAttributes");
		List<Field> fieldMetaDataList3 = getThemFields("Xtaas", "qafeedbackformattributes");
		
		PCollection<TableRow> pciFeedbackResponseListTable = pipeline.apply(BigQueryIO.Read.named("Reader1").from(table1Name));
		PCollection<TableRow> responseAttributesTable = pipeline.apply(BigQueryIO.Read.named("Reader2").from(table2Name));
		PCollection<TableRow> qaFeedbackFormAttributes = pipeline.apply(BigQueryIO.Read.named("Reader3").from(table3Name));
		
		PCollection<TableRow> resultPCollection = combineTableDetails(pciFeedbackResponseListTable, responseAttributesTable,
				fieldMetaDataList1, fieldMetaDataList2, "A_", "B_");
		
//		PCollection<TableRow> resultPCollection2 = combineTableDetails2(resultPCollection, qaFeedbackFormAttributes,
//				)
				
		
		resultPCollection.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
				.withSchema(tableSchema)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
		
		pipeline.run();
	}
}
