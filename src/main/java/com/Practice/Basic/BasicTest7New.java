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
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import com.google.common.collect.Iterators;

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
	
	private static class Select extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String id = (String) element.get("_id");
			String campaignId = (String) element.get("campaignId");
			String agentId = (String) element.get("agentId");
			String sectionName = (String) element.get("sectionName");
			String feedback = (String) element.get("feedback");
			
			String finalKey = id + campaignId + agentId + sectionName + feedback;
			context.output(KV.of(finalKey, element));
			
		}
	}
	
	private static class Select1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String id = (String) element.get("_id");
			String campaignId = (String) element.get("campaignId");
			String agentId = (String) element.get("agentId");
			String sectionName = (String) element.get("sectionName");
			
			String finalKey = id + campaignId + agentId + sectionName;
			context.output(KV.of(finalKey, element));
			
		}
	}
	
	private static class Select2 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			String id = (String) element.get("_id");
			String campaignId = (String) element.get("campaignId");
			String agentId = (String) element.get("agentId");
			
			String finalKey = id + campaignId + agentId;
			context.output(KV.of(finalKey, element));
			
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
	
	static PCollection<TableRow> operations(PCollection<TableRow> rowPCollection){

		PCollection<KV<String, TableRow>> currentRow = rowPCollection.apply(ParDo.of(new Select()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedBy = currentRow.apply(GroupByKey.create());
		
		PCollection<TableRow> resultPCollection = groupedBy
				.apply(ParDo.named("Meh").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						int size = Iterators.size(rowIterable.iterator());
						TableRow tableRow = rowIterable.iterator().next();
						tableRow.set("COUNT", size);
						context.output(tableRow);
						
					}
				}));
		
		
		PCollection<TableRow> resultPCollection1 = resultPCollection
				.apply(ParDo.named("Meh_V2").of(new DoFn<TableRow, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						TableRow element = context.element();
						int yesCount, noCount, naCount;
						String feedback = (String) element.get("feedback");
						
						if(feedback.equals("YES")){
							yesCount = (Integer) element.get("COUNT");
						}else {
							yesCount = 0;
						}
						if(feedback.equals("NO")){
							noCount = (Integer) element.get("COUNT");
						}else {
							noCount = 0;
						}
						if(feedback.equals("NA")){
							naCount = (Integer) element.get("COUNT");
						}else{
							naCount = 0;
						}
						element.remove("COUNT");
						element.remove("feedback");
						element.set("yes_count", yesCount);
						element.set("no_count", noCount);
						element.set("na_count", naCount);
						context.output(element);
					}
				}));
		
		PCollection<KV<String, TableRow>> pCollection1 = resultPCollection1.apply(ParDo.named("Select1").of(new Select1()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedBy2 = pCollection1.apply(GroupByKey.create());
		
		PCollection<TableRow> resultPCollection2 = groupedBy2
				.apply(ParDo.named("Meh_V3").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						TableRow tableRow = rowIterable.iterator().next();
						
						tableRow.set("yesc", 0);
						tableRow.set("noc", 0);
						tableRow.set("nac", 0);
						
						for(TableRow tableRow1 : rowIterable){
							if((int)tableRow1.get("yes_count") != 0){
								tableRow.replace("yesc", tableRow.get("yesc"), tableRow1.get("yes_count"));
							}
							if((int)tableRow1.get("no_count") != 0){
								tableRow.replace("noc", tableRow.get("noc"), tableRow1.get("no_count"));
							}
							if((int)tableRow1.get("na_count") != 0){
								tableRow.replace("nac", tableRow.get("nac"), tableRow1.get("na_count"));
							}
						}
						
						int yesc = (int) tableRow.get("yesc");
						int noc = (int) tableRow.get("noc");
						int nac = (int) tableRow.get("nac");
						int total = yesc + noc + nac;
						float avg_percent;
						if((total - nac) < 1){
							avg_percent = 1.0f;
						}
						else{
							avg_percent = (float) ((yesc * 1.00) / (total - nac));
						}
						
						String sectionName = (String) tableRow.get("sectionName");
						
						if(sectionName.equals("Call Closing")){
							tableRow.set("CallClosing", avg_percent);
						}
						else{
							tableRow.set("CallClosing", 0.0f);
						}
						if(sectionName.equals("Salesmanship")){
							tableRow.set("Salesmanship", avg_percent);
						}
						else{
							tableRow.set("Salesmanship", 0.0f);
						}
						if(sectionName.equals("Client Offer & Send")){
							tableRow.set("ClientOfferAndSend", avg_percent);
						}
						else{
							tableRow.set("ClientOfferAndSend", 0.0f);
						}
						if(sectionName.equals("Introduction")){
							tableRow.set("Introduction", avg_percent);
						}
						else{
							tableRow.set("Introduction", 0.0f);
						}
						if(sectionName.equals("Phone Etiquette")){
							tableRow.set("PhoneEtiquette", avg_percent);
						}
						else{
							tableRow.set("PhoneEtiquette", 0.0f);
						}
						if(sectionName.equals("Lead Validation")){
							tableRow.set("LeadValidation", avg_percent);
						}
						else{
							tableRow.set("LeadValidation", 0.0f);
						}
						
						
						tableRow.remove("yes_count");
						tableRow.remove("no_count");
						tableRow.remove("na_count");
						tableRow.remove("yesc");
						tableRow.remove("noc");
						tableRow.remove("nac");
						tableRow.remove("sectionName");
						
						context.output(tableRow);
					}
				}));
		
		PCollection<KV<String, TableRow>> pCollection2 = resultPCollection2.apply(ParDo.named("Select2").of(new Select2()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedBy3 = pCollection2.apply(GroupByKey.create());
		
		PCollection<TableRow> resultPCollection3 = groupedBy3
				.apply(ParDo.named("Meh_V3").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						TableRow row = rowIterable.iterator().next();
						
						double maxClientOfferAndSend = 0.0d,
								maxSalesmanship = 0.0d,
								maxCallClosing = 0.0d,
								maxIntroduction = 0.0d,
								maxPhoneEtiquette = 0.0d,
								maxLeadValidation = 0.0d;
						
						for(TableRow tableRow : rowIterable){
							if((double)tableRow.get("CallClosing") > maxCallClosing){
								maxCallClosing = (double)tableRow.get("CallClosing");
							}
							if((double)tableRow.get("Salesmanship") > maxSalesmanship){
								maxSalesmanship = (double)tableRow.get("Salesmanship");
							}
							if((double)tableRow.get("ClientOfferAndSend") > maxClientOfferAndSend){
								maxClientOfferAndSend = (double) tableRow.get("ClientOfferAndSend");
							}
							if((double)tableRow.get("Introduction") > maxIntroduction){
								maxIntroduction = (double)tableRow.get("Introduction");
							}
							if((double)tableRow.get("PhoneEtiquette") > maxPhoneEtiquette){
								maxPhoneEtiquette = (double)tableRow.get("PhoneEtiquette");
							}
							if((double)tableRow.get("LeadValidation") > maxLeadValidation){
								maxLeadValidation = (double)tableRow.get("LeadValidation");
							}
						}
						
						row.set("CallClosing", maxCallClosing);
						row.set("Salesmanship", maxSalesmanship);
						row.set("ClientOfferAndSend", maxClientOfferAndSend);
						row.set("Introduction", maxIntroduction);
						row.set("PhoneEtiquette", maxPhoneEtiquette);
						row.set("LeadValidation", maxLeadValidation);
						
						context.output(row);
					}
				}));
		
		
		return resultPCollection3;
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
//		setTheTableSchema(fieldSchemaList, "A_","Xtaas", "pci_feedbackResponseList");
//		setTheTableSchema(fieldSchemaList, "B_","Xtaas", "pci_responseAttributes");
//		setTheTableSchema(fieldSchemaList, "C_","Xtaas", "pci_prospectcall");
//		setTheTableSchema(fieldSchemaList, "D_","Xtaas", "CMPGN");

		fieldSchemaList.add(new TableFieldSchema().setName("_id").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("campaignId").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("agentId").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CallClosing").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("Salesmanship").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("ClientOfferAndSend").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("Introduction").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("PhoneEtiquette").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("LeadValidation").setType("FLOAT"));


		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
		PCollection<TableRow> itsAPCollection = rowPCollection3.apply(ParDo.of(new Filter()))
				.apply(ParDo.of(new FinalFieldTableRow()));
		
		PCollection<TableRow> pCollection = operations(itsAPCollection);
		
		pCollection.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
				.withSchema(tableSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		

		
//		pCollection.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
	
}
