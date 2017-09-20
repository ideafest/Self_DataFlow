package com.FinalJoins.Join1;

import com.Essential.Joins;
import com.Essential.Queries;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
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

public class F1 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			context.output(context.element().toPrettyString());
			
		}
	}
	
	private static class ExtractFromFeedbackResponseList_ResponseAttributes extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id") + tableRow.get("index");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ExtractFromTempJoin1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("B__id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ExtractFromProspectCall extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ExtractFromTempJoin2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("C_campaignId");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ExtractFromCMPGN extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class FilteringPCollection extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			TableRow tableRow = new TableRow();
			tableRow.set("_id", element.get("A__id"));
			tableRow.set("campaignId", element.get("C_campaignId"));
			tableRow.set("agentId", element.get("C_agentId"));
			tableRow.set("sectionName", element.get("A_sectionName"));
			tableRow.set("feedback", element.get("B_feedback"));
			
			String filtered = (String) element.get("A_sectionName");
			boolean aIsDeleted = (boolean) element.get("A_isdeleted");
			boolean aIsDirty = (boolean) element.get("A_isdirty");
			boolean bIsDeleted = (boolean) element.get("B_isdeleted");
			boolean bIsDirty = (boolean) element.get("B_isdirty");
			boolean cIsDeleted = (boolean) element.get("C_isdeleted");
			boolean cIsDirty = (boolean) element.get("C_isdirty");
			boolean dIsDeleted = (boolean) element.get("D_isdeleted");
			boolean dIsDirty = (boolean) element.get("D_isdirty");
			
			if(!(filtered.equals("Customer Satisfaction")) &&
					!(aIsDeleted) && !(aIsDirty) &&
					!(bIsDeleted) && !(bIsDirty) &&
					!(cIsDeleted) && !(cIsDirty) &&
					!(dIsDeleted) && !(dIsDirty)){
				context.output(tableRow);
			}
		}
	}
	
	private static class GetSelectFields1 extends DoFn<TableRow, KV<String, TableRow>>{
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
	
	private static class GetSelectFields2 extends DoFn<TableRow, KV<String, TableRow>>{
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
	
	private static class GetSelectFields3 extends DoFn<TableRow, KV<String, TableRow>>{
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
	
	static PCollection<TableRow> joinOperation1(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
			, String table1Prefix, String table2Prefix){
		
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
								
								for(String field: tableRow1.keySet()){
									tableRow.set(table1Prefix + field, tableRow1.get(field));
								}
								
								for(String field : tableRow2.keySet()){
									tableRow.set(table2Prefix + field, tableRow2.get(field));
								}
								context.output(tableRow);
							}
						}
					}
				}));
		
		
		return resultPCollection;
	}
	
	static PCollection<TableRow> joinOperation2(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
			, String table2Prefix){
		
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
								
								for(String field : tableRow2.keySet()){
									tableRow1.set(table2Prefix + field, tableRow2.get(field));
								}
								context.output(tableRow1);
							}
						}
					}
				}));
		
		
		return resultPCollection;
	}
	
	private static PCollection<TableRow> postOperations(PCollection<TableRow> rowPCollection){
		
		PCollection<TableRow> formattedPCollection = rowPCollection.apply(ParDo.of(new FilteringPCollection()));
		
		PCollection<KV<String, TableRow>> kvpCollection = formattedPCollection.apply(ParDo.of(new GetSelectFields1()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedPCollection = kvpCollection.apply(GroupByKey.create());
		
		PCollection<TableRow> tempPCollection1 = groupedPCollection
				.apply(ParDo.named("PostOperation1").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
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
		
		PCollection<TableRow> tempPCollection2 = tempPCollection1
				.apply(ParDo.named("PostOperation2").of(new DoFn<TableRow, TableRow>() {
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
		
		PCollection<KV<String, TableRow>> kvpCollection2 = tempPCollection2.apply(ParDo.of(new GetSelectFields2()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedPCollection2 = kvpCollection2.apply(GroupByKey.create());
		
		PCollection<TableRow> tempPCollection3 = groupedPCollection2
				.apply(ParDo.named("PostOperation3").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
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
		
		PCollection<KV<String, TableRow>> kvpCollection3 = tempPCollection3.apply(ParDo.of(new GetSelectFields3()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedPCollection3 = kvpCollection3.apply(GroupByKey.create());
		
		PCollection<TableRow> finalResultPCollection = groupedPCollection3
				.apply(ParDo.named("PostOperation4").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
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
		
		return finalResultPCollection;
	}
	

	
	public PCollection<TableRow> runIt(Init init){
		Joins joins = new Joins();

//		PCollection<KV<String, TableRow>> pciFeedbackResponseListPCollection = init.getPci_feedbackResponseList()
//				.apply(ParDo.of(new ExtractFromFeedbackResponseList_ResponseAttributes()));
//
//		PCollection<KV<String, TableRow>> pciResponseAttributesPCollection = init.getPci_responseAttributes()
//				.apply(ParDo.of(new ExtractFromFeedbackResponseList_ResponseAttributes()));
		
		PCollection<TableRow> tempJoin1 = init.getJoinOfPCIFeebackResponseListAndPciResponseAttributesAndCMPGN();
		
		PCollection<KV<String, TableRow>> tempPCollection1 = tempJoin1.apply(ParDo.of(new ExtractFromTempJoin1()));
		
		PCollection<KV<String, TableRow>> pciProspectCallPCollection = init.getPci_prospectCall()
				.apply(ParDo.of(new ExtractFromProspectCall()));
		
		PCollection<TableRow> tempJoin2 = joins.innerJoin2(tempPCollection1, pciProspectCallPCollection, "C_");
		
		
		PCollection<KV<String, TableRow>> tempPCollection2 = tempJoin2.apply(ParDo.of(new ExtractFromTempJoin2()));
		
		PCollection<KV<String, TableRow>> cmpgnPCollection = init.getCMPGN()
				.apply(ParDo.of(new ExtractFromCMPGN()));
		
		PCollection<TableRow> finalJoinResult = joins.innerJoin2(tempPCollection2, cmpgnPCollection, "D_");
		
		PCollection<TableRow> resultPCollection = postOperations(finalJoinResult);
		
		return resultPCollection;
	}
	
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
//		PCollection<KV<String, TableRow>> pciFeedbackResponseListPCollection = pipeline
//				.apply(BigQueryIO.Read.named("pci_feedbackResponseList").from(queries.pciFeedbackResponseList))
//				.apply(ParDo.of(new ExtractFromFeedbackResponseList_ResponseAttributes()));
//
//		PCollection<KV<String, TableRow>> pciResponseAttributesPCollection = pipeline
//				.apply(BigQueryIO.Read.named("pci_responseAttributes").from(queries.pciResponseAttributes))
//				.apply(ParDo.of(new ExtractFromFeedbackResponseList_ResponseAttributes()));
//
//		PCollection<TableRow> tempJoin1 = joinOperation1(pciFeedbackResponseListPCollection, pciResponseAttributesPCollection,
//		"A_", "B_");
//
//		PCollection<KV<String, TableRow>> tempPCollection1 = tempJoin1.apply(ParDo.of(new ExtractFromTempJoin1()));
//
//		PCollection<KV<String, TableRow>> prospectCallPCollection = pipeline
//				.apply(BigQueryIO.Read.named("prospectCall").from(queries.pciProspect))
//				.apply(ParDo.of(new ExtractFromProspectCall()));
//
//		PCollection<TableRow> tempJoin2 = joinOperation2(tempPCollection1, prospectCallPCollection, "C_");
//
//
//		PCollection<KV<String, TableRow>> tempPCollection2 = tempJoin2.apply(ParDo.of(new ExtractFromTempJoin2()));
//
//		PCollection<KV<String, TableRow>> cmpgnPCollection = pipeline
//				.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN))
//				.apply(ParDo.of(new ExtractFromCMPGN()));
//
//		PCollection<TableRow> finalJoinResult = joinOperation2(tempPCollection2, cmpgnPCollection, "D_");
//
//		PCollection<TableRow> resultPCollection = postOperations(finalJoinResult);
//
//		resultPCollection.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
//
//		pipeline.run();
//	}

}
