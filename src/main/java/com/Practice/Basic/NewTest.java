package com.Practice.Basic;

import com.google.api.services.bigquery.model.TableRow;
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
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.common.collect.Iterators;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NewTest {

	private static final String tableName = "vantage-167009:Learning.JoinTest9Temp2";
	
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
	
	private static class AfterOperation extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key = (String) context.element().get("_id");
			context.output(KV.of(key, context.element()));
		}
	}
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	private static PCollection<TableRow> maxOperation(PCollection<TableRow> rowPCollection){
	
		
		return null;
	
	}
	
	static PCollection<TableRow> operations(PCollection<KV<String, TableRow>> currentRow){
		
//		PCollection<KV<String, TableRow>> currentRow = filteredRow.apply(ParDo.of(new Select()));
		
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
						
//						TableRow maxTableRow = new TableRow();
						
//						maxTableRow.set("CallClosing", row.get("CallClosing"));
//						maxTableRow.set("Salesmanship",row.get("Salesmanship"));
//						maxTableRow.set("ClientOfferAndSend", row.get("ClientOfferAndSend"));
//						maxTableRow.set("Introduction", row.get("Introduction"));
//						maxTableRow.set("PhoneEtiquette", row.get("PhoneEtiquette"));
//						maxTableRow.set("LeadValidation", row.get("LeadValidation"));
						
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
//
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
				.apply(BigQueryIO.Read.named("Reader1").from(tableName))
				.apply(ParDo.named("FormatData1").of(new Select()));
		
		
		PCollection<TableRow> rowPCollection = operations(source1Table);
		
//		PCollection<KV<String, TableRow>> cmpgnPCollection = pipeline.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN))
//				.apply(ParDo.named("FormatData").of(new DoFn<TableRow, KV<String, TableRow>>() {
//					@Override
//					public void processElement(ProcessContext context) throws Exception {
//						TableRow tableRow = context.element();
//					}
//				}));
		
		rowPCollection.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
		
	}
	
	@Test
	public void test1(){
		List<Integer> integers = new ArrayList<>();
		integers.add(2);
		Iterator<Integer> iterators = integers.iterator();
		if (iterators.hasNext()){
			System.out.println(iterators.next());
		}
	}

}
