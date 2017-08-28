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
			
			String finalKey = id + campaignId + agentId + sectionName;
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
			String feedback = (String) element.get("feedback");
			
			String finalKey = id + campaignId + agentId + sectionName + feedback;
			context.output(KV.of(finalKey, element));
			
		}
	}
	
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	static PCollection<TableRow> groupBy (PCollection<KV<String, TableRow>> currentRow){
		
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
						}else{
							yesCount = 0;
						}
						
						if(feedback.equals("NO")){
							noCount = (Integer) element.get("COUNT");
						}else{
							noCount = 0;
						}
						
						
						if(feedback.equals("NA")){
							naCount = (Integer) element.get("COUNT");
						}else{
							naCount = 0;
						}
						element.remove("COUNT");
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

						
					}
				}));
		
		
		
		return resultPCollection2;
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		PCollection<KV<String, TableRow>> source1Table = pipeline
				.apply(BigQueryIO.Read.named("Reader1").from(tableName))
				.apply(ParDo.named("FormatData1").of(new Select()));
		
		
		PCollection<TableRow> rowPCollection = groupBy(source1Table);
		rowPCollection.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
		
	}

}
