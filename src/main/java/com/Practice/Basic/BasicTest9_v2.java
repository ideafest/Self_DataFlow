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
import org.junit.Test;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;

public class BasicTest9_v2 {
	
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

	
	private static class ReadFromTable3 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
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
	
	private static class GroupBy1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			
			String campaignId = (String) element.get("campaignId");
			String prospectCallId = (String) element.get("prospectCallId");
			String prospectInteractionSessionId = (String) element.get("prospectInteractionSessionId");
			String dni = (String) element.get("DNI");
			String callStartTime = (String) element.get("callStartTime");
			String callStartDate = (String) element.get("callStartDate");
			String status = (String) element.get("status");
			String industryList = (String) element.get("industryList");
			String dispositionStatus = (String) element.get("dispositionStatus");
			
			String finalKey = campaignId + prospectCallId + prospectInteractionSessionId
					+ dni + callStartTime + callStartDate + status + industryList + dispositionStatus;
			
			context.output(KV.of(finalKey, element));
		}
	}
	
	private static class Filter extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			TableRow element = context.element();
			
			boolean aIsDirty = (boolean) element.get("A_isdirty");
			boolean aIsDeleted = (boolean) element.get("A_isdeleted");
			boolean bIsDirty = (boolean) element.get("B_isdirty");
			boolean bIsDeleted = (boolean) element.get("B_isdeleted");
			boolean cIsDirty = (boolean) element.get("C_isdirty");
			boolean cIsDeleted = (boolean) element.get("C_isdeleted");
			boolean dIsDirty = (boolean) element.get("D_isdirty");
			boolean dIsDeleted = (boolean) element.get("D_isdeleted");
			boolean eIsDirty = (boolean) element.get("E_isdirty");
			boolean eIsDeleted = (boolean) element.get("E_isdeleted");
			
			if(!aIsDirty && !aIsDeleted
					&& !bIsDirty && !bIsDeleted
					&& !cIsDirty && !cIsDeleted
					&& !dIsDirty && !dIsDeleted
					&& !eIsDirty && !eIsDeleted){
				context.output(element);
			}
			
		}
	}
	
	private static boolean existsIn(String str, String[] strArray){
		boolean flag = false;
		for(String s : strArray){
			if(s.equals(str)){
				flag = true;
			}
		}
		return flag;
	}
	
	private static String getString(String str){
		return str.replaceAll("\"", "");
	}
	
	private static String getDate(String str){
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		return stringTokenizer.nextToken();
	}
	
	private static class Select1 extends DoFn<TableRow, TableRow> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			TableRow element = context.element();
			TableRow tableRow = new TableRow();
			
			tableRow.set("campaignId", element.get("B_campaignid"));
			tableRow.set("prospectCallId", element.get("B_prospectcallid"));
			tableRow.set("prospectInteractionSessionId", element.get("B_prospectinteractionsessionid"));
			tableRow.set("DNI", element.get("C_phone"));
			tableRow.set("callStartTime", element.get("B_callstarttime"));
			tableRow.set("callStartDate", getDate((String) element.get("B_callstarttime")));
			tableRow.set("status", element.get("A_status"));
			tableRow.set("dispositionStatus", element.get("B_dispositionstatus"));
			tableRow.set("industryList", element.get("C_industrylist"));
			
			String[] revenueArray = {"\"AnnualRevenue\"","\"What is the Annual Revenue of your Company?\"","\"AnnualRevenue\""};
			String[] eventTimelineArray = {"\"EventTimeline\"","\"What is your timeline?\"","\"When is your event?\"", "\"EventTimeline\""};
			String[] industry = {"\"Industry\"", "\"Which Industry you belong to?\"", "\"Which_Industry_you_belong_to_\"", "\"Industry\""};
			String[] budget = {"\"Budget\"", "\"What is your budget?\"", "\"Budget\""};
			
			String answerKey = (element.get("D_answerskey") == null) ? (String) element.get("D_answerskey") : "null";
			String answerValue = (element.get("D_answersvalue") == null ) ? (String) element.get("D_answersvalue") : "null";
			
			if(existsIn(answerKey, revenueArray)) tableRow.set("Revenue", answerValue); else tableRow.set("Revenue", "null");
			if(existsIn(answerKey, eventTimelineArray)) tableRow.set("EventTimeline", answerValue); else tableRow.set("EventTimeline", "null");
			if(existsIn(answerKey, industry)) tableRow.set("Industry", answerValue); else tableRow.set("Industry", "null");
			if(existsIn(answerKey, budget)) tableRow.set("Budget", answerValue); else tableRow.set("Budget", "null");
			
			
			
			if(!(answerKey == null)){
				if(getString(answerKey).equals("CurrentlyEvaluatingCallAnalytics")) tableRow.set("CurrentlyEvaluatingCallAnalytics", answerValue);
				if(getString(answerKey).equals("Title")) tableRow.set("Title", answerValue);
				if(getString(answerKey).equals("IsCountryUS")) {
					tableRow.set("Country", "USA");
				}
				else{
					tableRow.set("Country", "Other");
				}
				
				
				if(getString(answerKey).equals("Department")) tableRow.set("Department", answerValue);
				if(getString(answerKey).equals("MarketingDeptYesNo")) {
					if (getString(answerValue).toLowerCase().equals("yes")) {
						tableRow.set("Department", "Marketing");
					} else if (getString(answerValue).toLowerCase().equals("no")) {
						tableRow.set("Department", "Other");
					}
				}
				
				if(getString(answerKey).equals("NumberOfEmployeesLessThan50")) {
					if (getString(answerValue).toLowerCase().equals("yes")) {
						tableRow.set("NumberOfEmployees", "Less than 50");
					} else if (getString(answerValue).toLowerCase().equals("no")) {
						tableRow.set("NumberOfEmployees", "More than 50");
					}
				}
				if(getString(answerKey).equals("NumberOfEmployees")) tableRow.set("NumberOfEmployees", answerValue);
			}
			else{
				tableRow.set("CurrentlyEvaluatingCallAnalytics", "null");
				tableRow.set("Title", "null");
				tableRow.set("Country", "Other");
				tableRow.set("Department","null");
				tableRow.set("Department", "null");
				tableRow.set("NumberOfEmployees", "null");
//				tableRow.set("NumberOfEmployees", "null");
			}
			
			context.output(tableRow);
		}
	}
	
	private static PCollection<TableRow> operations(PCollection<TableRow> rowPCollection){
		
		PCollection<KV<String, TableRow>> kvCollection = rowPCollection.apply(ParDo.of(new GroupBy1()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedColl1 = kvCollection.apply(GroupByKey.create());
		
		PCollection<TableRow> pCollection = groupedColl1
				.apply(ParDo.named("Meh1").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						TableRow freshRow = rowIterable.iterator().next();
						
						String maxRevenue = "null", maxEventTimeline = "null", maxIndustry = "null", maxBudget = "null",
								maxCurrentlyEvaluatingCallAnalytics = "null", maxDepartment = "null", maxTitle = "null",
								maxNumberOfEmployees = "null", maxCountry = "null";
						
						for(TableRow tableRow : rowIterable){

							if(((String)tableRow.get("Revenue")).compareTo(maxRevenue) < 0){
								maxRevenue = (String) tableRow.get("Revenue");
							}
							
							if(((String)tableRow.get("EventTimeline")).compareTo(maxEventTimeline) < 0){
								maxEventTimeline = (String) tableRow.get("EventTimeline");
							}
							
							if(((String)tableRow.get("Industry")).compareTo(maxIndustry) < 0){
								maxIndustry = (String) tableRow.get("Industry");
							}
							
							if(((String)tableRow.get("Budget")).compareTo(maxBudget) < 0){
								maxBudget = (String) tableRow.get("Budget");
							}
							
							if(((String)tableRow.get("CurrentlyEvaluatingCallAnalytics")).compareTo(maxCurrentlyEvaluatingCallAnalytics) < 0){
								maxCurrentlyEvaluatingCallAnalytics = (String) tableRow.get("CurrentlyEvaluatingCallAnalytics");
							}
							
							if(((String)tableRow.get("Department")).compareTo(maxDepartment) < 0){
								maxDepartment = (String) tableRow.get("Department");
							}
							
							if(((String)tableRow.get("Title")).compareTo(maxTitle) < 0){
								maxTitle = (String) tableRow.get("Title");
							}
							
							if(((String)tableRow.get("NumberOfEmployees")).compareTo(maxNumberOfEmployees) < 0){
								maxNumberOfEmployees = (String) tableRow.get("NumberOfEmployees");
							}
							
							if(((String)tableRow.get("Country")).compareTo(maxCountry) < 0){
								maxCountry = (String) tableRow.get("Country");
							}
						}
						
						freshRow.set("Revenue", maxRevenue);
						freshRow.set("EventTimeline", maxEventTimeline);
						freshRow.set("Industry", maxIndustry);
						freshRow.set("Budget", maxBudget);
						freshRow.set("CurrentlyEvaluatingCallAnalytics", maxCurrentlyEvaluatingCallAnalytics);
						freshRow.set("Department", maxDepartment);
						freshRow.set("Title", maxTitle);
						freshRow.set("NumberOfEmployees", maxNumberOfEmployees);
						freshRow.set("Country", maxCountry);
						
						context.output(freshRow);
						
					}
				}));
		
		return pCollection;
	}
	
	
	
	static PCollection<TableRow> combineTableDetails2(PCollection<KV<String, TableRow>> stringPCollection1, PCollection<KV<String, TableRow>> stringPCollection2
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
	
	static PCollection<TableRow> multiCombine(PCollection<KV<String, TableRow>> stringPCollection1,
	                                          PCollection<KV<String, TableRow>> stringPCollection2,
	                                          PCollection<KV<String, TableRow>> stringPCollection3,
	                                          PCollection<KV<String, TableRow>> stringPCollection4,
	                                          String table1Prefix, String table2Prefix,
	                                          String table3Prefix, String table4Prefix){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag3 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag4 = new TupleTag<>();
		
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
										for(String field : tableRow1.keySet()){
											tableRow.set(table1Prefix + field, tableRow1.get(field));
										}
										for(String field : tableRow2.keySet()){
											tableRow.set(table2Prefix + field, tableRow2.get(field));
										}
										for(String field : tableRow3.keySet()){
											tableRow.set(table3Prefix + field, tableRow3.get(field));
										}
										for(String field : tableRow4.keySet()){
											tableRow.set(table4Prefix + field,  tableRow4.get(field));
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
		
		@Description("Start time for query")
		@Validation.Required
		String startTime();
		void setStartTime(String startTme);
		
		@Description("End time for query")
		@Validation.Required
		String endTime();
		void setEndTime(String endTime);
	}
	
	public static void main(String[] args) {
		
		Queries queries = new Queries();
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
//		List<Field> fieldMetaDataList1 = getThemFields("Xtaas", "prospectcalllog_new");
//		List<Field> fieldMetaDataList2 = getThemFields("Xtaas", "prospectcall");
//		List<Field> fieldMetaDataList3 = getThemFields("Xtaas", "prospect");
//		List<Field> fieldMetaDataList4 = getThemFields("Xtaas", "answers");
//		List<Field> fieldMetaDataList5 = getThemFields("Xtaas", "CMPGN");
//
//
//		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
//		setTheTableSchema(fieldSchemaList, "A_","Xtaas", "prospectcalllog_new");
//		setTheTableSchema(fieldSchemaList, "B_","Xtaas", "prospectcall");
//		setTheTableSchema(fieldSchemaList, "C_","Xtaas", "prospect");
//		setTheTableSchema(fieldSchemaList, "D_","Xtaas", "answers");
//		setTheTableSchema(fieldSchemaList, "E_","Xtaas", "CMPGN");
//		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);

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
				"A_", "B_", "C_", "D_");

		PCollection<KV<String, TableRow>> join1Table = finalResult.apply(ParDo.of(new ReadFromJoinResult3()));

		PCollection<TableRow> newFinalResult = combineTableDetails2(join1Table, source5Table, "E_");
//
//		PCollection<TableRow> newFinalResult = pipeline.apply(BigQueryIO.Read.from("vantage-167009:Learning.JoinTest9TempResult1"));
		
		PCollection<TableRow> itsAMario = newFinalResult.apply(ParDo.named("Filter").of(new Filter()))
				.apply(ParDo.named("Select1").of(new Select1()));
	
		PCollection<TableRow> iterablePCollection = operations(itsAMario);
//		newFinalResult.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
//						.withSchema(tableSchema)
//						.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
//						.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		
		iterablePCollection.apply(ParDo.of(new ConvertToString()))
				.apply(TextIO.Write.named("Writer").to(options.getOutput()));
		
		pipeline.run();
	}
	
	@Test
	public void test1(){
		String str = "\"ARE_YOU_CURRENTLY_EVALUATING_LOCAL_MARKETING_AUTOMATION_PLATFORMS?\"";
		String string = str.replaceAll("\"", "");
		System.out.println(str);
		System.out.println(string);
	}
}
