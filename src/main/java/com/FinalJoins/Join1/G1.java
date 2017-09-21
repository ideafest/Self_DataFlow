package com.FinalJoins.Join1;

import com.Essential.JobOptions;
import com.Essential.Joins;
import com.Essential.Queries;
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

import java.util.StringTokenizer;

public class G1 {
	
	private static class ExtractFromProspectCallLog_ProspectCall extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	
	private static class ExtractFromProspect extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	
	private static class ExtractFromAnswers extends DoFn<TableRow, KV<String, TableRow>>{
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
	
	private static class ExtractFromCMPGN extends DoFn<TableRow, KV<String, TableRow>>{
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
			
			String callStartTime = (String) element.get("B_callstarttime");
			if(callStartTime != null){
				tableRow.set("callStartDate", getDate(callStartTime));
			}
			tableRow.set("status", element.get("A_status"));
			tableRow.set("dispositionStatus", element.get("B_dispositionstatus"));
			tableRow.set("industryList", element.get("C_industrylist"));
			
			String[] revenueArray = {"\"AnnualRevenue\"","\"What is the Annual Revenue of your Company?\"","\"AnnualRevenue\""};
			String[] eventTimelineArray = {"\"EventTimeline\"","\"What is your timeline?\"","\"When is your event?\"", "\"EventTimeline\""};
			String[] industry = {"\"Industry\"", "\"Which Industry you belong to?\"", "\"Which_Industry_you_belong_to_\"", "\"Industry\""};
			String[] budget = {"\"Budget\"", "\"What is your budget?\"", "\"Budget\""};
			
			String answerKey, answerValue;
			
			if(element.get("D_answerskey") == null){
				answerKey = "null";
			}else{
				answerKey = (String) element.get("D_answerskey");
			}
			
			if(element.get("D_answersvalue") == null){
				answerValue = "null";
			}else{
				answerValue = (String) element.get("D_answersvalue");
			}
			
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
							
							if(tableRow.get("Revenue") != null){
								if(((String)tableRow.get("Revenue")).compareTo(maxRevenue) < 0){
									maxRevenue = (String) tableRow.get("Revenue");
								}
							}
							
							if(tableRow.get("EventTimeline") != null){
								if(((String)tableRow.get("EventTimeline")).compareTo(maxEventTimeline) < 0){
									maxEventTimeline = (String) tableRow.get("EventTimeline");
								}
							}
							
							if(tableRow.get("Industry") != null){
								if(((String)tableRow.get("Industry")).compareTo(maxIndustry) < 0){
									maxIndustry = (String) tableRow.get("Industry");
								}
							}
							
							if(tableRow.get("Budget") != null){
								if(((String)tableRow.get("Budget")).compareTo(maxBudget) < 0){
									maxBudget = (String) tableRow.get("Budget");
								}
							}
							
							if(tableRow.get("CurrentlyEvaluatingCallAnalytics") != null){
								if(((String)tableRow.get("CurrentlyEvaluatingCallAnalytics")).compareTo(maxCurrentlyEvaluatingCallAnalytics) < 0){
									maxCurrentlyEvaluatingCallAnalytics = (String) tableRow.get("CurrentlyEvaluatingCallAnalytics");
								}
							}
							
							
							if(tableRow.get("Department") != null){
								if(((String)tableRow.get("Department")).compareTo(maxDepartment) < 0){
									maxDepartment = (String) tableRow.get("Department");
								}
							}
							
							if(tableRow.get("Title") != null){
								if(((String)tableRow.get("Title")).compareTo(maxTitle) < 0){
									maxTitle = (String) tableRow.get("Title");
								}
							}
							
							if(tableRow.get("NumberOfEmployees") != null){
								if(((String)tableRow.get("NumberOfEmployees")).compareTo(maxNumberOfEmployees) < 0){
									maxNumberOfEmployees = (String) tableRow.get("NumberOfEmployees");
								}
							}
							
							
							if(tableRow.get("Country") != null){
								if(((String)tableRow.get("Country")).compareTo(maxCountry) < 0){
									maxCountry = (String) tableRow.get("Country");
								}
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
	
	
	public PCollection<TableRow> runIt(Init init){
		Joins joins = new Joins();
		
		PCollection<KV<String, TableRow>> prospectCallLogPCollection = init.getProspectCallLog()
				.apply(ParDo.named("FormatData1").of(new ExtractFromProspectCallLog_ProspectCall()));
		
		PCollection<KV<String, TableRow>> prospectCallPCollection = init.getProspectCall()
				.apply(ParDo.named("FormatData2").of(new ExtractFromProspectCallLog_ProspectCall()));
		
		PCollection<KV<String, TableRow>> prospectPCollection =init.getProspect()
				.apply(ParDo.named("FormatData3").of(new ExtractFromProspect()));
		
		PCollection<KV<String, TableRow>> answersPCollection = init.getAnswers()
				.apply(ParDo.named("FormatData4").of(new ExtractFromAnswers()));
		
		PCollection<KV<String, TableRow>> cmpgnPCollection = init.getCMPGN()
				.apply(ParDo.named("FormatData5").of(new ExtractFromCMPGN()));

		PCollection<TableRow> tempPCollection1 = joins.multiCombine(prospectCallLogPCollection, prospectCallPCollection, prospectPCollection, answersPCollection,
				"A_", "B_", "C_", "D_",
				"MultiJoin");

		PCollection<KV<String, TableRow>> tempPCollection2 = tempPCollection1.apply(ParDo.of(new ReadFromJoinResult3()));

		PCollection<TableRow> result = joins.innerJoin2(tempPCollection2, cmpgnPCollection, "E_",
				"JoiningCMPGN");


		PCollection<TableRow> tempPCollection3 = result.apply(ParDo.named("Filter").of(new Filter()))
				.apply(ParDo.named("Select1").of(new Select1()));

		PCollection<TableRow> resultPCollection = operations(tempPCollection3);
		return resultPCollection;
	}
	
}
