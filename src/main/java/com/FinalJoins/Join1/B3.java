package com.FinalJoins.Join1;

import com.Practice.Basic.Joins;
import com.Practice.Basic.Queries;
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
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

public class B3 {
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element().toPrettyString());
		}
	}
	
	
	private static class ExtractFromB2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key = (String) context.element().get("A_campaignId");
			context.output(KV.of(key, context.element()));
		}
	}
	
	private static class ExtractTempJoin1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key = (String) context.element().get("A_agentId");
			context.output(KV.of(key, context.element()));
		}
	}
	
	private static class ExtractTempJoin2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key = (String) context.element().get("A_qaId");
			context.output(KV.of(key, context.element()));
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
	
	private static class ExtractFromAgent extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ExtractFromQa extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			if(id != null){
				context.output(KV.of(id, tableRow));
			}
		}
	}
	
	private static class GenerateKV extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			String key1 = (String) context.element().get("campaignId");
			String key2 = (String) context.element().get("prospectCallId");
			String key3 = (String) context.element().get("prospectInteractionSessionId");
			context.output(KV.of(key1 + key2 + key3, context.element()));
		}
	}
	
	private static class SelectFromJoinResult extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			TableRow freshRow = new TableRow();
			
			freshRow.set("CampaignName", element.get("C_name"));
			
			String agentName = element.get("D_firstname") + " " + element.get("D_lastname");
			freshRow.set("AgentName", agentName);
			
			String qaName = element.get("E_firstname") + " " + element.get("E_lastname");
			freshRow.set("QAName", qaName);
			freshRow.set("_id", element.get("A__id"));
			freshRow.set("campaignId", element.get("A_campaignid"));
			freshRow.set("agentId", element.get("A_agentid"));
			freshRow.set("qaId", element.get("A_qaid"));
			freshRow.set("prospectCallId", element.get("A_prospectcallid"));
			freshRow.set("prospectInteractionSessionId", element.get("A_prospectinteractionsessionid"));
			freshRow.set("callDate", element.get("A_calldate"));
			freshRow.set("callStartTime", element.get("A_callStartTime"));
			freshRow.set("status", element.get("A_status"));
			freshRow.set("dispositionStatus", element.get("A_dispositionstatus"));
			freshRow.set("subStatus", element.get("A_substatus"));
			freshRow.set("overallScore", element.get("A_overallscore"));
			freshRow.set("feedbackDate", element.get("A_feedbackdate"));
			freshRow.set("feedbackTime", element.get("A_feedbacktime"));
			freshRow.set("lead_validation_notes", element.get("A_lead_validation_notes"));
			freshRow.set("PHONEETIQUETTE_CUSTOMER_ENGAGEMENT", element.get("A_PHONEETIQUETTE_CUSTOMER_ENGAGEMENT"));
			freshRow.set("PHONEETIQUETTE_PROFESSIONALISM", element.get("A_PHONEETIQUETTE_PROFESSIONALISM"));
			freshRow.set("SALESMANSHIP_REBUTTAL_USE", element.get("A_SALESMANSHIP_REBUTTAL_USE"));
			freshRow.set("SALESMANSHIP_PROVIDE_INFORMATION", element.get("A_SALESMANSHIP_PROVIDE_INFORMATION"));
			freshRow.set("INTRODUCTION_BRANDING_PERSONAL_CORPORATE", element.get("A_INTRODUCTION_BRANDING_PERSONAL_CORPORATE"));
			freshRow.set("INTRODUCTION_MARKETING_EFFORTS", element.get("A_INTRODUCTION_MARKETING_EFFORTS"));
			freshRow.set("CUSTOMERSATISFACTION_OVERALL_SERVICE", element.get("A_CUSTOMERSATISFACTION_OVERALL_SERVICE"));
			freshRow.set("CLIENT_FULL_DETAILS", element.get("A_CLIENT_FULL_DETAILS"));
			freshRow.set("CLIENT_PII", element.get("A_CLIENT_PII"));
			freshRow.set("CALLCLOSING_BRANDING_PERSONAL_CORPORATE", element.get("A_CALLCLOSING_BRANDING_PERSONAL_CORPORATE"));
			freshRow.set("PHONEETIQUETTE_CALL_PACING", element.get("A_PHONEETIQUETTE_CALL_PACING"));
			freshRow.set("PHONEETIQUETTE_CALL_HOLD_PURPOSE", element.get("A_PHONEETIQUETTE_CALL_HOLD_PURPOSE"));
			freshRow.set("SALESMANSHIP_PREQUALIFICATION_QUESTIONS", element.get("A_SALESMANSHIP_PREQUALIFICATION_QUESTIONS"));
			freshRow.set("INTRODUCTION_PREPARE_READY", element.get("A_INTRODUCTION_PREPARE_READY"));
			freshRow.set("INTRODUCTION_CALL_RECORD", element.get("A_INTRODUCTION_CALL_RECORD"));
			freshRow.set("CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL", element.get("A_CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL"));
			freshRow.set("CLIENT_POSTQUALIFICATION_QUESTIONS", element.get("A_CLIENT_POSTQUALIFICATION_QUESTIONS"));
			freshRow.set("CLIENT_OBTAIN_CUSTOMER_CONSENT", element.get("A_CLIENT_OBTAIN_CUSTOMER_CONSENT"));
			freshRow.set("CALLCLOSING_EXPECTATION_SETTING", element.get("A_CALLCLOSING_EXPECTATION_SETTING"));
			freshRow.set("CALLCLOSING_REDIAL_NUMBER", element.get("A_CALLCLOSING_REDIAL_NUMBER"));
			freshRow.set("LEAD_VALIDATION_VALID", element.get("A_LEAD_VALIDATION_VALID"));
			freshRow.set("ClientOfferAndSend", element.get("B_ClientOfferAndSend"));
			freshRow.set("Salesmanship", element.get("B_Salesmanship"));
			freshRow.set("CallClosing", element.get("B_CallClosing"));
			freshRow.set("Introduction", element.get("B_Introduction"));
			freshRow.set("PhoneEtiquette", element.get("B_PhoneEtiquette"));
			freshRow.set("LeadValidation", element.get("B_LeadValidation"));
			
			boolean cIsDirty = (boolean) element.get("C_isdirty");
			boolean cIsDeleted = (boolean) element.get("C_isdeleted");
			boolean dIsDirty = (boolean) element.get("D_isdirty");
			boolean dIsDeleted = (boolean) element.get("D_isdeleted");
			boolean eIsDirty = (boolean) element.get("E_isdirty");
			boolean eIsDeleted = (boolean) element.get("E_isdeleted");
			
			if(!cIsDirty && !cIsDeleted
					&& !dIsDirty && !dIsDeleted
					&& !eIsDirty && !eIsDeleted) {
				context.output(freshRow);
			}
			
		}
	}
	
	private static PCollection<TableRow> postOperations(PCollection<TableRow> rowPCollection){
		
		PCollection<TableRow> filteredRow = rowPCollection.apply(ParDo.of(new SelectFromJoinResult()));
		
		PCollection<KV<String, TableRow>> generatedPCollection = filteredRow.apply(ParDo.of(new GenerateKV()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupedPCollection = generatedPCollection.apply(GroupByKey.create());
		
		PCollection<TableRow> filteredPCollection = groupedPCollection
				.apply(ParDo.of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
						
						String maxFeedbackTime = "";
						
						for(TableRow tableRow : rowIterable){
							String updatedTime = (String)tableRow.get("feedbackTime");
							if(updatedTime.compareTo(maxFeedbackTime) > 0){
								maxFeedbackTime = updatedTime;
							}
						}
						for(TableRow tableRow : rowIterable){
							String updatedTime = (String)tableRow.get("feedbackTime");
							if(maxFeedbackTime.equals(updatedTime)){
								context.output(tableRow);
							}
						}
					}
				}));
		
		return filteredPCollection;
		
	}
	
	public PCollection<TableRow> runIt(Pipeline pipeline){
		Queries queries = new Queries();
		Joins joins = new Joins();
		B2 b2 = new B2();
		
		PCollection<KV<String, TableRow>> b2PCollection = b2.runIt(pipeline).apply(ParDo.of(new ExtractFromB2()));
		PCollection<KV<String, TableRow>> cmpgnPCollection = pipeline
				.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN))
				.apply(ParDo.of(new ExtractFromCMPGN()));
		
		PCollection<TableRow> tempJoin1 = joins.innerJoin2(b2PCollection, cmpgnPCollection, "C_");
		
		PCollection<KV<String, TableRow>> tempJoin1PCollection = tempJoin1.apply(ParDo.of(new ExtractTempJoin1()));
		PCollection<KV<String, TableRow>> agentPCollection = pipeline
				.apply(BigQueryIO.Read.named("agent").from(queries.agent))
				.apply(ParDo.of(new ExtractFromAgent()));
		
		PCollection<TableRow> tempJoin2 = joins.innerJoin2(tempJoin1PCollection, agentPCollection, "D_");
		
		PCollection<KV<String, TableRow>> tempJoin2PCollection = tempJoin2.apply(ParDo.of(new ExtractTempJoin2()));
		PCollection<KV<String, TableRow>> qaPCollection = pipeline
				.apply(BigQueryIO.Read.named("qa").from(queries.qa))
				.apply(ParDo.of(new ExtractFromQa()));
		
		PCollection<TableRow> finalJoinPCollection = joins.innerJoin2(tempJoin2PCollection, qaPCollection, "E_");
		
		PCollection<TableRow> resultPCollection = postOperations(finalJoinPCollection);
		
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
//		Joins joins = new Joins();
//		Queries queries = new Queries();
//		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
//		Pipeline pipeline = Pipeline.create(options);
//
//		B2 b2 = new B2();
//
//		PCollection<KV<String, TableRow>> b2PCollection = b2.runIt(pipeline).apply(ParDo.of(new ExtractFromB2()));
//		PCollection<KV<String, TableRow>> cmpgnPCollection = pipeline
//				.apply(BigQueryIO.Read.named("CMPGN").fromQuery(queries.CMPGN))
//				.apply(ParDo.of(new ExtractFromCMPGN()));
//
//		PCollection<TableRow> tempJoin1 = joins.innerJoin2(b2PCollection, cmpgnPCollection, "C_");
//
//		PCollection<KV<String, TableRow>> tempJoin1PCollection = tempJoin1.apply(ParDo.of(new ExtractTempJoin1()));
//		PCollection<KV<String, TableRow>> agentPCollection = pipeline
//				.apply(BigQueryIO.Read.named("agent").from(queries.agent))
//				.apply(ParDo.of(new ExtractFromAgent()));
//
//		PCollection<TableRow> tempJoin2 = joins.innerJoin2(tempJoin1PCollection, agentPCollection, "D_");
//
//		PCollection<KV<String, TableRow>> tempJoin2PCollection = tempJoin2.apply(ParDo.of(new ExtractTempJoin2()));
//		PCollection<KV<String, TableRow>> qaPCollection = pipeline
//				.apply(BigQueryIO.Read.named("qa").from(queries.qa))
//				.apply(ParDo.of(new ExtractFromQa()));
//
//		PCollection<TableRow> finalJoinPCollection = joins.innerJoin2(tempJoin2PCollection, qaPCollection, "E_");
//
//		PCollection<TableRow> resultPCollection = postOperations(finalJoinPCollection);
//
//		resultPCollection.apply(ParDo.of(new ConvertToString()))
//				.apply(TextIO.Write.to(options.getOutput()));
//
//		pipeline.run();
//	}

}
