package com.Practice.GottaTestThis;

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
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.GroupByKey;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.util.List;
import java.util.StringTokenizer;

public class ForJoin7 {
	
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
	
	private static String getDate(String str){
		StringTokenizer stringTokenizer = new StringTokenizer(str);
		return stringTokenizer.nextToken();
	}
	
	private static class ConvertToString extends DoFn<TableRow, String> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			
			context.output(context.element().toPrettyString());
			
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
	
	private static class ReadFromTable2 extends DoFn<TableRow, KV<String, TableRow>>{
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
			String id = (String) tableRow.get("attribute");
			if(id == null){
				context.output(KV.of("null", tableRow));
			}else{
				context.output(KV.of(id, tableRow));
			}
			
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
	private static class ReadFromTable5 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	
	private static class ReadFromJoin1 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("B__id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromJoin2 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("B_attribute");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromJoin3 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("A__id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class ReadFromJoin4 extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("C_campaignId");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private static class Select1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow element = context.element();
			
			String id = (String) element.get("_id   ");
			String campaignId = (String) element.get("campaignId");
			String agentId = (String) element.get("agentId");
			String prospectCallId = (String) element.get("prospectCallId");
			String prospectInteractionSessionId = (String) element.get("prospectInteractionSessionId");
			String callStartTime = (String) element.get("callStartTime");
			String callDate = (String) element.get("callDate");
			String dispositionStatus = (String) element.get("dispositionStatus");
			String subStatus = (String) element.get("subStatus");
			String qaId = (String) element.get("qaId");
			String overallScore = (String) element.get("overallScore");
			String feedbackTime = (String) element.get("feedbackTime");
			String feedbackDate = (String) element.get("feedbackDate");
			
			String finalKey = id + campaignId + agentId + prospectCallId + prospectInteractionSessionId
					+ callStartTime + callDate + dispositionStatus + subStatus + qaId + overallScore
					+ feedbackTime + feedbackDate;
			
			context.output(KV.of(finalKey, element));
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
			tableRow.set("prospectCallId", element.get("C_prospectcallid"));
			tableRow.set("prospectInteractionSessionId", element.get("C_prospectinteractionsessionid"));
			tableRow.set("callDate", getDate((String) element.get("C_callstarttime")));
			tableRow.set("status", element.get("C_status"));
			tableRow.set("dispositionStatus", element.get("C_dispositionstatus"));
			tableRow.set("subStatus", element.get("C_substatus"));
			tableRow.set("qaId", element.get("E_qaId"));
			tableRow.set("overallScore", element.get("E_overallScore"));
			tableRow.set("qaComments", element.get("E_qaComments"));
			tableRow.set("feedbackTime", element.get("E_feedbackTime"));
			tableRow.set("feedbackDate", getDate((String) element.get("E_feedbackTime")));
			tableRow.set("sectionName", element.get("A_sectionName"));
			tableRow.set("attribute", element.get("D_attribute"));
			tableRow.set("attributeComment", element.get("B_attributeComment"));
			tableRow.set("label", element.get("D_label"));
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
	
	private static PCollection<TableRow> operations(PCollection<TableRow> rowPCollection){
		PCollection<TableRow> firstResult = rowPCollection
				.apply(ParDo.named("Meh_v1").of(new DoFn<TableRow, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						
						TableRow element = context.element();
						String feedback = (String) element.get("feedback");
						String attribute = (String) element.get("attribute");
						switch (attribute){
							case "PHONEETIQUETTE_CUSTOMER_ENGAGEMENT":
								element.set("PhoneEtiquette_Customer_Engagement", feedback);
								break;
							case "PHONEETIQUETTE_PROFESSIONALISM":
								element.set("PhoneEtiquette_Professionalism", feedback);
								break;
							case "SALESMANSHIP_REBUTTAL_USE":
								element.set("Salesmanship_Rebuttal_Use", feedback);
								break;
							case "SALESMANSHIP_PROVIDE_INFORMATION":
								element.set("Salesmanship_Provide_Information", feedback);
								break;
							case "INTRODUCTION_BRANDING_PERSONAL_CORPORATE":
								element.set("Introduction_Branding_Personal_Corporate", feedback);
								break;
							case "INTRODUCTION_MARKETING_EFFORTS":
								element.set("Introduction_Marketing_Efforts", feedback);
								break;
							case "CUSTOMERSATISFACTION_OVERALL_SERVICE":
								element.set("CustomerSatisfaction_Overall_Service", feedback);
								break;
							case "CLIENT_FULL_DETAILS":
								element.set("Client_Full_Details", feedback);
								break;
							case "CLIENT_PII":
								element.set("Client_PII", feedback);
								break;
							case "CALLCLOSING_BRANDING_PERSONAL_CORPORATE":
								element.set("CallClosing_Branding_Personal_Corporate", feedback);
								break;
							case "PHONEETIQUETTE_CALL_PACING":
								element.set("PhoneEtiquette_Call_Pacing", feedback);
								break;
							case "PHONEETIQUETTE_CALL_HOLD_PURPOSE":
								element.set("PhoneEtiquette_Call_Hold_Purpose", feedback);
								break;
							case "SALESMANSHIP_PRE-QUALIFICATION_QUESTIONS":
								element.set("Salesmanship_Pre-Qualification_Questions", feedback);
								break;
							case "INTRODUCTION_PREPARE_READY":
								element.set("Introduction_Prepare_Ready", feedback);
								break;
							case "INTRODUCTION_CALL_RECORD":
								element.set("Introduction_Call_Record", feedback);
								break;
							case "CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL":
								element.set("CustomerSatisfaction_Representative_On_Call", feedback);
								break;
							case "CLIENT_POST-QUALIFICATION QUESTIONS":
								element.set("Client_Post-Qualification_Questions", feedback);
								break;
							case "CLIENT_OBTAIN_CUSTOMER_CONSENT":
								element.set("Client_Obtain_Customer_Consent", feedback);
								break;
							case "CALLCLOSING_EXPECTATION_SETTING":
								element.set("CallClosing_Expectation_Setting", feedback);
								break;
							case "CALLCLOSING_REDIAL_NUMBER":
								element.set("CallClosing_Redial_Number", feedback);
								break;
							case "LEAD_VALIDATION_VALID":
								element.set("Lead_Validation_Valid", feedback);
								break;
//							??Get this one checked
//                          case "LEAD_VALIDATION_NOTES":
//								element.set("Lead_Validation_Notes", element.get("attributeComment"));
//								break;
						
						}
						
						element.remove("feedback");
						element.remove("attribute");
						element.remove("attributeComment");
						
						context.output(element);
						
					}
				}));
		
		PCollection<KV<String, TableRow>> pCollection1 = firstResult.apply(ParDo.named("Woo").of(new Select1()));
		
		PCollection<KV<String, Iterable<TableRow>>> groupResult1 = pCollection1.apply(GroupByKey.create());
		
		PCollection<TableRow> iterablePCollection = groupResult1
				.apply(ParDo.named("Meh_v2").of(new DoFn<KV<String, Iterable<TableRow>>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, Iterable<TableRow>> element = context.element();
						Iterable<TableRow> rowIterable = element.getValue();
//						context.output(rowIterable);
						
						TableRow freshRow = rowIterable.iterator().next();
						
						String maxStatus = "null", maxPhoneEtiquetteCustomerEngagement = "null",
								maxPhoneEtiquetteProfessionalism = "null", maxSalesmanshipRebuttalUse = "null",
								maxSalesmanshipProvideInformation = "null", maxIntroductionMarketingEfforts = "null",
								maxIntroductionBrandingPersonalCorporate = "null", maxClientPII = "null",
								maxCustomerSatisfactionOverallService = "null", maxClientFullDetails = "null",
								maxCallClosingBranding = "null", maxPhoneEtiquetteCallPacing = "null",
								maxPhoneEtiquetteCallHoldPurpose = "null", maxSalesmanshipQualificationQuestions = "null",
								maxIntroductionPrepareReady = "null", maxIntroductionCallRecord = "null",
								maxCustomerSatisfactionRepresentativeOnCall = "null",
								maxClientPostQualificationQuestions = "null", maxClientObtainCustomerConsent = "null",
								maxCallClosingExpectationSetting = "null", maxCallClosingRedialNumber = "null",
								maxLeadValidationValid = "null";
						
						
						for(TableRow tableRow : rowIterable){
							
							if(((String) tableRow.get("status")).compareTo(maxStatus) > 0){
								maxStatus = (String) tableRow.get("status");
							}
							
							if(((String) tableRow.get("PhoneEtiquette_Customer_Engagement")).compareTo(maxPhoneEtiquetteCustomerEngagement) > 0){
								maxPhoneEtiquetteCustomerEngagement = (String) tableRow.get("PhoneEtiquette_Customer_Engagement");
							}
							
							if(((String) tableRow.get("PhoneEtiquette_Professionalism")).compareTo(maxPhoneEtiquetteProfessionalism) > 0){
								maxPhoneEtiquetteProfessionalism = (String) tableRow.get("PhoneEtiquette_Professionalism");
							}
							
							if(((String) tableRow.get("Salesmanship_Provide_Information")).compareTo(maxSalesmanshipProvideInformation) > 0){
								maxSalesmanshipProvideInformation = (String) tableRow.get("Salesmanship_Provide_Information");
							}
							
							if(((String) tableRow.get("Salesmanship_Rebuttal_Use")).compareTo(maxSalesmanshipRebuttalUse) > 0){
								maxSalesmanshipRebuttalUse = (String) tableRow.get("Salesmanship_Rebuttal_Use");
							}
							
							if(((String) tableRow.get("Introduction_Marketing_Efforts")).compareTo(maxIntroductionMarketingEfforts) > 0){
								maxIntroductionMarketingEfforts = (String) tableRow.get("Introduction_Marketing_Efforts");
							}
							
							if(((String) tableRow.get("Introduction_Branding_Personal_Corporate")).compareTo(maxIntroductionBrandingPersonalCorporate) > 0){
								maxIntroductionBrandingPersonalCorporate = (String) tableRow.get("Introduction_Branding_Personal_Corporate");
							}
							
							if(((String) tableRow.get("CustomerSatisfaction_Overall_Service")).compareTo(maxCustomerSatisfactionOverallService) > 0){
								maxCustomerSatisfactionOverallService = (String) tableRow.get("CustomerSatisfaction_Overall_Service");
							}
							
							if(((String) tableRow.get("Client_PII")).compareTo(maxClientPII) > 0){
								maxClientPII = (String) tableRow.get("Client_PII");
							}
							
							if(((String) tableRow.get("Client_Full_Details")).compareTo(maxClientFullDetails) > 0){
								maxClientFullDetails = (String) tableRow.get("Client_Full_Details");
							}
							
							if(((String) tableRow.get("CallClosing_Branding_Personal_Corporate")).compareTo(maxCallClosingBranding) > 0){
								maxCallClosingBranding = (String) tableRow.get("CallClosing_Branding_Personal_Corporate");
							}
							
							if(((String) tableRow.get("PhoneEtiquette_Call_Pacing")).compareTo(maxPhoneEtiquetteCallPacing) > 0){
								maxPhoneEtiquetteCallPacing = (String) tableRow.get("PhoneEtiquette_Call_Pacing");
							}
							
							if(((String) tableRow.get("PhoneEtiquette_Call_Hold_Purpose")).compareTo(maxPhoneEtiquetteCallHoldPurpose) > 0){
								maxPhoneEtiquetteCallHoldPurpose = (String) tableRow.get("PhoneEtiquette_Call_Hold_Purpose");
							}
							
							if(((String) tableRow.get("Salesmanship_Pre-Qualification_Questions")).compareTo(maxSalesmanshipQualificationQuestions) > 0){
								maxSalesmanshipQualificationQuestions = (String) tableRow.get("Salesmanship_Pre-Qualification_Questions");
							}
							
							if(((String) tableRow.get("Introduction_Prepare_Ready")).compareTo(maxIntroductionPrepareReady) > 0){
								maxIntroductionPrepareReady = (String) tableRow.get("Introduction_Prepare_Ready");
							}
							
							if(((String) tableRow.get("Introduction_Call_Record")).compareTo(maxIntroductionCallRecord) > 0){
								maxIntroductionCallRecord = (String) tableRow.get("Introduction_Call_Record");
							}
							
							if(((String) tableRow.get("CustomerSatisfaction_Representative_On_Call")).compareTo(maxCustomerSatisfactionRepresentativeOnCall) > 0){
								maxCustomerSatisfactionRepresentativeOnCall = (String) tableRow.get("CustomerSatisfaction_Representative_On_Call");
							}
							
							if(((String) tableRow.get("Client_Post-Qualification_Questions")).compareTo(maxClientPostQualificationQuestions) > 0){
								maxClientPostQualificationQuestions = (String) tableRow.get("Client_Post-Qualification_Questions");
							}
							
							if(((String) tableRow.get("Client_Obtain_Customer_Consent")).compareTo(maxClientObtainCustomerConsent) > 0){
								maxClientObtainCustomerConsent = (String) tableRow.get("Client_Obtain_Customer_Consent");
							}
							
							if(((String) tableRow.get("CallClosing_Expectation_Setting")).compareTo(maxCallClosingExpectationSetting) > 0){
								maxCallClosingExpectationSetting = (String) tableRow.get("CallClosing_Expectation_Setting");
							}
							
							if(((String) tableRow.get("CallClosing_Redial_Number")).compareTo(maxCallClosingRedialNumber) > 0){
								maxCallClosingRedialNumber = (String) tableRow.get("CallClosing_Redial_Number");
							}
							
							if(((String) tableRow.get("Lead_Validation_Valid")).compareTo(maxLeadValidationValid) > 0){
								maxLeadValidationValid = (String) tableRow.get("Lead_Validation_Valid");
							}
						}
						
						freshRow.set("status", maxStatus);
						freshRow.set("PhoneEtiquette_Customer_Engagement", maxPhoneEtiquetteCustomerEngagement);
						freshRow.set("PhoneEtiquette_Professionalism", maxPhoneEtiquetteProfessionalism);
						freshRow.set("Salesmanship_Rebuttal_Use", maxSalesmanshipRebuttalUse);
						freshRow.set("Salesmanship_Provide_Information", maxSalesmanshipProvideInformation);
						freshRow.set("Introduction_Branding_Personal_Corporate", maxIntroductionBrandingPersonalCorporate);
						freshRow.set("Introduction_Marketing_Efforts", maxIntroductionMarketingEfforts);
						freshRow.set("CustomerSatisfaction_Overall_Service", maxCustomerSatisfactionOverallService);
						freshRow.set("Client_Full_Details", maxClientFullDetails);
						freshRow.set("Client_PII", maxClientPII);
						freshRow.set("CallClosing_Branding_Personal_Corporate", maxCallClosingBranding);
						freshRow.set("PhoneEtiquette_Call_Pacing", maxPhoneEtiquetteCallPacing);
						freshRow.set("PhoneEtiquette_Call_Hold_Purpose", maxPhoneEtiquetteCallHoldPurpose);
						freshRow.set("Salesmanship_Pre-Qualification_Questions", maxSalesmanshipQualificationQuestions);
						freshRow.set("Introduction_Prepare_Ready", maxIntroductionPrepareReady);
						freshRow.set("Introduction_Call_Record", maxIntroductionCallRecord);
						freshRow.set("CustomerSatisfaction_Representative_On_Call", maxCustomerSatisfactionRepresentativeOnCall);
						freshRow.set("Client_Post-Qualification_Questions", maxClientPostQualificationQuestions);
						freshRow.set("Client_Obtain_Customer_Consent", maxClientObtainCustomerConsent);
						freshRow.set("CallClosing_Expectation_Setting", maxCallClosingExpectationSetting);
						freshRow.set("CallClosing_Redial_Number", maxCallClosingRedialNumber);
						freshRow.set("Lead_Validation_Valid", maxLeadValidationValid);
						
						context.output(freshRow);
					}
				}));
		
		return iterablePCollection;
	}
	
	public PCollection<TableRow> runIt(Pipeline pipeline){
		
		Queries queries = new Queries();
		
		List<Field> fieldMetaDataList1 = getThemFields("Xtaas","pci_feedbackResponseList");
		List<Field> fieldMetaDataList2 = getThemFields("Xtaas","pci_responseAttributes");
		List<Field> fieldMetaDataList3 = getThemFields("Xtaas","PC_PCI");
		List<Field> fieldMetaDataList4 = getThemFields("Xtaas", "qafeedbackformattributes");
		List<Field> fieldMetaDataList5 = getThemFields("Xtaas", "pci_qafeedback");
		List<Field> fieldMetaDataList6 = getThemFields("Xtaas", "CMPGN");
		
		//pci_feedbackresponselist(A) with pci_responseattributes(B) {A._id = B._id & A.INDEX = B.index}
		PCollection<KV<String, TableRow>> source1Table = pipeline
				.apply(BigQueryIO.Read.named("Reader1").from(queries.pciFeedbackResponseList))
				.apply(ParDo.named("FormatData1").of(new ReadFromTable1()));
		
		PCollection<KV<String, TableRow>> source2Table = pipeline
				.apply(BigQueryIO.Read.named("Reader2").from(queries.pciResponseAttributes))
				.apply(ParDo.named("FormatData2").of(new ReadFromTable1()));
		
		PCollection<TableRow> joinResult1 = combineTableDetails(source1Table, source2Table,
				fieldMetaDataList1, fieldMetaDataList2, "A_", "B_");
		
		//with PC_PCI(P) {B._id = P._id}
		PCollection<KV<String, TableRow>> source3Table = joinResult1
				.apply(ParDo.named("FormatData3").of(new ReadFromJoin1()));
		
		PCollection<KV<String, TableRow>> source4Table = pipeline
				.apply(BigQueryIO.Read.named("Reader4").fromQuery(queries.PC_PCI))
				.apply(ParDo.named("FormatData4").of(new ReadFromTable2()));
		
		PCollection<TableRow> joinResult2 = combineTableDetails2(source3Table, source4Table,
				fieldMetaDataList3, "C_");
		
		//with qafeedbackformattributes(E) {B.attribute = E.attribute}
		PCollection<KV<String, TableRow>> source5Table = joinResult2
				.apply(ParDo.named("FormatData5").of(new ReadFromJoin2()));
		
		PCollection<KV<String, TableRow>> source6Table = pipeline
				.apply(BigQueryIO.Read.named("Reader6").from(queries.qaFeedbackFormAttributes))
				.apply(ParDo.named("FormatData6").of(new ReadFromTable3()));
		
		PCollection<TableRow> joinResult3 = combineTableDetails2(source5Table, source6Table,
				fieldMetaDataList4, "D_");
		
		//with pci_qafeedback(F) {A._id = F._id}
		PCollection<KV<String, TableRow>> source7Table = joinResult3
				.apply(ParDo.named("FormatData7").of(new ReadFromJoin3()));
		
		PCollection<KV<String, TableRow>> source8Table = pipeline
				.apply(BigQueryIO.Read.named("Reader8").from(queries.pciQaFeedback))
				.apply(ParDo.named("FormatData8").of(new ReadFromTable4()));
		
		PCollection<TableRow> joinResult4 = combineTableDetails2(source7Table, source8Table,
				fieldMetaDataList5, "E_");
		
		//with CMPGN(G) {P.campaignid = G._id}
		PCollection<KV<String, TableRow>> source9Table = joinResult4
				.apply(ParDo.named("FormatData9").of(new ReadFromJoin4()));
		
		PCollection<KV<String, TableRow>> source10Table = pipeline
				.apply(BigQueryIO.Read.named("Reader10").fromQuery(queries.CMPGN))
				.apply(ParDo.named("FormatData10").of(new ReadFromTable5()));
		
		PCollection<TableRow> finalResult = combineTableDetails2(source9Table, source10Table,
				fieldMetaDataList6, "F_");
		
		
		PCollection<TableRow> rowPCollection = finalResult.apply(ParDo.of(new FinalFieldTableRow()));
		
		PCollection<TableRow> iterablePCollection = operations(rowPCollection);
		
		return iterablePCollection;
	
	}

}
