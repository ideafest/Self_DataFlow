import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class BQuery_v2 {
	
	public static String readFile(String filePath) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(filePath));
		try {
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			
			while (line != null) {
				sb.append(line);
				sb.append("\n");
				line = br.readLine();
			}
			return sb.toString();
		} finally {
			br.close();
		}
	}
	
	private static class ExtractAndSend extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
		}
	}
	
	private static interface Options extends PipelineOptions {
		@Description("Input path to file containing query")
		@Validation.Required
		String getInputPath();
		void setInputPath(String inputPath);
		
		@Description("Output Path")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) throws IOException {
		
		Options pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		pipelineOptions.setTempLocation("gs://practice001/temp");
		
		Pipeline pipeline = Pipeline.create(pipelineOptions);
		
		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
		fieldSchemaList.add(new TableFieldSchema().setName("campaignid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("campaign").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("rownum").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("batch_date").setType("DATE"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospectcallid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospectinteractionsessionid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("DNI").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("record_date").setType("DATE"));
		fieldSchemaList.add(new TableFieldSchema().setName("record_datetime").setType("TIMESTAMP"));
		fieldSchemaList.add(new TableFieldSchema().setName("call_date").setType("DATE"));
		fieldSchemaList.add(new TableFieldSchema().setName("call_datetime").setType("TIMESTAMP"));
		fieldSchemaList.add(new TableFieldSchema().setName("callstartdate").setType("DATE"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospecthandledurationformatted").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("voicedurationformatted").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("status").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("dispositionstatus").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("substatus").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("recordingurl").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("twiliocallsid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("deliveredassetid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("callbackdate").setType("TIMESTAMP"));
		fieldSchemaList.add(new TableFieldSchema().setName("outboundnumber").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("latestprospectidentitychangelogid").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("callretrycount").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("dnc_note").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("dnc_trigger").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("AgentName").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("QAName").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("_id").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("callstarttime").setType("TIMESTAMP"));
		fieldSchemaList.add(new TableFieldSchema().setName("qaid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("overallscore").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("qastatus").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("feedbackdate").setType("DATE"));
		fieldSchemaList.add(new TableFieldSchema().setName("feedbacktime").setType("TIMESTAMP"));
		fieldSchemaList.add(new TableFieldSchema().setName("clintOfferAndsend").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("Salesmanship").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("CallClosing").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("Introduction").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("PhoneEtiquette").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("LeadValidation").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("PHONEETIQUETTE_CUSTOMER_ENGAGEMENT").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("PHONEETIQUETTE_PROFESSIONALISM").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("SALESMANSHIP_REBUTTAL_USE").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("SALESMANSHIP_PROVIDE_INFORMATION").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("INTRODUCTION_BRANDING_PERSONAL_CORPORATE").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("INTRODUCTION_MARKETING_EFFORTS").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CUSTOMERSATISFACTION_OVERALL_SERVICE").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CLIENT_FULL_DETAILS").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CLIENT_PII").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CALLCLOSING_BRANDING_PERSONAL_CORPORATE").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("PHONEETIQUETTE_CALL_PACING").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("PHONEETIQUETTE_CALL_HOLD_PURPOSE").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("SALESMANSHIP_PREQUALIFICATION_QUESTIONS").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("INTRODUCTION_PREPARE_READY").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("INTRODUCTION_CALL_RECORD").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CLIENT_POSTQUALIFICATION_QUESTIONS").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CLIENT_OBTAIN_CUSTOMER_CONSENT").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CALLCLOSING_EXPECTATION_SETTING").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CALLCLOSING_REDIAL_NUMBER").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("LEAD_VALIDATION_VALID").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("lead_validation_notes").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("Revenue").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("EventTimeline").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("Industry").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("industrylist").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("Budget").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("CurrentlyEvaluatingCallAnalytics").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("Department").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("Title").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("NumberOfEmployees").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("Country").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("industry1").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("industry2").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("industry3").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("industry4").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_callback").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_success").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_failure").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_dialercode").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_contact").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_contact_pcid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_max_retry_limit_reached").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_LOCAL_DNC_ERROR").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_NATIONAL_DNC_ERROR").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_CALL_ABANDONED").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_qa_count").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_call_count").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_answer_machine").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_busy").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_no_answer").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_unreachable").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("call_attempted").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("interaction_pcid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("queued").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_callback").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_success").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_failure").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_dialercode").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_contact").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_contact_pcid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("dnc_added").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_max_retry_limit_reached").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_LOCAL_DNC_ERROR").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_NATIONAL_DNC_ERROR").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_CALL_ABANDONED").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_qa_count").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_count").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("bad_record").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_answer_machine").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_busy").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_no_answer").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospect_unreachable").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("prospecthandleduration").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("callduration").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("voiceduration").setType("INTEGER"));
		fieldSchemaList.add(new TableFieldSchema().setName("agentid").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_day").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_month").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_year").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_week").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_dayofweek").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_dayofyear").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_hour").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_minute").setType("FLOAT"));
		fieldSchemaList.add(new TableFieldSchema().setName("transactiondate_second").setType("FLOAT"));
		TableSchema schema = new TableSchema().setFields(fieldSchemaList);
		
		pipeline.apply(BigQueryIO.Read.named("Reader").fromQuery(readFile(pipelineOptions.getInputPath())))
				.apply(ParDo.of(new ExtractAndSend()))
				.apply(BigQueryIO.Write.named("Writer").to(pipelineOptions.getOutput())
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		pipeline.run();
		
	}
	
}
