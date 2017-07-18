package com.example;

import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;

import java.util.ArrayList;
import java.util.List;

public class BQuery_v2 {
	
	private static String query = "select *\n" +
			"from\n" +
			"(\n" +
			"SELECT\n" +
			"      A.campaignid as campaignid,\n" +
			"      A.campaign as campaign,\n" +
			"      A.rownum as rownum,\n" +
			"      cast(date(A.batch_date) as date) as batch_date,\n" +
			"      A.prospectcallid as prospectcallid,\n" +
			"      A.prospectinteractionsessionid as prospectinteractionsessionid,\n" +
			"      A.DNI as DNI,\n" +
			"      cast(date(A.updateddate) as date) AS record_date,\n" +
			"      A.updateddate as record_datetime,\n" +
			"      cast(date(A.updateddate) as date) AS call_date,\n" +
			"      A.updateddate AS call_datetime,\n" +
			"      cast(date(A.callstartdate) as date) as callstartdate,\n" +
			"      A.prospecthandledurationformatted as prospecthandledurationformatted,\n" +
			"      A.voicedurationformatted as voicedurationformatted,\n" +
			"      E.name as status,\n" +
			"      nvl(F.name, 'Not Available') as dispositionstatus,\n" +
			"      nvl(G.name, 'Not Available')  as substatus,\n" +
			"      A.recordingurl as recordingurl,\n" +
			"      A.twiliocallsid as twiliocallsid,\n" +
			"      A.deliveredassetid as deliveredassetid,\n" +
			"      A.callbackdate as callbackdate,\n" +
			"      A.outboundnumber as outboundnumber,\n" +
			"      A.latestprospectidentitychangelogid as latestprospectidentitychangelogid,\n" +
			"      A.callretrycount as callretrycount,\n" +
			"      A.dnc_note as dnc_note,\n" +
			"      A.dnc_trigger as dnc_trigger,\n" +
			"      C.AgentName as AgentName,\n" +
			"      C.QAName as QAName,\n" +
			"      C._id as _id, \n" +
			"      C.callstarttime as callstarttime,\n" +
			"      C.qaid as qaid,\n" +
			"      C.overallscore as overallscore,\n" +
			"      case when C.STATUS = 'QA_INITIATED' then 'QA Initiated'\n" +
			"           when C.STATUS = 'QA_COMPLETE' then 'QA Completed'\n" +
			"           when A.STATUS = 'WRAPUP_COMPLETE' and A.dispositionstatus in ('SUCCESS', 'FAILURE') then 'QA Pending'\n" +
			"           else null\n" +
			"      end as qastatus,\n" +
			"      C.feedbackdate as feedbackdate,\n" +
			"      C.feedbacktime as feedbacktime,\n" +
			"      C.clintOfferAndsend as clintOfferAndsend,\n" +
			"      C.Salesmanship as Salesmanship,\n" +
			"      C.CallClosing as CallClosing,\n" +
			"      C.Introduction as Introduction,\n" +
			"      C.PhoneEtiquette as PhoneEtiquette,\n" +
			"      C.LeadValidation as LeadValidation,\n" +
			"      C.PHONEETIQUETTE_CUSTOMER_ENGAGEMENT as PHONEETIQUETTE_CUSTOMER_ENGAGEMENT,\n" +
			"      C.PHONEETIQUETTE_PROFESSIONALISM as PHONEETIQUETTE_PROFESSIONALISM,\n" +
			"      C.SALESMANSHIP_REBUTTAL_USE as SALESMANSHIP_REBUTTAL_USE,\n" +
			"      C.SALESMANSHIP_PROVIDE_INFORMATION as SALESMANSHIP_PROVIDE_INFORMATION,\n" +
			"      C.INTRODUCTION_BRANDING_PERSONAL_CORPORATE as INTRODUCTION_BRANDING_PERSONAL_CORPORATE,\n" +
			"      C.INTRODUCTION_MARKETING_EFFORTS as INTRODUCTION_MARKETING_EFFORTS,\n" +
			"      C.CUSTOMERSATISFACTION_OVERALL_SERVICE as CUSTOMERSATISFACTION_OVERALL_SERVICE,\n" +
			"      C.CLIENT_FULL_DETAILS as CLIENT_FULL_DETAILS,\n" +
			"      C.CLIENT_PII as CLIENT_PII,\n" +
			"      C.CALLCLOSING_BRANDING_PERSONAL_CORPORATE as CALLCLOSING_BRANDING_PERSONAL_CORPORATE,\n" +
			"      C.PHONEETIQUETTE_CALL_PACING as PHONEETIQUETTE_CALL_PACING,\n" +
			"      C.PHONEETIQUETTE_CALL_HOLD_PURPOSE as PHONEETIQUETTE_CALL_HOLD_PURPOSE,\n" +
			"      C.SALESMANSHIP_PREQUALIFICATION_QUESTIONS as SALESMANSHIP_PREQUALIFICATION_QUESTIONS,\n" +
			"      C.INTRODUCTION_PREPARE_READY as INTRODUCTION_PREPARE_READY,\n" +
			"      C.INTRODUCTION_CALL_RECORD as INTRODUCTION_CALL_RECORD,\n" +
			"      C.CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL as CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL,\n" +
			"      C.CLIENT_POSTQUALIFICATION_QUESTIONS as CLIENT_POSTQUALIFICATION_QUESTIONS,\n" +
			"      C.CLIENT_OBTAIN_CUSTOMER_CONSENT as CLIENT_OBTAIN_CUSTOMER_CONSENT,\n" +
			"      C.CALLCLOSING_EXPECTATION_SETTING as CALLCLOSING_EXPECTATION_SETTING,\n" +
			"      C.CALLCLOSING_REDIAL_NUMBER as CALLCLOSING_REDIAL_NUMBER,\n" +
			"      C.LEAD_VALIDATION_VALID as LEAD_VALIDATION_VALID,   \n" +
			"      C.lead_validation_notes as lead_validation_notes,\n" +
			"      D.Revenue as Revenue,\n" +
			"      D.EventTimeline as EventTimeline,\n" +
			"      D.Industry as Industry,\n" +
			"      D.industrylist as industrylist,\n" +
			"      D.Budget as Budget,\n" +
			"      D.CurrentlyEvaluatingCallAnalytics as CurrentlyEvaluatingCallAnalytics,\n" +
			"      D.Department as Department,\n" +
			"      D.Title as Title,\n" +
			"      D.NumberOfEmployees as NumberOfEmployees,\n" +
			"      D.Country as Country,\n" +
			"     \n" +
			"      nth(1,split(D.industrylist,',')) as industry1,\n" +
			"      nth(2,split(D.industrylist,',')) as industry2,\n" +
			"      nth(3,split(D.industrylist,',')) as industry3,\n" +
			"      nth(4,split(D.industrylist,',')) as industry4,\n" +
			"\n" +
			"      case when A.substatus='CALLBACK_USER_GENERATED' then 1 else 0 end as interaction_callback,\n" +
			"      case when A.dispositionstatus='SUCCESS' then 1 else 0 end as interaction_success,\n" +
			"      case when A.dispositionstatus='FAILURE' then 1 else 0 end as interaction_failure,\n" +
			"\n" +
			"      case when A.dispositionstatus = 'DIALERCODE' then 1\n" +
			"            else 0\n" +
			"      end as interaction_dialercode,\n" +
			"\n" +
			"      case when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'SUCCESS' then 1\n" +
			"            when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'FAILURE'\n" +
			"                 and A.substatus in ('DNCL', 'DNRM', 'FAILED_QUALIFICATION', 'NO_CONSENT', 'NOTINTERESTED', 'OTHER', 'OUT_OF_COUNTRY', 'PROSPECT_UNREACHABLE') then 1\n" +
			"            when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'CALLBACK'\n" +
			"                 and A.substatus IN ('CALLBACK_USER_GENERATED', 'CALLBACK_GATEKEEPER') then 1\n" +
			"            when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'DIALERCODE'\n" +
			"                 and A.substatus = 'GATEKEEPER_ANSWERMACHINE' then 1\n" +
			"            else 0\n" +
			"      end as interaction_contact,\n" +
			"\n" +
			"      case when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'SUCCESS' then A.prospectcallid\n" +
			"           when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'FAILURE'\n" +
			"                  and A.substatus in ('DNCL', 'DNRM', 'FAILED_QUALIFICATION', 'NO_CONSENT', 'NOTINTERESTED', 'OTHER', 'OUT_OF_COUNTRY', 'PROSPECT_UNREACHABLE') then A.prospectcallid\n" +
			"            when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'CALLBACK'\n" +
			"                 and A.substatus IN ('CALLBACK_USER_GENERATED', 'CALLBACK_GATEKEEPER') then A.prospectcallid\n" +
			"            when A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'DIALERCODE'\n" +
			"                 and A.substatus = 'GATEKEEPER_ANSWERMACHINE' then A.prospectcallid\n" +
			"            else null\n" +
			"      end as interaction_contact_pcid,\n" +
			"\n" +
			"      case when A.status='MAX_RETRY_LIMIT_REACHED' then 1 else 0 end as interaction_max_retry_limit_reached,\n" +
			"      case when A.status='LOCAL_DNC_ERROR' then 1 else 0 end as interaction_LOCAL_DNC_ERROR,\n" +
			"      case when A.status='NATIONAL_DNC_ERROR' then 1 else 0 end as interaction_NATIONAL_DNC_ERROR,\n" +
			"      case when A.status='CALL_ABANDONED' then 1 else 0 end as interaction_CALL_ABANDONED,\n" +
			"      case when C.status='QA_COMPLETE' then 1 else 0 end as interaction_qa_count,\n" +
			"\n" +
			"      case when A.prospectinteractionsessionid is not null then 1 else 0 end as interaction_call_count,\n" +
			"\n" +
			"      case when A.substatus='ANSWERMACHINE' then 1 else 0 end as interaction_answer_machine,\n" +
			"      case when A.substatus='BUSY' then 1 else 0 end as interaction_busy,\n" +
			"      case when A.substatus='NOANSWER' then 1 else 0 end as interaction_no_answer,\n" +
			"      case when A.substatus='NO_LONGER_EMPLOYED' then 1 when A.substatus='PROSPECT_UNREACHABLE' then 1 else 0 end as interaction_unreachable,\n" +
			"\n" +
			"      case when A.status_seq > 3 then 1 else 0 end as call_attempted,\n" +
			"      case when A.status_seq > 3 then A.prospectcallid else null end as interaction_pcid,\n" +
			"\n" +
			"      case when A.rownum = 1 and A.status = 'QUEUED' then 1\n" +
			"           when A.rownum = 1 and A.dispositionstatus = 'CALLBACK' then 1\n" +
			"           when A.rownum = 1 and (A.dispositionstatus = 'DIALERCODE' and A.substatus not in ('DIALERMISDETECT','ANSWERMACHINE'))then 1\n" +
			"           else 0\n" +
			"      end as queued,\n" +
			"\n" +
			"      case when concat(string(A.rownum),A.substatus)='1CALLBACK_USER_GENERATED' then 1 else 0 end as prospect_callback,\n" +
			"      case when concat(string(A.rownum),A.dispositionstatus)='1SUCCESS' then 1 else 0 end as prospect_success,\n" +
			"      case when concat(string(A.rownum),A.dispositionstatus)='1FAILURE' then 1 else 0 end as prospect_failure,\n" +
			"\n" +
			"      case when A.rownum = 1 and A.dispositionstatus = 'DIALERCODE' then 1\n" +
			"           else 0\n" +
			"      end as prospect_dialercode,\n" +
			"\n" +
			"     case when A.rownum = 1 and (A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'SUCCESS') then 1\n" +
			"          when A.rownum = 1 and ((A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'FAILURE')\n" +
			"                 and A.substatus in ('DNCL', 'DNRM', 'FAILED_QUALIFICATION', 'NO_CONSENT', 'NOTINTERESTED')) then 1\n" +
			"          when A.rownum = 1 and (A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND\n" +
			"               A.dispositionstatus = 'CALLBACK' and A.substatus = 'CALLBACK_USER_GENERATED') then 1\n" +
			"                else 0\n" +
			"          end as prospect_contact,\n" +
			"\n" +
			"     case when A.rownum = 1 and (A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'SUCCESS') then A.prospectcallid\n" +
			"          when A.rownum = 1 and (A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND A.dispositionstatus = 'FAILURE'\n" +
			"               and A.substatus in ('DNCL', 'DNRM', 'FAILED_QUALIFICATION', 'NO_CONSENT', 'NOTINTERESTED')) then A.prospectcallid\n" +
			"          when A.rownum = 1 and (A.status IN ('WRAPUP_COMPLETE', 'QA_INITIATED', 'QA_COMPLETE') AND\n" +
			"                 A.dispositionstatus = 'CALLBACK' and A.substatus = 'CALLBACK_USER_GENERATED') then A.prospectcallid\n" +
			"          else null\n" +
			"          end as prospect_contact_pcid,\n" +
			"\n" +
			"        case when concat(string(A.rownum),A.substatus)='1DNCL' then 1 else 0 end as dnc_added,\n" +
			"        case when concat(string(A.rownum),A.status)='1MAX_RETRY_LIMIT_REACHED' then 1 else 0 end as prospect_max_retry_limit_reached,\n" +
			"        case when concat(string(A.rownum),A.status)='1LOCAL_DNC_ERROR' then 1 else 0 end as prospect_LOCAL_DNC_ERROR,\n" +
			"        case when concat(string(A.rownum),A.status)='1NATIONAL_DNC_ERROR' then 1 else 0 end as prospect_NATIONAL_DNC_ERROR,\n" +
			"        case when concat(string(A.rownum),A.status)='1CALL_ABANDONED' then 1 else 0 end as prospect_CALL_ABANDONED,\n" +
			"        case when concat(string(A.rownum),C.status)='1QA_COMPLETE' then 1 else 0 end as prospect_qa_count,\n" +
			"\n" +
			"        case when A.rownum=1 then 1 else 0 end as prospect_count,\n" +
			"\n" +
			"\n" +
			"        case when concat(string(A.rownum),A.substatus)='1BADNUMBER' then 1 when concat(string(A.rownum),A.substatus)='FAKENUMBER' then 1 else 0 end as bad_record,\n" +
			"\n" +
			"        case when concat(string(A.rownum),A.substatus)='1ANSWERMACHINE' then 1 else 0 end as prospect_answer_machine,\n" +
			"        case when concat(string(A.rownum),A.substatus)='1BUSY' then 1 else 0 end as prospect_busy,\n" +
			"        case when concat(string(A.rownum),A.substatus)='1NOANSWER' then 1 else 0 end as prospect_no_answer,\n" +
			"        case when concat(string(A.rownum),A.substatus)='1NO_LONGER_EMPLOYED' then 1 when concat(string(A.rownum),A.substatus)='1PROSPECT_UNREACHABLE' then 1 else 0 end as prospect_unreachable,\n" +
			"     \n" +
			"      case when A.status in('UNKNOWN_ERROR','QUEUED','MAX_RETRY_LIMIT_REACHED','LOCAL_DNC_ERROR','CALL_TIME_RESTRICTION','CALL_NO_ANSWER',\n" +
			"                            'CALL_FAILED','CALL_CANCELED','CALL_BUSY', 'CALL_ABANDONED', 'NATIONAL_DNC_ERROR') then 0 else A.prospecthandleduration end AS prospecthandleduration,\n" +
			"  \n" +
			"      case when A.status in('UNKNOWN_ERROR','QUEUED','MAX_RETRY_LIMIT_REACHED','LOCAL_DNC_ERROR','CALL_TIME_RESTRICTION','CALL_NO_ANSWER','CALL_FAILED',\n" +
			"                                        'CALL_CANCELED','CALL_BUSY','CALL_ABANDONED','NATIONAL_DNC_ERROR') then 0 else A.callduration end AS callduration,\n" +
			"                                   \n" +
			"      case when A.status in('UNKNOWN_ERROR','QUEUED','MAX_RETRY_LIMIT_REACHED','LOCAL_DNC_ERROR','CALL_TIME_RESTRICTION','CALL_NO_ANSWER','CALL_FAILED',\n" +
			"                                         'CALL_CANCELED','CALL_BUSY','CALL_ABANDONED','NATIONAL_DNC_ERROR') then 0 else A.voiceduration end AS voiceduration,\n" +
			"                                               \n" +
			"      case when A.status not in('UNKNOWN_ERROR','QUEUED','MAX_RETRY_LIMIT_REACHED','LOCAL_DNC_ERROR','CALL_TIME_RESTRICTION','CALL_NO_ANSWER','CALL_FAILED',\n" +
			"                                 'CALL_CANCELED','CALL_BUSY','CALL_ABANDONED', 'NATIONAL_DNC_ERROR') then A.agentid else 'SYSTEM' end AS agentid,\n" +
			" \n" +
			"                                                \n" +
			"      cast(day(A.updateddate) as float) as transactiondate_day,\n" +
			"      cast(month(A.updateddate) as float) as transactiondate_month,\n" +
			"      cast(year(A.updateddate) as float) as transactiondate_year,\n" +
			"      cast(week(A.updateddate) as float) as transactiondate_week,\n" +
			"      cast(dayofweek(A.updateddate)-1 as float) as transactiondate_dayofweek,\n" +
			"      cast(dayofyear(A.updateddate) as float) as transactiondate_dayofyear,\n" +
			"      cast(hour(A.updateddate) as float) as transactiondate_hour,\n" +
			"      cast(minute(A.updateddate) as float) as transactiondate_minute,\n" +
			"      cast(second(A.updateddate) as float) as transactiondate_second\n" +
			"\n" +
			"FROM\n" +
			"( select row_number() over(partition by campaignid,prospectcallid order by updateddate desc) as rownum, \n" +
			"  --A.*\n" +
			"  \n" +
			"           campaignid,\n" +
			"           agentid,\n" +
			"           prospectcallid,\n" +
			"           prospectinteractionsessionid,\n" +
			"            DNI,\n" +
			"            callstarttime,\n" +
			"            callstartdate,\n" +
			"            prospecthandledurationformatted,\n" +
			"            voicedurationformatted,\n" +
			"            prospecthandleduration,\n" +
			"            voiceduration,\n" +
			"            callduration,\n" +
			"            status,\n" +
			"            dispositionstatus, --status,  --: \"QA_COMPLETE\"}).count()   \n" +
			"            substatus,\n" +
			"            recordingurl,\n" +
			"            createddate,\n" +
			"            updateddate,\n" +
			"            status_seq,\n" +
			"            twiliocallsid ,\n" +
			"            deliveredassetid ,\n" +
			"            callbackdate,\n" +
			"            outboundnumber ,\n" +
			"            latestprospectidentitychangelogid,\n" +
			"            callretrycount,\n" +
			"\n" +
			"           campaign,\n" +
			"           batch_date ,\n" +
			"           dnc_note as dnc_note,\n" +
			"           dnc_trigger as dnc_trigger\n" +
			"  \n" +
			"  FROM\n" +
			"(\n" +
			"SELECT --A.*,\n" +
			"            A.campaignid as campaignid,\n" +
			"            A.agentid as agentid,\n" +
			"            A.prospectcallid as prospectcallid,\n" +
			"            A.prospectinteractionsessionid as prospectinteractionsessionid ,\n" +
			"            A.DNI as DNI,\n" +
			"            A.callstarttime as callstarttime,\n" +
			"            A.callstartdate as callstartdate,\n" +
			"            A.prospecthandledurationformatted as prospecthandledurationformatted,\n" +
			"            A.voicedurationformatted as voicedurationformatted,\n" +
			"            A.prospecthandleduration as prospecthandleduration,\n" +
			"            A.voiceduration as voiceduration,\n" +
			"            A.callduration as callduration,\n" +
			"            A.status as status,\n" +
			"            A.dispositionstatus as dispositionstatus,--status,  --: \"QA_COMPLETE\"}).count()   \n" +
			"            A.substatus as substatus ,\n" +
			"            A.recordingurl as recordingurl,\n" +
			"            A.createddate as createddate,\n" +
			"            A.updateddate as updateddate,\n" +
			"            A.status_seq as status_seq,\n" +
			"            A.twiliocallsid as twiliocallsid ,\n" +
			"            A.deliveredassetid as deliveredassetid ,\n" +
			"            A.callbackdate as callbackdate,\n" +
			"            A.outboundnumber as outboundnumber ,\n" +
			"            A.latestprospectidentitychangelogid as latestprospectidentitychangelogid,\n" +
			"            A.callretrycount as callretrycount,\n" +
			"\n" +
			"       B.name AS campaign,\n" +
			"       C.batch_date as batch_date ,\n" +
			"           case when substatus = 'DNCL' then 'DNCL requested by prospect while on the call' else null end as dnc_note,\n" +
			"           case when substatus = 'DNCL' then 'CALL' else null end as dnc_trigger\n" +
			"    FROM\n" +
			"    (\n" +
			"\n" +
			"     select\n" +
			"            campaignid,\n" +
			"            agentid,\n" +
			"            prospectcallid,\n" +
			"            prospectinteractionsessionid ,\n" +
			"            DNI,\n" +
			"            callstarttime,\n" +
			"            callstartdate,\n" +
			"            prospecthandledurationformatted ,\n" +
			"            voicedurationformatted,\n" +
			"            prospecthandleduration,\n" +
			"            voiceduration,\n" +
			"            callduration,\n" +
			"            status,\n" +
			"            dispositionstatus,  --status, --   \n" +
			"            substatus,\n" +
			"            recordingurl,\n" +
			"            createddate,\n" +
			"            updateddate,\n" +
			"            status_seq,\n" +
			"            twiliocallsid,\n" +
			"            deliveredassetid,\n" +
			"            callbackdate,\n" +
			"            outboundnumber,\n" +
			"            latestprospectidentitychangelogid,\n" +
			"            callretrycount\n" +
			"     from\n" +
			"      (\n" +
			"     select row_number() over(partition by B.campaignid, B.prospectcallid, B.prospectinteractionsessionid order by B.updateddate desc) as interactionrownum,\n" +
			"               B.campaignid as campaignid,\n" +
			"               B.agentid as agentid,\n" +
			"               B.prospectcallid as prospectcallid,\n" +
			"               B.prospectinteractionsessionid as prospectinteractionsessionid,\n" +
			"               B.createddate as createddate,\n" +
			"               B.updateddate as updateddate,\n" +
			"               B.prospecthandleduration as prospecthandleduration,\n" +
			"               B.DNI as DNI,\n" +
			"               B.callstarttime as callstarttime,\n" +
			"               B.callstartdate as callstartdate,\n" +
			"               B.prospecthandledurationformatted as prospecthandledurationformatted,\n" +
			"               B.voicedurationformatted as voicedurationformatted,\n" +
			"               B.voiceduration as voiceduration,\n" +
			"               B.callduration as callduration,\n" +
			"               B.status as status,\n" +
			"               B.dispositionstatus as dispositionstatus,\n" +
			"               B.substatus as substatus,\n" +
			"               B.recordingurl as recordingurl ,\n" +
			"               B.status_seq as status_seq,\n" +
			"               B.twiliocallsid as twiliocallsid,\n" +
			"               B.deliveredassetid as deliveredassetid,\n" +
			"               B.callbackdate as callbackdate,\n" +
			"               B.outboundnumber as outboundnumber,\n" +
			"               B.latestprospectidentitychangelogid as latestprospectidentitychangelogid,\n" +
			"               B.callretrycount as callretrycount\n" +
			"     FROM\n" +
			"     (\n" +
			"       SELECT     P.campaignid as campaignid,\n" +
			"                  P.prospectcallid as prospectcallid,\n" +
			"                  nvl(P.prospectinteractionsessionid, concat(P.prospectcallid,'-',RPAD(string(P.updatedDate),19,'0'),'-',D.code)) as prospectinteractionsessionid ,\n" +
			"                  MAX(D.status_seq) status_seq\n" +
			"          FROM [vantage-167009:Xtaas.PC_PCI] P INNER JOIN [vantage-167009:Xtaas.master_status] D ON P.status = D.code\n" +
			"          GROUP BY campaignid,prospectcallid,prospectinteractionsessionid\n" +
			"        \n" +
			"     ) A\n" +
			"     INNER JOIN\n" +
			"     (  SELECT P.campaignid as campaignid,\n" +
			"               P.agentid as agentid,\n" +
			"               P.prospectcallid as prospectcallid,\n" +
			"               nvl(P.prospectinteractionsessionid,concat(P.prospectcallid,'-',RPAD(string(P.updatedDate),19,'0'),'-',D.code)) as prospectinteractionsessionid,\n" +
			"               P.createddate as createddate,\n" +
			"               P.updateddate as updateddate,\n" +
			"               P.prospecthandleduration as prospecthandleduration,\n" +
			"               C.phone AS DNI,\n" +
			"               P.callstarttime as callstarttime,\n" +
			"               cast(date(P.callstarttime) as date) AS callstartdate,\n" +
			"               concat(LPAD((string(integer(P.prospecthandleduration / 60))),2,'0'),':',LPAD(string(P.prospecthandleduration % 60),2,'0')) AS prospecthandledurationformatted,\n" +
			"               concat(LPAD(string(integer(nvl(P.telcoduration,0) / 60)),2,'0'),':',LPAD(string(nvl (P.telcoduration,0) % 60),2,'0')) AS voicedurationformatted,\n" +
			"               P.telcoduration AS voiceduration,\n" +
			"               P.callduration as callduration,\n" +
			"               P.status as status,\n" +
			"               P.dispositionstatus as dispositionstatus,\n" +
			"               P.substatus as substatus,\n" +
			"               P.recordingurl as recordingurl ,\n" +
			"               D.status_seq as status_seq,\n" +
			"               P.twiliocallsid as twiliocallsid,\n" +
			"               P.deliveredassetid as deliveredassetid,\n" +
			"               P.callbackdate as callbackdate,\n" +
			"               P.outboundnumber as outboundnumber,\n" +
			"               P.latestprospectidentitychangelogid as latestprospectidentitychangelogid,\n" +
			"               P.callretrycount as callretrycount\n" +
			"       \n" +
			"         FROM [vantage-167009:Xtaas.PC_PCI] P\n" +
			"              INNER JOIN [vantage-167009:Xtaas.pci_prospect] C ON P._id = C._id\n" +
			"              INNER JOIN [vantage-167009:Xtaas.master_status] D ON P.status = D.code\n" +
			"        WHERE\n" +
			"              C.isdeleted = false  and C.isdirty = false\n" +
			"     ) B ON A.campaignid = B.campaignid\n" +
			"           AND A.prospectcallid = B.prospectcallid\n" +
			"           AND A.prospectinteractionsessionid = B.prospectinteractionsessionid\n" +
			"           AND A.status_seq = B.status_seq\n" +
			"     )\n" +
			"     where interactionrownum = 1\n" +
			"\n" +
			"    )A\n" +
			"    INNER JOIN\n" +
			"      [vantage-167009:Xtaas.CMPGN] B ON A.campaignid = B._id\n" +
			"      INNER JOIN\n" +
			"    (\n" +
			"   SELECT B.campaignid as campaignid, B.prospectcallid as prospectcallid, cast(date(A.createddate) as date) AS batch_date\n" +
			"   FROM [vantage-167009:Xtaas.prospectcalllog] A  INNER JOIN [vantage-167009:Xtaas.prospectcall] B ON A._id = B._id\n" +
			"   WHERE  A.isdeleted = false and B.isdeleted = false and A.isdirty = false and B.isdirty = false\n" +
			"   )C ON A.campaignid = C.campaignid AND A.prospectcallid = C.prospectcallid\n" +
			"   where B.isdeleted = false and B.isdirty = false\n" +
			"   ) A\n" +
			"   ,\n" +
			"   (\n" +
			"SELECT     1  as rownum,\n" +
			"           A.campaignid as campaignid,\n" +
			"           A.createdby as agentid,\n" +
			"           A.prospectcallid as prospectcallid,\n" +
			"           cast(null as string) as prospectinteractionsessionid,\n" +
			"           A.phonenumber as DNI,\n" +
			"           null as callstarttime,\n" +
			"           null as callstartdate,\n" +
			"           cast(null as string) as prospecthandledurationformatted,\n" +
			"           cast(null as string) as voicedurationformatted,\n" +
			"           0 as prospecthandleduration,\n" +
			"           0 as voiceduration,\n" +
			"           0 as callduration,\n" +
			"           cast(null as string) as status,\n" +
			"           cast(null as string) as dispositionstatus,\n" +
			"           'DNCL' as substatus,\n" +
			"           A.recordingurl as recordingurl,\n" +
			"           A.createddate as createddate,\n" +
			"           A.updateddate as updateddate,\n" +
			"           cast(null as integer) as status_seq,\n" +
			"           cast(null as string) as twiliocallsid,\n" +
			"           cast(null as string) as deliveredassetid,\n" +
			"           null as callbackdate,\n" +
			"           cast(null as string) as outboundnumber,\n" +
			"           null as latestprospectidentitychangelogid,\n" +
			"           0 as callretrycount,\n" +
			"           B.name as campaign,\n" +
			"           null as batch_date,\n" +
			"           A.note as dnc_note,\n" +
			"           A.trigger as dnc_trigger\n" +
			"     from [vantage-167009:Xtaas.dnclist] A\n" +
			"     INNER JOIN\n" +
			"     [vantage-167009:Xtaas.CMPGN] B ON A.campaignid = B._id\n" +
			"     where trigger = 'DIRECT'\n" +
			"            and A.isdeleted = false and B.isdeleted = false\n" +
			"            and A.isdirty = false and B.isdirty = false\n" +
			")\n" +
			") A\n" +
			"LEFT OUTER JOIN\n" +
			"(select --A.*,\n" +
			"        A.qa_seq_num as qa_seq_num,\n" +
			"        A.campaignName as campaignName,\n" +
			"    A.AgentName As AgentName,\n" +
			"    A.QAName as QAName,\n" +
			"    A._id as _id,\n" +
			"                A.campaignid as campaignid,\n" +
			"                A.agentid as agentid,\n" +
			"                A.qaid as qaid,\n" +
			"                A.prospectcallid as prospectcallid,\n" +
			"                A.prospectinteractionsessionid as prospectinteractionsessionid,\n" +
			"                A.calldate as calldate,\n" +
			"                A.callstarttime as callstarttime,\n" +
			"                A.status as status,\n" +
			"                A.dispositionstatus as dispositionstatus,\n" +
			"                A.substatus as substatus,\n" +
			"                A.overallscore as overallscore,\n" +
			"                A.feedbackdate as feedbackdate,\n" +
			"                A.feedbacktime as feedbacktime,\n" +
			"                A.lead_validation_notes as lead_validation_notes,\n" +
			"                A.PHONEETIQUETTE_CUSTOMER_ENGAGEMENT as PHONEETIQUETTE_CUSTOMER_ENGAGEMENT,\n" +
			"                A.PHONEETIQUETTE_PROFESSIONALISM as PHONEETIQUETTE_PROFESSIONALISM,\n" +
			"                A.SALESMANSHIP_REBUTTAL_USE as SALESMANSHIP_REBUTTAL_USE,\n" +
			"                A.SALESMANSHIP_PROVIDE_INFORMATION as SALESMANSHIP_PROVIDE_INFORMATION,\n" +
			"                A.INTRODUCTION_BRANDING_PERSONAL_CORPORATE as INTRODUCTION_BRANDING_PERSONAL_CORPORATE,\n" +
			"                A.INTRODUCTION_MARKETING_EFFORTS as INTRODUCTION_MARKETING_EFFORTS,\n" +
			"                A.CUSTOMERSATISFACTION_OVERALL_SERVICE as CUSTOMERSATISFACTION_OVERALL_SERVICE,\n" +
			"                A.CLIENT_FULL_DETAILS as CLIENT_FULL_DETAILS,\n" +
			"                A.CLIENT_PII as CLIENT_PII,\n" +
			"                A.CALLCLOSING_BRANDING_PERSONAL_CORPORATE as CALLCLOSING_BRANDING_PERSONAL_CORPORATE,\n" +
			"                A.PHONEETIQUETTE_CALL_PACING as PHONEETIQUETTE_CALL_PACING,\n" +
			"                A.PHONEETIQUETTE_CALL_HOLD_PURPOSE as PHONEETIQUETTE_CALL_HOLD_PURPOSE,\n" +
			"                A.SALESMANSHIP_PREQUALIFICATION_QUESTIONS as SALESMANSHIP_PREQUALIFICATION_QUESTIONS,\n" +
			"                A.INTRODUCTION_PREPARE_READY as INTRODUCTION_PREPARE_READY,\n" +
			"                A.INTRODUCTION_CALL_RECORD as INTRODUCTION_CALL_RECORD,\n" +
			"                A.CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL as CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL,\n" +
			"                A.CLIENT_POSTQUALIFICATION_QUESTIONS as CLIENT_POSTQUALIFICATION_QUESTIONS,\n" +
			"                A.CLIENT_OBTAIN_CUSTOMER_CONSENT as CLIENT_OBTAIN_CUSTOMER_CONSENT,\n" +
			"                A.CALLCLOSING_EXPECTATION_SETTING as CALLCLOSING_EXPECTATION_SETTING,\n" +
			"                A.CALLCLOSING_REDIAL_NUMBER as CALLCLOSING_REDIAL_NUMBER,\n" +
			"                A.LEAD_VALIDATION_VALID as LEAD_VALIDATION_VALID,\n" +
			"              \n" +
			"              A.clintOfferAndsend as clintOfferAndsend,\n" +
			"              A.Salesmanship as Salesmanship,\n" +
			"              A.CallClosing as CallClosing,\n" +
			"              A.Introduction as Introduction,\n" +
			"              A.PhoneEtiquette as PhoneEtiquette,\n" +
			"              A.LeadValidation as LeadValidation\n" +
			"        \n" +
			"  from\n" +
			"  (\n" +
			"    select row_number() over(partition by A.campaignid, A.prospectcallid, A.prospectinteractionsessionid order by A.feedbacktime desc) qa_seq_num, \n" +
			"    --A.*\n" +
			"    A.campaignName as campaignName,\n" +
			"    A.AgentName As AgentName,\n" +
			"    A.QAName as QAName,\n" +
			"    A._id as _id,\n" +
			"                A.campaignid as campaignid,\n" +
			"                A.agentid as agentid,\n" +
			"                A.qaid as qaid,\n" +
			"                A.prospectcallid as prospectcallid,\n" +
			"                A.prospectinteractionsessionid as prospectinteractionsessionid,\n" +
			"                A.calldate as calldate,\n" +
			"                A.callstarttime as callstarttime,\n" +
			"                A.status as status,\n" +
			"                A.dispositionstatus as dispositionstatus,\n" +
			"                A.substatus as substatus,\n" +
			"                A.overallscore as overallscore,\n" +
			"                A.feedbackdate as feedbackdate,\n" +
			"                A.feedbacktime as feedbacktime,\n" +
			"                A.lead_validation_notes as lead_validation_notes,\n" +
			"                A.PHONEETIQUETTE_CUSTOMER_ENGAGEMENT as PHONEETIQUETTE_CUSTOMER_ENGAGEMENT,\n" +
			"                A.PHONEETIQUETTE_PROFESSIONALISM as PHONEETIQUETTE_PROFESSIONALISM,\n" +
			"                A.SALESMANSHIP_REBUTTAL_USE as SALESMANSHIP_REBUTTAL_USE,\n" +
			"                A.SALESMANSHIP_PROVIDE_INFORMATION as SALESMANSHIP_PROVIDE_INFORMATION,\n" +
			"                A.INTRODUCTION_BRANDING_PERSONAL_CORPORATE as INTRODUCTION_BRANDING_PERSONAL_CORPORATE,\n" +
			"                A.INTRODUCTION_MARKETING_EFFORTS as INTRODUCTION_MARKETING_EFFORTS,\n" +
			"                A.CUSTOMERSATISFACTION_OVERALL_SERVICE as CUSTOMERSATISFACTION_OVERALL_SERVICE,\n" +
			"                A.CLIENT_FULL_DETAILS as CLIENT_FULL_DETAILS,\n" +
			"                A.CLIENT_PII as CLIENT_PII,\n" +
			"                A.CALLCLOSING_BRANDING_PERSONAL_CORPORATE as CALLCLOSING_BRANDING_PERSONAL_CORPORATE,\n" +
			"                A.PHONEETIQUETTE_CALL_PACING as PHONEETIQUETTE_CALL_PACING,\n" +
			"                A.PHONEETIQUETTE_CALL_HOLD_PURPOSE as PHONEETIQUETTE_CALL_HOLD_PURPOSE,\n" +
			"                A.SALESMANSHIP_PREQUALIFICATION_QUESTIONS as SALESMANSHIP_PREQUALIFICATION_QUESTIONS,\n" +
			"                A.INTRODUCTION_PREPARE_READY as INTRODUCTION_PREPARE_READY,\n" +
			"                A.INTRODUCTION_CALL_RECORD as INTRODUCTION_CALL_RECORD,\n" +
			"                A.CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL as CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL,\n" +
			"                A.CLIENT_POSTQUALIFICATION_QUESTIONS as CLIENT_POSTQUALIFICATION_QUESTIONS,\n" +
			"                A.CLIENT_OBTAIN_CUSTOMER_CONSENT as CLIENT_OBTAIN_CUSTOMER_CONSENT,\n" +
			"                A.CALLCLOSING_EXPECTATION_SETTING as CALLCLOSING_EXPECTATION_SETTING,\n" +
			"                A.CALLCLOSING_REDIAL_NUMBER as CALLCLOSING_REDIAL_NUMBER,\n" +
			"                A.LEAD_VALIDATION_VALID as LEAD_VALIDATION_VALID,\n" +
			"              \n" +
			"              A.clintOfferAndsend as clintOfferAndsend,\n" +
			"              A.Salesmanship as Salesmanship,\n" +
			"              A.CallClosing as CallClosing,\n" +
			"              A.Introduction as Introduction,\n" +
			"              A.PhoneEtiquette as PhoneEtiquette,\n" +
			"              A.LeadValidation as LeadValidation\n" +
			"    \n" +
			"    \n" +
			"    \n" +
			"    from\n" +
			"      (\n" +
			"        SELECT C.NAME AS CampaignName,\n" +
			"              concat(D.firstname,' ',D.lastname) AS AgentName,\n" +
			"              concat(E.firstname,' ',E.lastname) AS QAName,\n" +
			"              --A.*,\n" +
			"              A._id as _id,\n" +
			"                A.campaignid as campaignid,\n" +
			"                A.agentid as agentid,\n" +
			"                A.qaid as qaid,\n" +
			"                A.prospectcallid as prospectcallid,\n" +
			"                A.prospectinteractionsessionid as prospectinteractionsessionid,\n" +
			"                A.calldate as calldate,\n" +
			"                A.callstarttime as callstarttime,\n" +
			"                A.status as status,\n" +
			"                A.dispositionstatus as dispositionstatus,\n" +
			"                A.substatus as substatus,\n" +
			"                A.overallscore as overallscore,\n" +
			"                A.feedbackdate as feedbackdate,\n" +
			"                A.feedbacktime as feedbacktime,\n" +
			"                A.lead_validation_notes as lead_validation_notes,\n" +
			"                A.PHONEETIQUETTE_CUSTOMER_ENGAGEMENT as PHONEETIQUETTE_CUSTOMER_ENGAGEMENT,\n" +
			"                A.PHONEETIQUETTE_PROFESSIONALISM as PHONEETIQUETTE_PROFESSIONALISM,\n" +
			"                A.SALESMANSHIP_REBUTTAL_USE as SALESMANSHIP_REBUTTAL_USE,\n" +
			"                A.SALESMANSHIP_PROVIDE_INFORMATION as SALESMANSHIP_PROVIDE_INFORMATION,\n" +
			"                A.INTRODUCTION_BRANDING_PERSONAL_CORPORATE as INTRODUCTION_BRANDING_PERSONAL_CORPORATE,\n" +
			"                A.INTRODUCTION_MARKETING_EFFORTS as INTRODUCTION_MARKETING_EFFORTS,\n" +
			"                A.CUSTOMERSATISFACTION_OVERALL_SERVICE as CUSTOMERSATISFACTION_OVERALL_SERVICE,\n" +
			"                A.CLIENT_FULL_DETAILS as CLIENT_FULL_DETAILS,\n" +
			"                A.CLIENT_PII as CLIENT_PII,\n" +
			"                A.CALLCLOSING_BRANDING_PERSONAL_CORPORATE as CALLCLOSING_BRANDING_PERSONAL_CORPORATE,\n" +
			"                A.PHONEETIQUETTE_CALL_PACING as PHONEETIQUETTE_CALL_PACING,\n" +
			"                A.PHONEETIQUETTE_CALL_HOLD_PURPOSE as PHONEETIQUETTE_CALL_HOLD_PURPOSE,\n" +
			"                A.SALESMANSHIP_PREQUALIFICATION_QUESTIONS as SALESMANSHIP_PREQUALIFICATION_QUESTIONS,\n" +
			"                A.INTRODUCTION_PREPARE_READY as INTRODUCTION_PREPARE_READY,\n" +
			"                A.INTRODUCTION_CALL_RECORD as INTRODUCTION_CALL_RECORD,\n" +
			"                A.CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL as CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL,\n" +
			"                A.CLIENT_POSTQUALIFICATION_QUESTIONS as CLIENT_POSTQUALIFICATION_QUESTIONS,\n" +
			"                A.CLIENT_OBTAIN_CUSTOMER_CONSENT as CLIENT_OBTAIN_CUSTOMER_CONSENT,\n" +
			"                A.CALLCLOSING_EXPECTATION_SETTING as CALLCLOSING_EXPECTATION_SETTING,\n" +
			"                A.CALLCLOSING_REDIAL_NUMBER as CALLCLOSING_REDIAL_NUMBER,\n" +
			"                A.LEAD_VALIDATION_VALID as LEAD_VALIDATION_VALID,\n" +
			"              B.ClientOfferAndSend as clintOfferAndsend,\n" +
			"              B.Salesmanship as Salesmanship,\n" +
			"              B.CallClosing as CallClosing,\n" +
			"              B.Introduction as Introduction,\n" +
			"              B.PhoneEtiquette as PhoneEtiquette,\n" +
			"              B.LeadValidation as LeadValidation\n" +
			"        FROM\n" +
			"        (\n" +
			"               SELECT _id,\n" +
			"                campaignid,\n" +
			"                agentid,\n" +
			"                qaid,\n" +
			"                prospectcallid,\n" +
			"                prospectinteractionsessionid,\n" +
			"                cast(date(callstarttime) as date) AS calldate,\n" +
			"                callstarttime,\n" +
			"                MAX(STATUS) status,\n" +
			"                dispositionstatus,\n" +
			"                substatus,\n" +
			"                overallscore,\n" +
			"                cast(date(feedbacktime) as date) As feedbackdate,\n" +
			"                feedbacktime,\n" +
			"                MAX(lead_validation_notes) AS lead_validation_notes,\n" +
			"                MAX(PHONEETIQUETTE_CUSTOMER_ENGAGEMENT) AS PHONEETIQUETTE_CUSTOMER_ENGAGEMENT,\n" +
			"                MAX(PHONEETIQUETTE_PROFESSIONALISM) AS PHONEETIQUETTE_PROFESSIONALISM,\n" +
			"                MAX(SALESMANSHIP_REBUTTAL_USE) AS SALESMANSHIP_REBUTTAL_USE,\n" +
			"                MAX(SALESMANSHIP_PROVIDE_INFORMATION) AS SALESMANSHIP_PROVIDE_INFORMATION,\n" +
			"                MAX(INTRODUCTION_BRANDING_PERSONAL_CORPORATE) AS INTRODUCTION_BRANDING_PERSONAL_CORPORATE,\n" +
			"                MAX(INTRODUCTION_MARKETING_EFFORTS) AS INTRODUCTION_MARKETING_EFFORTS,\n" +
			"                MAX(CUSTOMERSATISFACTION_OVERALL_SERVICE) AS CUSTOMERSATISFACTION_OVERALL_SERVICE,\n" +
			"                MAX(CLIENT_FULL_DETAILS) AS CLIENT_FULL_DETAILS,\n" +
			"                MAX(CLIENT_PII) AS CLIENT_PII,\n" +
			"                MAX(CALLCLOSING_BRANDING_PERSONAL_CORPORATE) AS CALLCLOSING_BRANDING_PERSONAL_CORPORATE,\n" +
			"                MAX(PHONEETIQUETTE_CALL_PACING) AS PHONEETIQUETTE_CALL_PACING,\n" +
			"                MAX(PHONEETIQUETTE_CALL_HOLD_PURPOSE) AS PHONEETIQUETTE_CALL_HOLD_PURPOSE,\n" +
			"                MAX(SALESMANSHIP_PREQUALIFICATION_QUESTIONS) AS SALESMANSHIP_PREQUALIFICATION_QUESTIONS,\n" +
			"                MAX(INTRODUCTION_PREPARE_READY) AS INTRODUCTION_PREPARE_READY,\n" +
			"                MAX(INTRODUCTION_CALL_RECORD) AS INTRODUCTION_CALL_RECORD,\n" +
			"                MAX(CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL) AS CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL,\n" +
			"                MAX(CLIENT_POSTQUALIFICATION_QUESTIONS) AS CLIENT_POSTQUALIFICATION_QUESTIONS,\n" +
			"                MAX(CLIENT_OBTAIN_CUSTOMER_CONSENT) AS CLIENT_OBTAIN_CUSTOMER_CONSENT,\n" +
			"                MAX(CALLCLOSING_EXPECTATION_SETTING) AS CALLCLOSING_EXPECTATION_SETTING,\n" +
			"                MAX(CALLCLOSING_REDIAL_NUMBER) AS CALLCLOSING_REDIAL_NUMBER,\n" +
			"                MAX(LEAD_VALIDATION_VALID) AS LEAD_VALIDATION_VALID\n" +
			"        FROM\n" +
			"                (       \n" +
			"                SELECT _id,\n" +
			"                  campaignid,\n" +
			"                  agentid,\n" +
			"                  prospectcallid,\n" +
			"                  prospectinteractionsessionid,\n" +
			"                  callstarttime,\n" +
			"                  STATUS,\n" +
			"                  dispositionstatus,\n" +
			"                  substatus,\n" +
			"                  qaid,\n" +
			"                  overallscore,\n" +
			"                  feedbacktime,\n" +
			"                  sectionname,\n" +
			"                  CASE WHEN attribute = 'PHONEETIQUETTE_CUSTOMER_ENGAGEMENT' THEN feedback END AS  PHONEETIQUETTE_CUSTOMER_ENGAGEMENT,\n" +
			"                  CASE WHEN attribute = 'PHONEETIQUETTE_PROFESSIONALISM' THEN feedback END AS  PHONEETIQUETTE_PROFESSIONALISM,\n" +
			"                  CASE WHEN attribute = 'SALESMANSHIP_REBUTTAL_USE' THEN feedback END AS  SALESMANSHIP_REBUTTAL_USE,\n" +
			"                  CASE WHEN attribute = 'SALESMANSHIP_PROVIDE_INFORMATION' THEN feedback END AS  SALESMANSHIP_PROVIDE_INFORMATION,\n" +
			"                  CASE WHEN attribute = 'INTRODUCTION_BRANDING_PERSONAL_CORPORATE' THEN feedback END AS  INTRODUCTION_BRANDING_PERSONAL_CORPORATE,\n" +
			"                  CASE WHEN attribute = 'INTRODUCTION_MARKETING_EFFORTS' THEN feedback END AS  INTRODUCTION_MARKETING_EFFORTS,\n" +
			"                  CASE WHEN attribute = 'CUSTOMERSATISFACTION_OVERALL_SERVICE' THEN feedback END AS  CUSTOMERSATISFACTION_OVERALL_SERVICE,\n" +
			"                  CASE WHEN attribute = 'CLIENT_FULL_DETAILS' THEN feedback END AS  CLIENT_FULL_DETAILS,\n" +
			"                  CASE WHEN attribute = 'CLIENT_PII' THEN feedback END AS  CLIENT_PII,\n" +
			"                  CASE WHEN attribute = 'CALLCLOSING_BRANDING_PERSONAL_CORPORATE' THEN feedback END AS  CALLCLOSING_BRANDING_PERSONAL_CORPORATE,\n" +
			"                  CASE WHEN attribute = 'PHONEETIQUETTE_CALL_PACING' THEN feedback END AS  PHONEETIQUETTE_CALL_PACING,\n" +
			"                  CASE WHEN attribute = 'PHONEETIQUETTE_CALL_HOLD_PURPOSE' THEN feedback END AS  PHONEETIQUETTE_CALL_HOLD_PURPOSE,\n" +
			"                  CASE WHEN attribute = 'SALESMANSHIP_PRE-QUALIFICATION_QUESTIONS' THEN feedback END AS  SALESMANSHIP_PREQUALIFICATION_QUESTIONS,\n" +
			"                  CASE WHEN attribute = 'INTRODUCTION_PREPARE_READY' THEN feedback END AS INTRODUCTION_PREPARE_READY,\n" +
			"                  CASE WHEN attribute = 'INTRODUCTION_CALL_RECORD' THEN feedback END AS INTRODUCTION_CALL_RECORD,\n" +
			"                  CASE WHEN attribute = 'CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL' THEN feedback END AS CUSTOMERSATISFACTION_REPRESENTATIVE_ON_CALL,\n" +
			"                  CASE WHEN attribute = 'CLIENT_POST-QUALIFICATION QUESTIONS' THEN feedback END AS CLIENT_POSTQUALIFICATION_QUESTIONS,\n" +
			"                  CASE WHEN attribute = 'CLIENT_OBTAIN_CUSTOMER_CONSENT' THEN feedback END AS CLIENT_OBTAIN_CUSTOMER_CONSENT,\n" +
			"                  CASE WHEN attribute = 'CALLCLOSING_EXPECTATION_SETTING' THEN feedback END AS CALLCLOSING_EXPECTATION_SETTING,\n" +
			"                  CASE WHEN attribute = 'CALLCLOSING_REDIAL_NUMBER' THEN feedback END AS CALLCLOSING_REDIAL_NUMBER,\n" +
			"                  CASE WHEN attribute = 'LEAD_VALIDATION_VALID' THEN feedback END AS LEAD_VALIDATION_VALID,\n" +
			"                  CASE WHEN attribute = 'LEAD_VALIDATION_VALID' THEN attributecomment else null END AS LEAD_VALIDATION_NOTES\n" +
			"                        FROM\n" +
			"                        (\n" +
			"                       SELECT A._id as _id,\n" +
			"                    P.campaignid as campaignid,\n" +
			"                    P.agentid as agentid,\n" +
			"                    P.prospectcallid as prospectcallid,\n" +
			"                    P.prospectinteractionsessionid as prospectinteractionsessionid,\n" +
			"                    P.callstarttime as callstarttime,\n" +
			"                    P.STATUS as status,\n" +
			"                    P.dispositionstatus as dispositionstatus,\n" +
			"                    P.substatus as substatus,\n" +
			"                    F.qaid as qaid,\n" +
			"                    cast(F.overallscore as float) as overallscore,\n" +
			"                    F.qacomments as qacomments,\n" +
			"                    F.feedbacktime as feedbacktime,\n" +
			"                    A.sectionname as sectionname,\n" +
			"                    E.attribute as attribute,\n" +
			"                    B.attributecomment as attributecomment,\n" +
			"                    E.label as label,\n" +
			"                    B.feedback as feedback\n" +
			"                    FROM [vantage-167009:Xtaas.pci_feedbackResponseList] A\n" +
			"                 INNER JOIN [vantage-167009:Xtaas.pci_responseAttributes] B ON A._id = B._id AND A.INDEX = B.index\n" +
			"                 INNER JOIN [vantage-167009:Xtaas.PC_PCI] P ON B._id = P._id\n" +
			"                 INNER JOIN [vantage-167009:Xtaas.qafeedbackformattributes] E ON B.attribute = E.attribute\n" +
			"                 INNER JOIN [vantage-167009:Xtaas.pci_qafeedback] F ON A._id = F._id\n" +
			"                 INNER JOIN [vantage-167009:Xtaas.CMPGN] G on P.campaignid = G._id\n" +
			"                                WHERE F.qaid IS NOT NULL\n" +
			"                 AND   A.isdeleted = false AND A.isdirty = false\n" +
			"                 AND   B.isdeleted = false AND B.isdirty = false\n" +
			"                 AND   E.isdeleted = false AND E.isdirty = false\n" +
			"                 AND   F.isdeleted = false AND F.isdirty = false\n" +
			"                 AND   G.isdeleted = false AND G.isdirty = false\n" +
			"                        )\n" +
			"    \n" +
			"                        )\n" +
			"               \n" +
			"        GROUP BY _id, campaignid, agentid, prospectcallid, prospectinteractionsessionid,callstarttime ,calldate,dispositionstatus,substatus,qaid,overallscore,feedbacktime,feedbackdate\n" +
			"        \n" +
			"        ) A\n" +
			"        INNER JOIN\n" +
			"        (\n" +
			"           SELECT _id,\n" +
			"              campaignid,\n" +
			"              agentid,\n" +
			"              MAX(ClientOfferAndSend) ClientOfferAndSend,\n" +
			"              MAX(Salesmanship) Salesmanship,\n" +
			"              MAX(CallClosing) CallClosing,\n" +
			"              MAX(Introduction) Introduction,\n" +
			"              MAX(PhoneEtiquette) PhoneEtiquette,\n" +
			"              MAX(LeadValidation) LeadValidation\n" +
			"                FROM\n" +
			"                (\n" +
			"                       \n" +
			"         SELECT _id,\n" +
			"                campaignid,\n" +
			"                agentid,\n" +
			"                CASE WHEN sectionname = 'Client Offer & Send' THEN avg_percent ELSE 0 END AS ClientOfferAndSend,\n" +
			"                CASE WHEN sectionname = 'Salesmanship' THEN avg_percent ELSE 0 END AS Salesmanship,\n" +
			"                CASE WHEN sectionname = 'Call Closing' THEN avg_percent ELSE 0 END AS CallClosing,\n" +
			"                CASE WHEN sectionname = 'Introduction' THEN avg_percent ELSE 0 END AS Introduction,\n" +
			"                CASE WHEN sectionname = 'Phone Etiquette' THEN avg_percent ELSE 0 END AS PhoneEtiquette,\n" +
			"                CASE WHEN sectionname = 'Lead Validation' THEN avg_percent ELSE 0 END AS LeadValidation\n" +
			"                        FROM\n" +
			"                        (\n" +
			"                                 SELECT _id,\n" +
			"                                        campaignid,\n" +
			"                                        agentid,\n" +
			"                                        sectionname,\n" +
			"                                        CASE WHEN (total - nac) < 1 THEN 1.00 ELSE yesc*1.00 /(total - nac) END AS avg_percent\n" +
			"                                FROM\n" +
			"                                (SELECT _id,\n" +
			"                                                A.campaignid as campaignid,\n" +
			"                                                A.agentid as agentid,\n" +
			"                                                A.sectionname as sectionname,\n" +
			"                                                yesc,\n" +
			"                                                noc,\n" +
			"                                                nac,\n" +
			"                                                (yesc + noc + nac) AS total\n" +
			"                                        FROM\n" +
			"                                        (\n" +
			"                                               \n" +
			"                                          SELECT        _id,\n" +
			"                                                        campaignid,\n" +
			"                                                        agentid,\n" +
			"                                                        sectionname,\n" +
			"                                                        SUM(yes_count) yesc,\n" +
			"                                                        SUM(no_count) noc,\n" +
			"                                                        SUM(na_count) nac\n" +
			"                                                FROM\n" +
			"                                                (\n" +
			"                                                        SELECT _id,\n" +
			"                                                                campaignid,\n" +
			"                                                                agentid,\n" +
			"                                                                sectionname,\n" +
			"                                                                CASE WHEN feedback='YES' THEN count ELSE 0 END AS yes_count,\n" +
			"                                                                CASE WHEN feedback='NO' THEN count ELSE 0 END AS no_count,\n" +
			"                                                                CASE WHEN feedback='NA' THEN count ELSE 0 END AS na_count\n" +
			"                                                        FROM\n" +
			"                                                        (SELECT A._id as _id,\n" +
			"                                                                        C.campaignid as campaignid,\n" +
			"                                                                        C.agentid as agentid,\n" +
			"                                                                        A.sectionname as sectionname,\n" +
			"                                                                        B.feedback as feedback,\n" +
			"                                                                        cast(COUNT(B.feedback) as integer) AS COUNT\n" +
			"                                                                FROM [vantage-167009:Xtaas.pci_feedbackResponseList] A\n" +
			"                                                                        INNER JOIN [vantage-167009:Xtaas.pci_responseAttributes] B ON A._id = B._id AND A.INDEX = B.index\n" +
			"                                                                        INNER JOIN [vantage-167009:Xtaas.pci_prospectcall] C ON B._id=C._id\n" +
			"                                                                        INNER JOIN [vantage-167009:Xtaas.CMPGN] D on C.campaignid = D._id\n" +
			"                                                                WHERE sectionname NOT LIKE 'Customer Satisfaction'\n" +
			"                          AND   A.isdeleted = false AND A.isdirty = false\n" +
			"                          AND   B.isdeleted = false AND B.isdirty = false\n" +
			"                          AND   C.isdeleted = false AND C.isdirty = false\n" +
			"                          AND   D.isdeleted = false AND D.isdirty = false\n" +
			"                    GROUP BY _id,\n" +
			"                          campaignid,\n" +
			"                          agentid,\n" +
			"                          sectionname,\n" +
			"                          feedback\n" +
			"                    ORDER BY _id,\n" +
			"                          campaignid,\n" +
			"                          agentid,\n" +
			"                          sectionname\n" +
			"                                                        )\n" +
			"                                                )\n" +
			"                GROUP BY _id, campaignid, agentid, sectionname\n" +
			"                                                ORDER BY _id, campaignid, agentid, sectionname\n" +
			"                                        ) A\n" +
			"                                        ORDER BY _id, campaignid, agentid, sectionname\n" +
			"                                )\n" +
			"                        )\n" +
			"                )\n" +
			"                GROUP BY _id, campaignid, agentid\n" +
			"       ) B ON A._id = B._id\n" +
			"        INNER JOIN [vantage-167009:Xtaas.CMPGN] C ON C._id = A.campaignid\n" +
			"        INNER JOIN [vantage-167009:Xtaas.agent] D ON D._id = A.agentid\n" +
			"        INNER JOIN [vantage-167009:Xtaas.qa] E ON E._id = A.qaid\n" +
			"        WHERE C.isdeleted = FALSE AND C.isdirty = false\n" +
			"                AND   D.isdeleted = FALSE AND D.isdirty = false\n" +
			"                AND   E.isdeleted = FALSE AND E.isdirty = false\n" +
			" \n" +
			"    ) A\n" +
			"\n" +
			"  ) A\n" +
			"  where qa_seq_num = 1\n" +
			"\n" +
			"\n" +
			") C ON A.campaignid = C.campaignid AND A.prospectcallid = C.prospectcallid\n" +
			"        AND A.prospectinteractionsessionid = C.prospectinteractionsessionid\n" +
			" \n" +
			"LEFT OUTER JOIN\n" +
			"(\n" +
			"        SELECT campaignid,\n" +
			"                prospectcallid,\n" +
			"                prospectinteractionsessionid,\n" +
			"                dni,\n" +
			"                callstarttime,\n" +
			"                callstartdate,\n" +
			"                status,\n" +
			"                dispositionstatus,\n" +
			"                    Industrylist,\n" +
			"                MAX(ltrim(rtrim(Revenue,'\"'),'\"')) Revenue,\n" +
			"                MAX(ltrim(rtrim(EventTimeline,'\"'),'\"')) EventTimeline,\n" +
			"                MAX(ltrim(rtrim(Industry,'\"'),'\"')) Industry,\n" +
			"                MAX(ltrim(rtrim(Budget,'\"'),'\"')) Budget,\n" +
			"                MAX(ltrim(rtrim(CurrentlyEvaluatingCallAnalytics,'\"'),'\"')) CurrentlyEvaluatingCallAnalytics,\n" +
			"                MAX(ltrim(rtrim(Department,'\"'),'\"')) Department,\n" +
			"                MAX(ltrim(rtrim(Title,'\"'),'\"')) Title,\n" +
			"                MAX(ltrim(rtrim(NumberOfEmployees,'\"'),'\"')) NumberOfEmployees,\n" +
			"                MAX(ltrim(rtrim(Country,'\"'),'\"')) Country\n" +
			"        FROM\n" +
			"        (SELECT B.campaignid as campaignid,\n" +
			"                        B.prospectcallid as prospectcallid,\n" +
			"                        B.prospectinteractionsessionid as prospectinteractionsessionid,\n" +
			"                        C.phone AS DNI,\n" +
			"                        B.callstarttime as callstarttime,\n" +
			"                        cast(date(B.callstarttime) as date) AS callstartdate,\n" +
			"                        A.status as status,\n" +
			"                        B.dispositionstatus as dispositionstatus,\n" +
			"                              C.Industrylist as industrylist,\n" +
			"                        CASE WHEN D.answerskey IN ('\"AnnualRevenue\"','\"What is the Annual Revenue of your Company?\"','AnnualRevenue') THEN D.answersvalue END AS Revenue,\n" +
			"                        CASE WHEN D.answerskey IN ('\"EventTimeline\"','\"What is your timeline?\"','\"When is your event?\"','EventTimeline') THEN D.answersvalue END AS EventTimeline,\n" +
			"                        CASE WHEN D.answerskey IN ('\"Industry\"','\"Which Industry you belong to?\"','Which_Industry_you_belong_to_','Industry') THEN D.answersvalue END AS Industry,\n" +
			"                        CASE WHEN D.answerskey IN ('\"Budget\"','\"What is your budget?\"','Budget') THEN D.answersvalue END AS Budget,\n" +
			"                        CASE WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'CurrentlyEvaluatingCallAnalytics' THEN D.answersvalue END AS CurrentlyEvaluatingCallAnalytics,\n" +
			"                        CASE WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'Department' THEN D.answersvalue\n" +
			"                             WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'MarketingDeptYesNo' and lower(ltrim(rtrim(D.answersvalue,'\"'),'\"')) = 'yes' THEN 'Marketing'\n" +
			"                             WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'MarketingDeptYesNo' and lower(ltrim(rtrim(D.answersvalue,'\"'),'\"')) = 'no' THEN 'Other'\n" +
			"                             END AS Department,\n" +
			"                        CASE WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'Title' THEN D.answersvalue END AS Title,\n" +
			"                        CASE WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'NumberOfEmployeesLessThan50' and lower(ltrim(rtrim(D.answersvalue,'\"'),'\"')) = 'yes' THEN 'Less than 50'\n" +
			"                             WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'NumberOfEmployeesLessThan50' and lower(ltrim(rtrim(D.answersvalue,'\"'),'\"')) = 'no' THEN 'More than 50'\n" +
			"                             WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'NumberOfEmployees' THEN D.answersvalue END AS NumberOfEmployees,\n" +
			"                        CASE WHEN ltrim(rtrim(D.answerskey,'\"'),'\"') = 'IsCountryUS' THEN 'USA' ELSE 'Other' END AS Country\n" +
			"                FROM [vantage-167009:Xtaas.prospectcalllog] A\n" +
			"                        INNER JOIN [vantage-167009:Xtaas.prospectcall] B ON A._id = B._id\n" +
			"                        INNER JOIN [vantage-167009:Xtaas.prospect] C ON B._id = C._id\n" +
			"                        INNER JOIN [vantage-167009:Xtaas.answers] D ON C._id = D._id\n" +
			"                        INNER JOIN [vantage-167009:Xtaas.CMPGN] E on B.campaignid = E._id\n" +
			"                WHERE\n" +
			"                      A.isdeleted = FALSE AND A.isdirty = false\n" +
			"                AND   B.isdeleted = FALSE AND B.isdirty = false\n" +
			"                AND   C.isdeleted = FALSE AND C.isdirty = false\n" +
			"                AND   D.isdeleted = FALSE AND D.isdirty = false\n" +
			"                AND   E.isdeleted = FALSE AND E.isdirty = false\n" +
			"        )\n" +
			"        GROUP BY campaignid,prospectcallid,prospectinteractionsessionid,dni,callstarttime,callstartdate,status,Industrylist,dispositionstatus\n" +
			") D ON A.campaignid = D.campaignid AND A.prospectcallid = D.prospectcallid\n" +
			"        AND A.prospectinteractionsessionid = D.prospectinteractionsessionid AND A.dispositionstatus = D.dispositionstatus\n" +
			"\n" +
			"\n" +
			"LEFT OUTER JOIN\n" +
			"  [vantage-167009:Xtaas.master_status] E on A.status = E.code\n" +
			"LEFT OUTER JOIN\n" +
			"  [vantage-167009:Xtaas.master_dispositionstatus] F on A.dispositionstatus = F.code\n" +
			"LEFT OUTER JOIN\n" +
			"  [vantage-167009:Xtaas.master_substatus] G on A.substatus = G.code\n" +
			")";
	
	private static class ExtractAndSend extends DoFn<TableRow, TableRow>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			context.output(context.element());
		}
	}
	
	public static void main(String[] args) {
		
		DataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		pipelineOptions.setTempLocation("gs://learning001/temp");
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
		fieldSchemaList.add(new TableFieldSchema().setName("qastatus").setType("FLOAT"));
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
		
		pipeline.apply(BigQueryIO.Read.named("Reader").fromQuery(query))
				.apply(ParDo.of(new ExtractAndSend()))
				.apply(BigQueryIO.Write.named("Writer").to("zimetrics:Learning.Query_Output")
				.withSchema(schema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		pipeline.run();
		
	}
	
}
