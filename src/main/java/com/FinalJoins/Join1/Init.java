package com.FinalJoins.Join1;

import com.Essential.JobOptions;
import com.Essential.Joins;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.io.Serializable;

public class Init implements Serializable{

	Joins joins = new Joins();
	
	PCollection<TableRow> PC_PCI;
	PCollection<TableRow> master_status;
	PCollection<TableRow> pci_prospect;
	PCollection<TableRow> prospectCallLog;
	PCollection<TableRow> prospectCall;
	PCollection<TableRow> prospect;
	PCollection<TableRow> dncList;
	PCollection<TableRow> CMPGN;
	PCollection<TableRow> pci_feedbackResponseList;
	PCollection<TableRow> pci_responseAttributes;
	PCollection<TableRow> qaFeedbackFormAttributes;
	PCollection<TableRow> pci_qaFeedback;
	PCollection<TableRow> pci_prospectCall;
	PCollection<TableRow> answers;
	PCollection<TableRow> agent;
	PCollection<TableRow> qa;
	PCollection<TableRow> master_dispositionstatus;
	PCollection<TableRow> master_substatus;
	
	PCollection<TableRow> joinOfPC_PCIAndMaster_Status;
	PCollection<TableRow> joinOfPCIFeedbackResponseListAndPciResponseAttributes;
	PCollection<TableRow> joinOfProspectCallLogAndProspectCall;
	
	
	void initTables(Pipeline pipeline){
		
		JobOptions jobOptions = (JobOptions) pipeline.getOptions();
		
		String PC_PCI = "SELECT * FROM [vantage-167009:Xtaas.PC_PCI]\n" +
				"\tWHERE  updateddate > '"+ jobOptions.getStartTime()
				+"' AND updateddate < '"+ jobOptions.getEndTime() +"'";
		
		String masterstatus = "vantage-167009:Xtaas.master_status";
		String pci_prospect = "vantage-167009:Xtaas.pci_prospect";
		
		String prospectCallLog = "SELECT a._id _id, a._class _class, a.status status, b.createddate createddate, a.createdby createdby, \n" +
				"a.updateddate updateddate, a.updatedby updatedby, a.version version, a.isdeleted isdeleted , a.callbackdate callbackdate, \n" +
				"a.isdirty isdirty\n" +
				"   FROM ( SELECT row_number()\n" +
				"          OVER( \n" +
				"          PARTITION BY prospectcallid\n" +
				"          ORDER BY updateddate ) AS rnum, _id, _class, prospectcallid, status, createddate, createdby, updateddate, \n" +
				"          updatedby, version, isdeleted, callbackdate, isdirty\n" +
				"           FROM [vantage-167009:Xtaas.pci_prospectcallinteraction]) a\n" +
				"   JOIN ( SELECT derived_table1.prospectcallid, max(derived_table1.rnum) AS rnum, \n" +
				"   max(derived_table1.updateddate) AS updateddate, min(derived_table1.createddate) AS createddate\n" +
				"           FROM ( SELECT row_number()\n" +
				"                  OVER( \n" +
				"                  PARTITION BY prospectcallid\n" +
				"                  ORDER BY updateddate) AS rnum, _id, _class, prospectcallid, status, createddate, createdby, updateddate, \n" +
				"                  updatedby, version, isdeleted, callbackdate, isdirty\n" +
				"                   FROM [vantage-167009:Xtaas.pci_prospectcallinteraction]) derived_table1\n" +
				"          GROUP BY derived_table1.prospectcallid) b ON a.prospectcallid = b.prospectcallid \n" +
				"          AND a.updateddate = b.updateddate \n" +
				"          AND a.rnum = b.rnum\n" +
				"where a.updateddate > '" + jobOptions.getStartTime() + "'" +
				"and a.updateddate < '" + jobOptions.getEndTime() + "'";
		
		
		String prospectCall = "SELECT a._id _id, a.prospectcallid prospectcallid, a.twiliocallsid twiliocallsid, a.agentid agentid, \n" +
				"a.previousprospectcallid previousprospectcallid, a.deliveredassetid deliveredassetid, \n" +
				"a.dispositionstatus dispositionstatus, a.substatus substatus, a.qastatus qastatus, a.campaignid campaignid, \n" +
				"a.callretrycount callretrycount, a.callstarttime callstarttime, a.callbacktime callbacktime, a.callbackhours callbackhours, \n" +
				"a.callduration callduration, a.outboundnumber outboundnumber, a.recordingurl recordingurl, \n" +
				"a.prospecthandleduration prospecthandleduration, a.telcoduration telcoduration, \n" +
				"a.prospectinteractionsessionid prospectinteractionsessionid, a.isdeleted isdeleted, \n" +
				"a.latestprospectidentitychangelogid latestprospectidentitychangelogid, a.isdirty isdirty\n" +
				"FROM [vantage-167009:Xtaas.pci_prospectcall] a\n" +
				"JOIN \n" +
				"[vantage-167009:Xtaas.prospectcalllog_max] b ON a._id = b._id\n";// +
//				"where b.updateddate > '" + jobOptions.getStartTime() + "'" +
//				"and b.updateddate < '" + jobOptions.getEndTime() + "'";
		
		
		String dncList = "SELECT * FROM [vantage-167009:Xtaas.dnclist]" +
				"WHERE" +
				"  updateddate > '" + jobOptions.getStartTime() + "'" +
				"  AND updateddate < '" + jobOptions.getEndTime() + "'";
		
		String CMPGN = "SELECT _id,_class,organizationid,campaignmanagerid,\n" +
				"status,name,startdate,enddate,type,deliverytarget,\n" +
				"domain,dailycap,qualificationclause,leadfilteringclause,\n" +
				"selfmanageteam,currentassetid,dialermode,createddate,\n" +
				"createdby,updateddate,updatedby,version,isdeleted,isdirty \n" +
				"FROM [vantage-167009:Xtaas.campaign] WHERE status in ('RUNNING','PAUSED','STOPPED','COMPLETED') ";
		
		String pciFeedbackResponseList = "vantage-167009:Xtaas.pci_feedbackResponseList";
		String pciResponseAttributes = "vantage-167009:Xtaas.pci_responseAttributes";
		String qaFeedbackFormAttributes = "vantage-167009:Xtaas.qafeedbackformattributes";
		String pciQaFeedback = "vantage-167009:Xtaas.pci_qafeedback";
		String pciProspectCall = "vantage-167009:Xtaas.pci_prospectcall";
		
		String prospect = "SELECT a._id _id, a.p_prospectcallid p_prospectcallid, a.status status, a.source source, a.sourceid sourceid, \n" +
				"a.campaigncontactid campaigncontactid, a.prefix, a.firstname firstname, a.lastname lastname, a.suffix suffix, \n" +
				"a.company company, a.title title, a.department department, a.phone phone, a.industry industry, a.industrylist industrylist, \n" +
				"a.email email, a.addressline1 addressline1, a.addressline2 addressline2, a.city city, a.zipcode zipcode, a.country country, \n" +
				"a.extension extension, a.statecode statecode, a.optedin optedin, a.callbacktimeinms callbacktimeinms, a.isdeleted isdeleted,\n" +
				"a.isdirty isdirty \n" +
				"   FROM [vantage-167009:Xtaas.pci_prospect] a\n" +
				"   JOIN [vantage-167009:Xtaas.prospectcalllog_max] b ON a._id = b._id\n";// +
//				"where b.updateddate > '" + jobOptions.getStartTime() + "'" +
//				"and b.updateddate < '" + jobOptions.getEndTime() + "'";
		
		String answers = "SELECT a._id _id, a.p_prospectcallid p_prospectcallid, a.index index, a.answerskey answerskey, a.answersvalue answersvalue,\n" +
				"a.isdeleted isdeleted, a.isdirty isdirty\n" +
				"   FROM [vantage-167009:Xtaas.pci_answers] a\n" +
				"   JOIN [vantage-167009:Xtaas.prospectcalllog_max] b ON a._id = b._id\n";// +
//				"where b.updateddate > '" + jobOptions.getStartTime() + "'" +
//				"and b.updateddate < '" + jobOptions.getEndTime() + "'";
		
		String agent = "vantage-167009:Xtaas.agent";
		String qa = "vantage-167009:Xtaas.qa";
		String master_dispositionStatus = "vantage-167009:Xtaas.master_status";
		String master_SubStatus = "vantage-167009:Xtaas.master_substatus";
		
		this.PC_PCI = pipeline.apply(BigQueryIO.Read.named("PC_PCI").fromQuery(PC_PCI));
		this.master_status = pipeline.apply(BigQueryIO.Read.named("master_status").from(masterstatus));
		this.pci_prospect = pipeline.apply(BigQueryIO.Read.named("pci_Prospect").from(pci_prospect));
		this.prospectCallLog = pipeline.apply(BigQueryIO.Read.named("prospectCallLog").fromQuery(prospectCallLog));
		this.prospectCall = pipeline.apply(BigQueryIO.Read.named("prospectCall").fromQuery(prospectCall));
		this.prospect = pipeline.apply(BigQueryIO.Read.named("prospect").fromQuery(prospect));
		this.answers = pipeline.apply(BigQueryIO.Read.named("answers").fromQuery(answers));
		this.dncList = pipeline.apply(BigQueryIO.Read.named("dnclist").fromQuery(dncList));
		this.CMPGN = pipeline.apply(BigQueryIO.Read.named("CMPGN").fromQuery(CMPGN));
		this.pci_feedbackResponseList = pipeline.apply(BigQueryIO.Read.named("pci_feedbackResponseList").from(pciFeedbackResponseList));
		this.pci_responseAttributes = pipeline.apply(BigQueryIO.Read.named("pci_responseAttributes").from(pciResponseAttributes));
		this.qaFeedbackFormAttributes = pipeline.apply(BigQueryIO.Read.named("qaFeedbackFormAttributes").from(qaFeedbackFormAttributes));
		this.pci_qaFeedback = pipeline.apply(BigQueryIO.Read.named("pci_qaFeedback").from(pciQaFeedback));
		this.pci_prospectCall = pipeline.apply(BigQueryIO.Read.named("pci_prospectCall").from(pciProspectCall));
		this.agent = pipeline.apply(BigQueryIO.Read.named("agent").from(agent));
		this.qa = pipeline.apply(BigQueryIO.Read.named("qa").from(qa));
		this.master_dispositionstatus = pipeline.apply(BigQueryIO.Read.named("master_dispositionstatus").from(master_dispositionStatus));
		this.master_substatus = pipeline.apply(BigQueryIO.Read.named("master_substatus").from(master_SubStatus));
		
		this.joinOfPC_PCIAndMaster_Status = performJoinOfPC_PCIAndMaster_Status();
		this.joinOfPCIFeedbackResponseListAndPciResponseAttributes = performJoinOfPCIFeedbackResponseListAndPciResponseAttributes();
		this.joinOfProspectCallLogAndProspectCall = performJoinOfProspectCallLogAndProspectCall();
	
	}
	
	public PCollection<TableRow> getPC_PCI() {
		return PC_PCI;
	}
	
	public PCollection<TableRow> getMaster_status() {
		return master_status;
	}
	
	public PCollection<TableRow> getPci_prospect() {
		return pci_prospect;
	}
	
	public PCollection<TableRow> getProspectCallLog() {
		return prospectCallLog;
	}
	
	public PCollection<TableRow> getProspectCall() {
		return prospectCall;
	}
	
	public PCollection<TableRow> getProspect() {
		return prospect;
	}
	
	public PCollection<TableRow> getDncList() {
		return dncList;
	}
	
	public PCollection<TableRow> getCMPGN() {
		return CMPGN;
	}
	
	public PCollection<TableRow> getPci_feedbackResponseList() {
		return pci_feedbackResponseList;
	}
	
	public PCollection<TableRow> getPci_responseAttributes() {
		return pci_responseAttributes;
	}
	
	public PCollection<TableRow> getQaFeedbackFormAttributes() {
		return qaFeedbackFormAttributes;
	}
	
	public PCollection<TableRow> getPci_qaFeedback() {
		return pci_qaFeedback;
	}
	
	public PCollection<TableRow> getPci_prospectCall() {
		return pci_prospectCall;
	}
	
	public PCollection<TableRow> getAnswers() {
		return answers;
	}
	
	public PCollection<TableRow> getAgent() {
		return agent;
	}
	
	public PCollection<TableRow> getQa() {
		return qa;
	}
	
	public PCollection<TableRow> getMaster_dispositionstatus() {
		return master_dispositionstatus;
	}
	
	public PCollection<TableRow> getMaster_substatus() {
		return master_substatus;
	}
	
	
	void initJoins(){
		this.joinOfPC_PCIAndMaster_Status = performJoinOfPC_PCIAndMaster_Status();
		this.joinOfPCIFeedbackResponseListAndPciResponseAttributes = performJoinOfPCIFeedbackResponseListAndPciResponseAttributes();
		this.joinOfProspectCallLogAndProspectCall = performJoinOfProspectCallLogAndProspectCall();
	}
	
	public PCollection<TableRow> getJoinOfPC_PCIAndMaster_Status() {
		return joinOfPC_PCIAndMaster_Status;
	}
	
	public PCollection<TableRow> getJoinOfPCIFeedbackResponseListAndPciResponseAttributes() {
		return joinOfPCIFeedbackResponseListAndPciResponseAttributes;
	}
	
	public PCollection<TableRow> getJoinOfProspectCallLogAndProspectCall() {
		return joinOfProspectCallLogAndProspectCall;
	}
	
	
	PCollection<TableRow> performJoinOfPC_PCIAndMaster_Status(){
		
		PCollection<KV<String, TableRow>> pcpci = getPC_PCI()
				.apply(ParDo.of(new DoFn<TableRow, KV<String, TableRow>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						TableRow tableRow = context.element();
						String id = (String) tableRow.get("status");
						context.output(KV.of(id, context.element()));
					}
				}));
		
		PCollection<KV<String, TableRow>> master_status = getMaster_status()
				.apply(ParDo.of(new DoFn<TableRow, KV<String, TableRow>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						TableRow tableRow = context.element();
						String id = (String) tableRow.get("code");
						context.output(KV.of(id, context.element()));
					}
				}));
		
		PCollection<TableRow> joinedPCollection = joins.innerJoin1(pcpci, master_status, "A_", "B_");
		
		return joinedPCollection;
	}
	
	PCollection<TableRow> performJoinOfPCIFeedbackResponseListAndPciResponseAttributes(){
		
		PCollection<KV<String, TableRow>> pciFeedbackResponsePCollection = getPci_feedbackResponseList()
				.apply(ParDo.of(new DoFn<TableRow, KV<String, TableRow>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						TableRow tableRow = context.element();
						String id = (String) tableRow.get("_id") + tableRow.get("index");
						context.output(KV.of(id, tableRow));
					}
				}));
		
		PCollection<KV<String, TableRow>> pciResponseAttributesPCollection = getPci_responseAttributes()
				.apply(ParDo.of(new DoFn<TableRow, KV<String, TableRow>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						TableRow tableRow = context.element();
						String id = (String) tableRow.get("_id") + tableRow.get("index");
						context.output(KV.of(id, tableRow));
					}
				}));
		
		PCollection<TableRow> joinedPCollection = joins.innerJoin1(pciFeedbackResponsePCollection, pciResponseAttributesPCollection, "A_", "B_");
		
		return joinedPCollection;
		
	}
	
	PCollection<TableRow> performJoinOfProspectCallLogAndProspectCall(){
		
		PCollection<KV<String, TableRow>> prospectCallLogPCollection = getProspectCallLog()
				.apply(ParDo.of(new DoFn<TableRow, KV<String, TableRow>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						TableRow tableRow = context.element();
						String id = (String) tableRow.get("_id");
						context.output(KV.of(id, tableRow));
					}
				}));
		
		PCollection<KV<String, TableRow>> prospectCallPCollection = getProspectCall()
				.apply(ParDo.of(new DoFn<TableRow, KV<String, TableRow>>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						TableRow tableRow = context.element();
						String id = (String) tableRow.get("_id");
						context.output(KV.of(id, tableRow));
					}
				}));
		
		PCollection<TableRow> joinedPCollection = joins.innerJoin1(prospectCallLogPCollection, prospectCallPCollection, "A_","B_");
		
		return joinedPCollection;
		
	}
}
