package com.Practice.Basic;

public class Queries {
	
	public final String PC_PCI = "(SELECT PC._id _id, PC.prospectcallid prospectcallid, PC.twiliocallsid twiliocallsid, PC.agentid agentid, PC.previousprospectcallid previousprospectcallid, PC.deliveredassetid deliveredassetid, \n" +
			"PC.dispositionstatus dispositionstatus, PC.substatus substatus, PC.qastatus qastatus, PC.campaignid campaignid, PC.callretrycount callretrycount, PC.callstarttime callstarttime, PC.callbacktime callbacktime, \n" +
			"PC.callbackhours callbackhours, PC.callduration callduration, PC.outboundnumber outboundnumber, PC.recordingurl recordingurl, PC.prospecthandleduration prospecthandleduration, PC.telcoduration telcoduration, \n" +
			"PC.latestprospectidentitychangelogid latestprospectidentitychangelogid, PC.prospectinteractionsessionid prospectinteractionsessionid, PC.isdeleted isdeleted, PC.isdirty isdirty , PCI._class _class, \n" +
			"PCI.status status, PCI.callbackdate callbackdate, PCI.createddate createddate, PCI.createdby createdby, PCI.updateddate updateddate, PCI.updatedby updatedby, \n" +
			"PCI.version version, PCI.isdeleted is_deleted, PCI.isdirty is_dirty\n" +
			"FROM \n" +
			"[vantage-167009:Xtaas.pci_prospectcall] PC INNER JOIN [vantage-167009:Xtaas.pci_prospectcallinteraction] PCI\n" +
			"ON PC._id = PCI._id\n" +
			"WHERE\n" +
			"PC.isdeleted = false AND PC.isdirty = false AND\n" +
			"PCI.isdeleted = false AND PCI.isdirty = false )";
	
	
	public final String prospectCall = "SELECT a._id _id, a.prospectcallid prospectcallid, a.twiliocallsid twiliocallsid, a.agentid agentid, a.previousprospectcallid previousprospectcallid, a.deliveredassetid deliveredassetid, a.dispositionstatus dispositionstatus, a.substatus substatus, a.qastatus qastatus, a.campaignid campaignid, a.callretrycount callretrycount, a.callstarttime callstarttime, a.callbacktime callbacktime, a.callbackhours callbackhours, a.callduration callduration, a.outboundnumber outboundnumber, a.recordingurl recordingurl, a.prospecthandleduration prospecthandleduration, a.telcoduration telcoduration, a.prospectinteractionsessionid prospectinteractionsessionid, a.isdeleted isdeleted, a.latestprospectidentitychangelogid latestprospectidentitychangelogid, a.isdirty isdirty\n" +
			"   \n" +
			"FROM [vantage-167009:Xtaas.pci_prospectcall] a\n" +
			"\n" +
			"JOIN \n" +
			"\n" +
			"[vantage-167009:Xtaas.prospectcalllog] b ON a._id = b._id;";
	
	public final String prospectCallLog = "vantage-167009:Xtaas.prospectcalllog_new";
	
	
	public final String CMPGN = "SELECT _id,_class,organizationid,campaignmanagerid,\n" +
			"status,name,startdate,enddate,type,deliverytarget,\n" +
			"domain,dailycap,qualificationclause,leadfilteringclause,\n" +
			"selfmanageteam,currentassetid,dialermode,createddate,\n" +
			"createdby,updateddate,updatedby,version,isdeleted,isdirty \n" +
			"FROM [vantage-167009:Xtaas.campaign] WHERE status in ('RUNNING','PAUSED','STOPPED','COMPLETED') ;";
	
	public final String master_status = "vantage-167009:Xtaas.master_status";
	
	public final String answers = "SELECT a._id _id, a.p_prospectcallid p_prospectcallid, a.index index, a.answerskey answerskey, a.answersvalue answersvalue, a.isdeleted isdeleted, a.isdirty isdirty\n" +
			"   FROM [vantage-167009:Xtaas.pci_answers] a\n" +
			"   JOIN [vantage-167009:Xtaas.prospectcalllog] b ON a._id = b._id;";
	
	public final String prospect = "SELECT a._id _id, a.p_prospectcallid p_prospectcallid, a.status status, a.source source, a.sourceid sourceid, a.campaigncontactid campaigncontactid, a.prefix, a.firstname firstname, a.lastname lastname, a.suffix suffix, a.company company, a.title title, a.department department, a.phone phone, a.industry industry, a.industrylist industrylist, a.email email, a.addressline1 addressline1, a.addressline2 addressline2, a.city city, a.zipcode zipcode, a.country country, a.extension extension, a.statecode statecode, a.optedin optedin, a.callbacktimeinms callbacktimeinms, a.isdeleted isdeleted,a.isdirty isdirty \n" +
			"   FROM [vantage-167009:Xtaas.pci_prospect] a\n" +
			"   JOIN [vantage-167009:Xtaas.prospectcalllog] b ON a._id = b._id;";
	
	public final String dncList = "vantage-167009:Xtaas.dnclist";
	public final String pciFeedbackResponseList = "vantage-167009:Xtaas.pci_feedbackResponseList";
	public final String pciResponseAttributes = "vantage-167009:Xtaas.pci_responseAttributes";
	public final String pciProspectCall = "vantage-167009:Xtaas.pci_prospectcall";
	public final String pciQaFeedback = "vantage-167009:Xtaas.pci_qafeedback";
	public final String qaFeedbackFormAttributes = "vantage-167009:Xtaas.qafeedbackformattributes";
	
	
	public final String source1 = "vantage-167009:Learning.Source1";
	public final String source2 = "vantage-167009:Learning.Source2";
	public final String source3 = "vantage-167009:Learning.Source3";
	public final String source4 = "vantage-167009:Learning.Source4";

	public final String temp_prospectCallLog = "vantage-167009:Learning.Temp_ProspectCallLog";
	public final String temp_prospectCall = "vantage-167009:Learning.Temp_ProspectCall";
	public final String temp_prospect = "vantage-167009:Learning.Temp_Prospect";
	public final String temp_answers = "vantage-167009:Learning.Temp_Answers";
	public final String temp_CMPGN = "vantage-167009:Learning.Temp_CMPGN";
	
	public final String temp_PCPCI = "vantage-167009:Learning.PCI_Temp";
	public final String temp_PciProspect = "vantage-167009:Learning.Temp_PciProspect";
	public final String pciProspect = "vantage-167009:Xtaas.pci_prospect";
}
