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
	
	public final String prospectCallLog = "SELECT a._id _id, a._class _class, a.status status, b.createddate createddate, a.createdby createdby, a.updateddate updateddate, a.updatedby updatedby, a.version version, a.isdeleted isdeleted , a.callbackdate callbackdate, a.isdirty isdirty\n" +
			"   FROM ( SELECT row_number()\n" +
			"          OVER( \n" +
			"          PARTITION BY prospectcallid\n" +
			"          ORDER BY updateddate) AS rnum, _id, _class, prospectcallid, status, createddate, createdby, updateddate, updatedby, version, isdeleted, callbackdate, isdirty\n" +
			"           FROM [vantage-167009:Xtaas.pci_prospectcallinteraction]) a\n" +
			"   JOIN ( SELECT derived_table1.prospectcallid, max(derived_table1.rnum) AS rnum, max(derived_table1.updateddate) AS updateddate, min(derived_table1.createddate) AS createddate\n" +
			"           FROM ( SELECT row_number()\n" +
			"                  OVER( \n" +
			"                  PARTITION BY prospectcallid\n" +
			"                  ORDER BY updateddate) AS rnum, _id, _class, prospectcallid, status, createddate, createdby, updateddate, updatedby, version, isdeleted, callbackdate, isdirty\n" +
			"                   FROM [vantage-167009:Xtaas.pci_prospectcallinteraction]) derived_table1\n" +
			"          GROUP BY derived_table1.prospectcallid) b ON a.prospectcallid = b.prospectcallid AND a.updateddate = b.updateddate AND a.rnum = b.rnum;";
	
	
	public final String CMPGN = "SELECT _id,_class,organizationid,campaignmanagerid,\n" +
			"status,name,startdate,enddate,type,deliverytarget,\n" +
			"domain,dailycap,qualificationclause,leadfilteringclause,\n" +
			"selfmanageteam,currentassetid,dialermode,createddate,\n" +
			"createdby,updateddate,updatedby,version,isdeleted,isdirty \n" +
			"FROM [vantage-167009:Xtaas.campaign] WHERE status = 'RUNNING' ;";
	
	public final String master_status = "vantage-167009:Xtaas.master_status";
	
}
