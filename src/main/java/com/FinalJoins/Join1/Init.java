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

public class Init {
	
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
	PCollection<TableRow> entity_agent;
	PCollection<TableRow> entity_qa;
	PCollection<TableRow> entity_prospect;
	PCollection<TableRow> statemapping;
	PCollection<TableRow> new_entity_campaign;
	PCollection<TableRow> campaignassets;
	
	
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
		
		String entityAgent = "\n" +
				" SELECT a._id _id, a.firstname firstname, a.lastname lastname, d.nickname nickname, a.partnerid partnerid, a.status status, d.line1 line1, d.line2 line2, d.city city, d.state state, d.postalcode postalcode, d.country country, d.timezone timezone, b.languages languages, c.domains domains\n" +
				"   FROM [vantage-167009:Xtaas.agent] a\n" +
				"   JOIN ( SELECT agentlanguages._id _id, agentlanguages.isdeleted, agentlanguages.isdirty,Group_concat(agentlanguages.languages, ',')\n" +
				"          OVER( \n" +
				"          PARTITION BY agentlanguages._id) AS languages\n" +
				"           FROM [vantage-167009:Xtaas.agentlanguages] agentlanguages) b ON a._id = b._id\n" +
				"   JOIN ( SELECT agentdomains._id _id, agentdomains.isdeleted, agentdomains.isdirty,Group_concat(agentdomains.domains, ',')\n" +
				"     OVER( \n" +
				"     PARTITION BY agentdomains._id) AS domains\n" +
				"      FROM [vantage-167009:Xtaas.agentdomains] agentdomains) c ON a._id = c._id\n" +
				"   JOIN [vantage-167009:Xtaas.agentaddress] d ON a._id = d._id\n" +
				"  WHERE a.isdeleted = false AND a.isdirty = false AND b.isdeleted = false AND b.isdirty = false AND c.isdeleted = false AND c.isdirty = false AND d.isdeleted = false AND d.isdirty = false\n" +
				"  Group by _id,firstname,lastname,nickname,partnerid,status,line1,line2,city,state,postalcode,country,timezone, languages, domains";
		
		
		String entityQa = " SELECT  a._id _id, a.firstname firstname, a.lastname lastname, a.partnerid partnerid, a.status status, d.line1 line1, d.line2 line2, d.city city, d.state state, d.postalcode postalcode, d.country country, d.timezone timezone,group_concat(b.languages, ',') \n" +
				" AS languages,Group_concat(c.domains, ',') \n" +
				" AS domains\n" +
				"   FROM [vantage-167009:Xtaas.qa] a\n" +
				"   LEFT JOIN [vantage-167009:Xtaas.qalanguages] b ON a._id = b._id\n" +
				"   LEFT JOIN [vantage-167009:Xtaas.qadomains] c ON a._id = c._id\n" +
				"   LEFT JOIN [vantage-167009:Xtaas.qaaddress] d ON a._id = d._id\n" +
				"  WHERE a.isdeleted = false AND a.isdirty = false\n" +
				"  group by _id,firstname,lastname,partnerid,status,line1,line2,city,state,postalcode,country,timezone\n" +
				"  ORDER BY _id,domains,languages";
		
		
		String entityProspect = "SELECT prospectcallid,source,sourceid,status,campaigncontactid,prefixx,firstname, lastname,suffix,title,department,company,industry,email,phone,addressline1,addressline2,city,statecode,zipcode,country,p_domain,p_min_revenue,p_max_revenue,p_min_empcount,p_max_empcount,management_level,mgt_level_c_level,mgt_level_vice_president,mgt_level_president_principal,mgt_level_director,mgt_level_manager,mgt_level_other, \n" +
				"        CASE\n" +
				"            WHEN integer(p_max_revenue) < 1000000 THEN \"less_than_1_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 5000000 THEN \"1_mil_to_less_than_5_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 10000000 THEN \"5_mil_to_less_than_10_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 25000000 THEN \"10_mil_to_less_than_25_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 50000000 THEN \"25_mil_to_less_than_50_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 100000000 THEN \"50_mil_to_less_than_100_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 250000000 THEN \"100_mil_to_less_than_250_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 500000000 THEN \"250_mil_to_less_than_500_mil\"\n" +
				"            WHEN integer(p_max_revenue) < 1000000000 THEN \"500_mil_to_less_than_100_bil\"\n" +
				"            WHEN integer(p_max_revenue) >= 1000000000 THEN \"1_bil_and_above\"\n" +
				"            ELSE NULL\n" +
				"        END AS revenue_range\n" +
				"   FROM(\n" +
				"   SELECT a.p_prospectcallid AS prospectcallid, a.source source, a.sourceid sourceid, a.status status, a.campaigncontactid campaigncontactid, a.prefix prefixx, a.firstname firstname, a.lastname lastname, a.suffix suffix, a.title title, a.department department, a.company company, a.industry industry, a.email email, a.phone phone, a.addressline1 addressline1, a.addressline2 addressline2, a.city city, a.statecode statecode, a.zipcode zipcode, a.country country, b.p_domain p_domain, COALESCE(b.p_min_revenue, '99999999999999999999999999999','0') AS p_min_revenue, COALESCE(b.p_max_revenue, '99999999999999999999999999999','0') AS p_max_revenue, COALESCE(b.p_min_empcount,'99999999999999999999999999999','0') AS p_min_empcount, COALESCE(b.p_max_empcount,'99999999999999999999999999999','0') AS p_max_empcount, \n" +
				"                CASE\n" +
				"                    WHEN instr(lower(a.title), 'chief')|instr(lower(a.title),'c-level') > 0 OR instr(a.title, 'CEO')|\n" +
				"\t\t\t\t\tinstr(a.title,'COO')|\n" +
				"\t\t\t\t\tinstr(a.title,'CFO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CTO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CIO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CXO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CMO')\n" +
				"\t\t\t\t\t > 0 THEN 'C-LEVEL'\n" +
				"                    WHEN instr(lower(a.title), 'vice') > 0 AND instr(lower(a.title), 'president') > 0 THEN 'EXECUTIVE'\n" +
				"                    WHEN instr(lower(a.title), 'president') > 0 OR instr(lower(a.title), 'principal') > 0 THEN 'C-LEVEL'\n" +
				"                    WHEN instr(lower(a.title), 'director') > 0 THEN 'DIRECTOR_SENIOR-DIRECTOR'\n" +
				"                    WHEN instr(lower(a.title), 'manager') > 0 THEN 'MANAGER'\n" +
				"                    ELSE 'OTHER'\n" +
				"                END AS management_level, \n" +
				"                CASE\n" +
				"                    WHEN instr(lower(a.title), 'chief')|instr(lower(a.title),'c-level') > 0 OR instr(a.title, 'CEO')|\n" +
				"\t\t\t\t\tinstr(a.title,'COO')|\n" +
				"\t\t\t\t\tinstr(a.title,'CFO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CTO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CIO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CXO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CMO') > 0 THEN 1\n" +
				"                    ELSE 0\n" +
				"                END AS mgt_level_c_level, \n" +
				"                CASE\n" +
				"                    WHEN instr(lower(a.title), 'chief')|instr(lower(a.title),'c-level') = 0 AND instr(a.title, 'CEO')|\n" +
				"\t\t\t\t\tinstr(a.title,'COO')|\n" +
				"\t\t\t\t\tinstr(a.title,'CFO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CTO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CIO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CXO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CMO') = 0 AND instr(lower(a.title), 'vice') > 0 AND instr(lower(a.title), 'president') > 0 THEN 1\n" +
				"                    ELSE 0\n" +
				"                END AS mgt_level_vice_president, \n" +
				"                CASE\n" +
				"                    WHEN instr(lower(a.title), 'chief')|instr(lower(a.title),'c-level') = 0 AND instr(a.title, 'CEO')|\n" +
				"\t\t\t\t\tinstr(a.title,'COO')|\n" +
				"\t\t\t\t\tinstr(a.title,'CFO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CTO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CIO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CXO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CMO') = 0 AND instr(lower(a.title), 'vice president') = 0 AND instr(lower(a.title), 'vice-president') = 0 AND (instr(lower(a.title), 'president') > 0 OR instr(lower(a.title), 'principal') > 0) THEN 1\n" +
				"                    ELSE 0\n" +
				"                END AS mgt_level_president_principal, \n" +
				"                CASE\n" +
				"                   WHEN instr(lower(a.title), 'chief')|instr(lower(a.title),'c-level') = 0 AND instr(a.title, 'CEO')|\n" +
				"\t\t\t\t\tinstr(a.title,'COO')|\n" +
				"\t\t\t\t\tinstr(a.title,'CFO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CTO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CIO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CXO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CMO') = 0 AND instr(lower(a.title), 'president') = 0 AND instr(lower(a.title), 'principal') = 0 AND instr(lower(a.title), 'director') > 0 THEN 1\n" +
				"                    ELSE 0\n" +
				"                END AS mgt_level_director, \n" +
				"                CASE\n" +
				"                    WHEN instr(lower(a.title), 'chief')|instr(lower(a.title),'c-level') = 0 AND instr(a.title, 'CEO')|\n" +
				"\t\t\t\t\tinstr(a.title,'COO')|\n" +
				"\t\t\t\t\tinstr(a.title,'CFO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CTO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CIO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CXO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CMO') = 0 AND instr(lower(a.title), 'president') = 0 AND instr(lower(a.title), 'principal') = 0 AND instr(lower(a.title), 'director') = 0 AND instr(lower(a.title), 'manager') > 0 THEN 1\n" +
				"                    ELSE 0\n" +
				"                END AS mgt_level_manager, \n" +
				"                CASE\n" +
				"                    WHEN instr(lower(a.title), 'chief')|instr(lower(a.title),'c-level') = 0 AND instr(a.title, 'CEO')|\n" +
				"\t\t\t\t\tinstr(a.title,'COO')|\n" +
				"\t\t\t\t\tinstr(a.title,'CFO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CTO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CIO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CXO')|\n" +
				"\t\t\t\t\tinstr(a.title, 'CMO') = 0 AND instr(lower(a.title), 'president|principal|director|manager') = 0 OR COALESCE(a.title, '0') = '0' THEN 1\n" +
				"                    ELSE 0\n" +
				"                END AS mgt_level_other\n" +
				"           FROM [vantage-167009:Xtaas.prospect] a \n" +
				"      JOIN ( SELECT derived_table1._id, derived_table1.p_prospectcallid, max(derived_table1.p_domain) AS p_domain, max(derived_table1.p_min_revenue) AS p_min_revenue, max(derived_table1.p_max_revenue) AS p_max_revenue, max(derived_table1.p_min_empcount) AS p_min_empcount, max(derived_table1.p_max_empcount) AS p_max_empcount\n" +
				"                   FROM ( SELECT customattributes._id, customattributes.p_prospectcallid, \n" +
				"                                CASE\n" +
				"                                    WHEN customattributes.customattributeskey = 'domain' THEN customattributes.customattributesvalue\n" +
				"                                    ELSE NULL\n" +
				"                                END AS p_domain, \n" +
				"                                CASE\n" +
				"                                    WHEN customattributes.customattributeskey = 'minRevenue' THEN customattributes.customattributesvalue\n" +
				"                                    ELSE NULL\n" +
				"                                END AS p_min_revenue, \n" +
				"                                CASE\n" +
				"                                    WHEN customattributes.customattributeskey = 'maxRevenue' THEN customattributes.customattributesvalue\n" +
				"                                    ELSE NULL\n" +
				"                                END AS p_max_revenue, \n" +
				"                                CASE\n" +
				"                                    WHEN customattributes.customattributeskey = 'minEmployeeCount' THEN customattributes.customattributesvalue\n" +
				"                                    ELSE NULL\n" +
				"                                END AS p_min_empcount, \n" +
				"                                CASE\n" +
				"                                    WHEN customattributes.customattributeskey = 'maxEmployeeCount' THEN customattributes.customattributesvalue\n" +
				"                                    ELSE NULL\n" +
				"                                END AS p_max_empcount\n" +
				"                           FROM [vantage-167009:Xtaas.customattributes] customattributes\n" +
				"                          WHERE customattributes.isdeleted = false AND customattributes.isdirty = false) derived_table1\n" +
				"                  GROUP BY derived_table1._id, derived_table1.p_prospectcallid) b ON a._id = b._id\n" +
				"                  )";
		
		
		String statemapping = "vantage-167009:Xtaas.statemapping";
		
		String newEntityCampaign = "SELECT\n" +
				"  a._id AS campaignid,\n" +
				"  a.organizationid organizationid,\n" +
				"  a.campaignmanagerid campaignmanagerid,\n" +
				"  a.status status,\n" +
				"  a.name name,\n" +
				"  a.startdate startdate,\n" +
				"  a.enddate enddate,\n" +
				"  a.type type,\n" +
				"  a.deliverytarget deliverytarget,\n" +
				"  a.domain domain,\n" +
				"  a.dailycap dailycap,\n" +
				"  a.currentassetid currentassetid,\n" +
				"  a.dialermode dialermode,\n" +
				"  b.supervisorid supervisorid,\n" +
				"  b.callspeedperminperagent callspeedperminperagent,\n" +
				"  c.name AS teamname,\n" +
				"  CONCAT(d.firstname,d.lastname) AS partner\n" +
				"FROM\n" +
				"  [vantage-167009:Xtaas.campaign] a \n" +
				"  JOIN [vantage-167009:Xtaas.campaignteam] b\n" +
				"ON a._id = b._id\n" +
				"JOIN [vantage-167009:Xtaas.team] c\n" +
				"ON  b.teamid = c._id\n" +
				"JOIN [vantage-167009:Xtaas.partner] d\n" +
				"ON c.partnerid = d._id\n" +
				"WHERE a.isdeleted = FALSE  AND a.isdirty = FALSE\n" +
				"  AND b.isdeleted = FALSE  AND b.isdirty = FALSE\n" +
				"  AND c.isdeleted = FALSE  AND c.isdirty = FALSE\n" +
				"  AND d.isdeleted = FALSE  AND d.isdirty = FALSE";
		
		
		String campaignAssets = "vantage-167009:Xtaas.campaignassets";
		
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
		
		this.entity_agent = pipeline.apply(BigQueryIO.Read.named("entity_agent").fromQuery(entityAgent));
		this.entity_prospect = pipeline.apply(BigQueryIO.Read.named("entity_prospect").fromQuery(entityProspect));
		this.entity_qa = pipeline.apply(BigQueryIO.Read.named("entity_qa").fromQuery(entityQa));
		this.new_entity_campaign = pipeline.apply(BigQueryIO.Read.named("new_entity_campaign").fromQuery(newEntityCampaign));
		this.campaignassets = pipeline.apply(BigQueryIO.Read.named("camapaignassets").from(campaignAssets));
		this.statemapping = pipeline.apply(BigQueryIO.Read.named("statemapping").from(statemapping));
		
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
	
	public PCollection<TableRow> getEntity_agent() {
		return entity_agent;
	}
	
	public PCollection<TableRow> getEntity_qa() {
		return entity_qa;
	}
	
	public PCollection<TableRow> getEntity_prospect() {
		return entity_prospect;
	}
	
	public PCollection<TableRow> getStatemapping() {
		return statemapping;
	}
	
	public PCollection<TableRow> getNew_entity_campaign() {
		return new_entity_campaign;
	}
	
	public PCollection<TableRow> getCampaignassets() {
		return campaignassets;
	}
	
	PCollection<TableRow> joinOfPC_PCIAndMaster_Status;
	PCollection<TableRow> joinOfPCIFeedbackResponseListAndPciResponseAttributes;
	PCollection<TableRow> joinOfProspectCallLogAndProspectCall;
	
	public PCollection<TableRow> getJoinOfPC_PCIAndMaster_Status() {
		return joinOfPC_PCIAndMaster_Status;
	}
	
	public PCollection<TableRow> getJoinOfPCIFeedbackResponseListAndPciResponseAttributes() {
		return joinOfPCIFeedbackResponseListAndPciResponseAttributes;
	}
	
	public PCollection<TableRow> getJoinOfProspectCallLogAndProspectCall() {
		return joinOfProspectCallLogAndProspectCall;
	}
	
	public void initJoins(Init init){
		joinOfPC_PCIAndMaster_Status = performJoinOfPC_PCIAndMaster_Status(init);
		joinOfPCIFeedbackResponseListAndPciResponseAttributes = performJoinOfPCIFeedbackResponseListAndPciResponseAttributes(init);
		joinOfProspectCallLogAndProspectCall = performJoinOfProspectCallLogAndProspectCall(init);
	}
	
	
	private static class ExtractFromPC_PCI extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("status");
			context.output(KV.of(id, context.element()));
		}
	}
	
	private static class ExtractFromMaster_Status extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("code");
			context.output(KV.of(id, context.element()));
		}
	}
	
	
	private PCollection<TableRow> performJoinOfPC_PCIAndMaster_Status(Init init){
		
		PCollection<KV<String, TableRow>> pcpci = init.getPC_PCI()
				.apply(ParDo.of(new ExtractFromPC_PCI()));
		
		PCollection<KV<String, TableRow>> master_status = init.getMaster_status()
				.apply(ParDo.of(new ExtractFromMaster_Status()));
		
		PCollection<TableRow> joinedPCollection = joins.innerJoin1(pcpci, master_status, "A_", "B_",
				"JoinPCPCI_MasterStatus");
		
		return joinedPCollection;
	}
	
	private static class ExtractFromPCIFeedbackResponseList_PCIResponseEAttributes extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id") + tableRow.get("index");
			context.output(KV.of(id, tableRow));
		}
	}
	
	
	private PCollection<TableRow> performJoinOfPCIFeedbackResponseListAndPciResponseAttributes(Init init){
		
		PCollection<KV<String, TableRow>> pciFeedbackResponsePCollection = init.getPci_feedbackResponseList()
				.apply(ParDo.of(new ExtractFromPCIFeedbackResponseList_PCIResponseEAttributes()));
		
		PCollection<KV<String, TableRow>> pciResponseAttributesPCollection = init.getPci_responseAttributes()
				.apply(ParDo.of(new ExtractFromPCIFeedbackResponseList_PCIResponseEAttributes()));
		
		PCollection<TableRow> joinedPCollection = joins
				.innerJoin1(pciFeedbackResponsePCollection, pciResponseAttributesPCollection, "A_", "B_",
						"JoinPCIFeedbackResponseList_PCIResponseAttributes");
		
		return joinedPCollection;
		
	}
	
	private static class ExtractFromProspectCallLog_ProspectCall extends DoFn<TableRow, KV<String, TableRow>>{
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow tableRow = context.element();
			String id = (String) tableRow.get("_id");
			context.output(KV.of(id, tableRow));
		}
	}
	
	private PCollection<TableRow> performJoinOfProspectCallLogAndProspectCall(Init init){
		
		PCollection<KV<String, TableRow>> prospectCallLogPCollection = init.getProspectCallLog()
				.apply(ParDo.of(new ExtractFromProspectCallLog_ProspectCall()));
		
		PCollection<KV<String, TableRow>> prospectCallPCollection = init.getProspectCall()
				.apply(ParDo.of(new ExtractFromProspectCallLog_ProspectCall()));
		
		PCollection<TableRow> joinedPCollection = joins
				.innerJoin1(prospectCallLogPCollection, prospectCallPCollection, "A_","B_",
						"JoinProspectCallLog_ProspectCall");
		
		return joinedPCollection;
		
	}
	
}
