package com.Practice;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.values.PCollection;

import java.util.Arrays;

public class TableDetails {
	
	PCollection<TableRow> rowPCollection;
	String[] id_Details;
	
	public TableDetails() {
	}
	
	public TableDetails(PCollection<TableRow> rowPCollection, String[] id_Details) {
		this.rowPCollection = rowPCollection;
		this.id_Details = id_Details;
	}
	
	public PCollection<TableRow> getRowPCollection() {
		return rowPCollection;
	}
	
	public void setRowPCollection(PCollection<TableRow> rowPCollection) {
		this.rowPCollection = rowPCollection;
	}
	
	public String[] getId_Details() {
		return id_Details;
	}
	
	public void setId_Details(String[] id_Details) {
		this.id_Details = id_Details;
	}
	
	@Override
	public String toString() {
		return "TableDetails{" +
				"rowPCollection=" + rowPCollection +
				", id_Details=" + Arrays.toString(id_Details) +
				'}';
	}
}
