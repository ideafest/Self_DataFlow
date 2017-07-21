package com.example.Something;

import java.util.ArrayList;

public class FieldArrayList {
	
	ArrayList<SchemaDetails> schemaDetailsList;
	
	public FieldArrayList() {
		schemaDetailsList = new ArrayList<>();
	}
	
	public ArrayList<SchemaDetails> getSchemaDetailsList() {
		return schemaDetailsList;
	}
	
	public void setSchemaDetailsList(ArrayList<SchemaDetails> schemaDetailsList) {
		this.schemaDetailsList = schemaDetailsList;
	}
}
