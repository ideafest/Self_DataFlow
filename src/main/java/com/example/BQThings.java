package com.example;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;

import java.util.ArrayList;
import java.util.List;

public class BQThings {
	
	private static final String tableName = "zimetrics:Learning.shakespeare_copy";
	
	public static void main(String[] args) {
		
		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
		Table table = bigQuerySnippets.getTable("Learning","shakespeare_copy");
		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
		List<String> fieldNames = new ArrayList<>();
		
		for (Field field : fieldSchemas){
			System.out.println(field.getName() + ", " + field.getType().getValue());
			fieldNames.add(field.getName());
		}
		
		for(String sre : fieldNames){
			System.out.println(sre);
		}
		
		
	}
	
}
