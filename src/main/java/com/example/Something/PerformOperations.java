package com.example.Something;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;

import java.util.ArrayList;
import java.util.Iterator;

public class PerformOperations extends DoFn<TableRow, String> {
	@Override
	public void processElement(ProcessContext c) throws Exception {
	
	}
	
	public static PCollection<String> extractData(PCollection<TableRow> rowPCollection, PCollectionView<FieldArrayList> collectionView){
		PCollection<String> result = rowPCollection.apply(ParDo.named("Extractor").withSideInputs(collectionView)
				.of(new DoFn<TableRow, String>() {
				@Override
				public void processElement(ProcessContext context) throws Exception {
					TableRow row = context.element();
					ArrayList<SchemaDetails> detailsArrayList = context.sideInput(collectionView).getSchemaDetailsList();
					
					String outputString = "";
					int size = detailsArrayList.size();
					
					Iterator<SchemaDetails> detailsIterator = detailsArrayList.iterator();
					int index= 0;
					while (detailsIterator.hasNext()){
						index++;
						SchemaDetails schemaDetails = detailsIterator.next();
						String fieldName = schemaDetails.getOutputFieldName();
						String dataType = schemaDetails.getOtputFieldDataType();
						
						if(dataType.equalsIgnoreCase("STRING")){
							outputString += "\"" + row.get(fieldName) + "\"";
						}else{
							outputString += row.get(fieldName);
						}
						if(index < size){
							outputString += ",";
						}
					}
					context.output(outputString);
				}
				}));
		return result;
	}
	
	public static PCollection<SchemaDetails> translateToSchema(PCollection<TableRow> rowPCollection){
		return rowPCollection.apply(ParDo.named("Extracting required schema fields")
		.of(new DoFn<TableRow, SchemaDetails>() {
			@Override
			public void processElement(ProcessContext context) throws Exception {
				TableRow row = context.element();
				SchemaDetails schemaDetails = new SchemaDetails();
				schemaDetails.setOutputFieldName((String) row.get("input_field_name"));
				schemaDetails.setOtputFieldDataType((String) row.get("datatype"));
				context.output(schemaDetails);
			}
		}));
	}
}
