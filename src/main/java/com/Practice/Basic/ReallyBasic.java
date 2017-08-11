package com.Practice.Basic;

import com.example.BigQuerySnippets;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Table;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.Description;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.options.Validation;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ReallyBasic {
	
	
	private static final String table1Name = "vantage-167009:Learning.Test1";
	private static final String table2Name = "vantage-167009:Learning.Test2";
	
	static Logger LOG = LoggerFactory.getLogger(ReallyBasic.class);
	
	private static List<Field> getThemFields(String datasetName, String tableName){
		BigQuery bigQuery = BigQueryOptions.getDefaultInstance().getService();
		BigQuerySnippets bigQuerySnippets = new BigQuerySnippets(bigQuery);
		Table table = bigQuerySnippets.getTable(datasetName,tableName);
		List<Field> fieldSchemas = table.getDefinition().getSchema().getFields();
		
		return fieldSchemas;
	}

	
	static class ReadFromTable1 extends DoFn<TableRow, KV<String, TableRow>> {
		@Override
		public void processElement(ProcessContext context) throws Exception {
			TableRow row = context.element();
			String id = (String) row.get("id");
			context.output(KV.of(id, row));
		}
	}
	
	static List<TableRow> getTheNames(Iterator<TableRow> rowIterator){
		List<TableRow> rowList = new ArrayList<>();
		while(rowIterator.hasNext()){
			rowList.add(rowIterator.next());
		}
		return rowList;
	}
	
	static TableRow setTheNewRow(Iterator<TableRow> rowIterator1, Iterator<TableRow> rowIterator2, 	List<Field> fieldMetaDataList1, List<Field> fieldMetaDataList2){
		TableRow tableRow = new TableRow();
		
		List<TableRow> rowList1 = getTheNames(rowIterator1);
		List<TableRow> rowList2 = getTheNames(rowIterator2);
		
		long size1 = rowList1.spliterator().getExactSizeIfKnown();
		long size2 = rowList2.spliterator().getExactSizeIfKnown();
		long length = (size1 > size2) ? size1: size2;
		int i = 0,j = 0, counter = 0;
		
		while (counter < length){
		
			if(i+1 < size1){
				for (Field field : fieldMetaDataList1){
					tableRow.set(field.getName(), rowList1.get(i).get(field.getName()));
				}
				i++;
			}else{
				for (Field field : fieldMetaDataList1){
					tableRow.set(field.getName(), rowList1.get(i).get(field.getName()));
				}
			}
			
			if(j+1 < size2){
				for (Field field : fieldMetaDataList2){
					tableRow.set(field.getName(), rowList2.get(i).get(field.getName()));
				}
				j++;
			}else{
				for (Field field : fieldMetaDataList2){
					tableRow.set(field.getName(), rowList2.get(i).get(field.getName()));
				}
			}
			counter++;
		}
		return tableRow;
	}
	
	static PCollection<TableRow> combineTableDetails(PCollection<TableRow> stringPCollection1, PCollection<TableRow> stringPCollection2){
		
		PCollection<KV<String, TableRow>> kvpCollection1 = stringPCollection1.apply(ParDo.named("FormatData1").of(new ReadFromTable1()));
		PCollection<KV<String, TableRow>> kvpCollection2 = stringPCollection2.apply(ParDo.named("FormatData2").of(new ReadFromTable1()));
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						TableRow newRow = new TableRow();
						
						LOG.info("IN THE FUNCTION");
						
						Iterator<TableRow> rowIterator1 = element.getValue().getAll(tupleTag1).iterator();
						Iterator<TableRow> rowIterator2 = element.getValue().getAll(tupleTag2).iterator();
						
//						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
//						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
//
//						long sizeOfItr1 = rowIterable1.spliterator().getExactSizeIfKnown();
//						long sizeOfItr2 = rowIterable2.spliterator().getExactSizeIfKnown();

						List<Field> fieldMetaDataList1 = getThemFields("Learning","Test1");
						List<Field> fieldMetaDataList2 = getThemFields("Learning","Test2");
						
						
						TableRow tableRow = setTheNewRow(rowIterator1, rowIterator2, fieldMetaDataList1, fieldMetaDataList2);
						context.output(tableRow);
						
//						int i = 0, j = 0;
//
//						while(i < sizeOfItr1 || j < sizeOfItr2){
//							for( TableRow tableRow : rowIterable1 ){
//
//							}
//						}
//						while()
						
						LOG.info("GONNA ENTER THE LOOP");
//						while(rowIterator1.hasNext() || rowIterator2.hasNext()){
//							TableRow row1Details = rowIterator1.next();
//							TableRow row2Details = rowIterator2.next();
//
//							LOG.info("GOING FOR ROW1DETAILS");
//
//							if(row1Details != null){
//								LOG.info("NOTICE: "+row1Details.toPrettyString());
//								for(Field metaData : fieldMetaDataList1){
//									newRow.set(metaData.getName(), row1Details.get(metaData.getName()));
//								}
//							}
//
//							LOG.info("GOING FOR ROW2DETAILS");
//
//							if(row2Details != null){
//								LOG.info("NOTICE: "+row2Details.toPrettyString());
//								for(Field metaData : fieldMetaDataList2){
//									newRow.set(metaData.getName(), row2Details.get(metaData.getName()));
//								}
//							}
//
//							context.output(newRow);
//						}
					}
				}));
		
		return resultPCollection;
	}
	
	interface Options extends PipelineOptions {
		@Description("Output path for String")
		@Validation.Required
		String getOutput();
		void setOutput(String output);
	}
	
	public static void main(String[] args) {
		
		List<TableFieldSchema> fieldSchemaList = new ArrayList<>();
		fieldSchemaList.add(new TableFieldSchema().setName("id").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("value1").setType("STRING"));
		fieldSchemaList.add(new TableFieldSchema().setName("value2").setType("STRING"));

		TableSchema tableSchema = new TableSchema().setFields(fieldSchemaList);
		
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		Pipeline pipeline = Pipeline.create(options);
		
		
		PCollection<TableRow> rowPCollection1 = pipeline.apply(BigQueryIO.Read.named("Reader1").from(table1Name));
		PCollection<TableRow> rowPCollection2 = pipeline.apply(BigQueryIO.Read.named("Reader2").from(table2Name));
		
		PCollection<TableRow> pCollection = combineTableDetails(rowPCollection1, rowPCollection2);
		
		pCollection.apply(BigQueryIO.Write.named("Writer").to(options.getOutput())
				.withSchema(tableSchema)
				.withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED)
				.withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE));
		
		pipeline.run();
		
	}
	
	@Test
	public void test1(){
		for(Field field: getThemFields("Learning", "Test1")){
			System.out.println(field.getName());
		}
	}

	@Test
	public void test2(){
		List<Integer> intList = new ArrayList<>();
		for(int i = 0;i<14;i++){
			intList.add(i);
		}
		System.out.println(intList.spliterator().getExactSizeIfKnown());
	}
}
