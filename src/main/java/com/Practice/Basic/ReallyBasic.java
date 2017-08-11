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
	
	static TableRow setTheNewRow(Iterable<TableRow> rowIterable1, Iterable<TableRow> rowIterable2, 	List<Field> fieldMetaDataList1, List<Field> fieldMetaDataList2){
		
		TableRow tableRow = null;
		
		
		
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
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						List<Field> fieldMetaDataList1 = getThemFields("Learning","Test1");
						List<Field> fieldMetaDataList2 = getThemFields("Learning","Test2");
						
						TableRow tableRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								tableRow = new TableRow();
								
								for(Field field: fieldMetaDataList1){
									tableRow.set(field.getName(), tableRow1.get(field.getName()));
								}
								
								for(Field field : fieldMetaDataList2){
									tableRow.set(field.getName(), tableRow2.get(field.getName()));
								}
								context.output(tableRow);
							}
						}
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
