package com.Essential;

import com.FinalJoins.Join1.Init;
import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.join.CoGbkResult;
import com.google.cloud.dataflow.sdk.transforms.join.CoGroupByKey;
import com.google.cloud.dataflow.sdk.transforms.join.KeyedPCollectionTuple;
import com.google.cloud.dataflow.sdk.values.KV;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.TupleTag;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Joins implements Serializable {
	
	
	
	public PCollection<TableRow> leftOuterJoin(PCollection<KV<String, TableRow>> kvpCollection1,
	                                            PCollection<KV<String, TableRow>> kvpCollection2){
		
		TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> gbkResultPCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		List<String> fieldList = new ArrayList<>();
		
		PCollection<TableRow> resultPCollection = gbkResultPCollection
				.apply(ParDo.named("Result").of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterator1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterator2 = element.getValue().getAll(tupleTag2);
						
						TableRow freshRow;
						
						if(rowIterator1.spliterator().getExactSizeIfKnown() != 0) {
							for (TableRow tableRow1 : rowIterator1) {
								if(rowIterator2.spliterator().getExactSizeIfKnown() != 0) {
									for (TableRow tableRow2 : rowIterator2) {
										freshRow = new TableRow();
										for (String field : tableRow1.keySet()) {
											freshRow.set(field, tableRow1.get(field));
										}
										
										for (String field : tableRow2.keySet()) {
											freshRow.set(field, tableRow2.get(field));
											fieldList.add(field);
										}
										context.output(freshRow);
									}
								}
								else{
									freshRow = new TableRow();
									for (String field : tableRow1.keySet()) {
										freshRow.set(field, tableRow1.get(field));
									}
									
									for (String field : fieldList) {
										freshRow.set(field, "null");
									}
									context.output(freshRow);
								}
							}
						}
					}
				}));
		return resultPCollection;
	}
	
	public PCollection<TableRow> leftOuterJoin1(PCollection<KV<String, TableRow>> kvpCollection1,
	                                            PCollection<KV<String, TableRow>> kvpCollection2,
	                                            String table1Prefix, String table2Prefix,
	                                            String transformName){
		
		TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> gbkResultPCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		List<String> fieldList = new ArrayList<>();
		
		PCollection<TableRow> resultPCollection = gbkResultPCollection
				.apply(ParDo.named(transformName).of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterator1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterator2 = element.getValue().getAll(tupleTag2);
						
						TableRow freshRow;
						
						if(rowIterator1.spliterator().getExactSizeIfKnown() != 0) {
							for (TableRow tableRow1 : rowIterator1) {
								if(rowIterator2.spliterator().getExactSizeIfKnown() != 0) {
									for (TableRow tableRow2 : rowIterator2) {
										freshRow = new TableRow();
										for (String field : tableRow1.keySet()) {
											freshRow.set(table1Prefix + field, tableRow1.get(field));
										}
										
										for (String field : tableRow2.keySet()) {
											freshRow.set(table2Prefix + field, tableRow2.get(field));
											fieldList.add(field);
										}
										context.output(freshRow);
									}
								}
								else{
									freshRow = new TableRow();
									for (String field : tableRow1.keySet()) {
										freshRow.set(table1Prefix + field, tableRow1.get(field));
									}
									
									for (String field : fieldList) {
										freshRow.set(table2Prefix + field, "null");
									}
									context.output(freshRow);
								}
							}
						}
					}
				}));
		return resultPCollection;
	}
	
	public PCollection<TableRow> leftOuterJoin2(PCollection<KV<String, TableRow>> kvpCollection1,
	                                            PCollection<KV<String, TableRow>> kvpCollection2,
	                                            String table2Prefix,
	                                            String transformName){
		
		TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> gbkResultPCollection = KeyedPCollectionTuple
				.of(tupleTag1, kvpCollection1)
				.and(tupleTag2, kvpCollection2)
				.apply(CoGroupByKey.create());
		
		List<String> fieldList = new ArrayList<>();
		
		PCollection<TableRow> resultPCollection = gbkResultPCollection
				.apply(ParDo.named(transformName).of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterator1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterator2 = element.getValue().getAll(tupleTag2);
						
						
						
						if(rowIterator1.spliterator().getExactSizeIfKnown() != 0) {
							for (TableRow tableRow1 : rowIterator1) {
								if(rowIterator2.spliterator().getExactSizeIfKnown() != 0) {
									for (TableRow tableRow2 : rowIterator2) {
										
										for (String field : tableRow2.keySet()) {
											tableRow1.set(table2Prefix + field, tableRow2.get(field));
											fieldList.add(field);
										}
										context.output(tableRow1);
									}
								}
								else{
									for (String field : fieldList) {
										tableRow1.set(table2Prefix + field, "null");
									}
									context.output(tableRow1);
								}
							}
						}
					}
				}));
		return resultPCollection;
	}
	
	public PCollection<TableRow> innerJoin1(PCollection<KV<String, TableRow>> stringPCollection1,
	                                        PCollection<KV<String, TableRow>> stringPCollection2,
	                                        String table1Prefix, String table2Prefix,
	                                        String transformName){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named(transformName).of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						TableRow tableRow;
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								tableRow = new TableRow();
								
								for(String field: tableRow1.keySet()){
									tableRow.set(table1Prefix + field, tableRow1.get(field));
								}
								
								for(String field : tableRow2.keySet()){
									tableRow.set(table2Prefix + field, tableRow2.get(field));
								}
								context.output(tableRow);
							}
						}
					}
				}));
		return resultPCollection;
	}
	
	public PCollection<TableRow> innerJoin2(PCollection<KV<String, TableRow>> stringPCollection1,
	                                        PCollection<KV<String, TableRow>> stringPCollection2,
											String table2Prefix,
	                                        String transformName){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named(transformName).of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						
						for(TableRow tableRow1 : rowIterable1){
							
							for(TableRow tableRow2 : rowIterable2){
								
								for(String field : tableRow2.keySet()){
									tableRow1.set(table2Prefix + field, tableRow2.get(field));
								}
								context.output(tableRow1);
							}
						}
					}
				}));
		return resultPCollection;
	}
	
	public PCollection<TableRow> multiCombine(PCollection<KV<String, TableRow>> stringPCollection1,
	                                          PCollection<KV<String, TableRow>> stringPCollection2,
	                                          PCollection<KV<String, TableRow>> stringPCollection3,
	                                          PCollection<KV<String, TableRow>> stringPCollection4,
	                                          String table1Prefix, String table2Prefix,
	                                          String table3Prefix, String table4Prefix,
	                                          String transformName){
		
		final TupleTag<TableRow> tupleTag1 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag2 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag3 = new TupleTag<>();
		final TupleTag<TableRow> tupleTag4 = new TupleTag<>();
		
		PCollection<KV<String, CoGbkResult>> pCollection = KeyedPCollectionTuple
				.of(tupleTag1, stringPCollection1)
				.and(tupleTag2, stringPCollection2)
				.and(tupleTag3, stringPCollection3)
				.and(tupleTag4, stringPCollection4)
				.apply(CoGroupByKey.create());
		
		PCollection<TableRow> resultPCollection = pCollection
				.apply(ParDo.named(transformName).of(new DoFn<KV<String, CoGbkResult>, TableRow>() {
					@Override
					public void processElement(ProcessContext context) throws Exception {
						KV<String, CoGbkResult> element = context.element();
						
						Iterable<TableRow> rowIterable1 = element.getValue().getAll(tupleTag1);
						Iterable<TableRow> rowIterable2 = element.getValue().getAll(tupleTag2);
						Iterable<TableRow> rowIterable3 = element.getValue().getAll(tupleTag3);
						Iterable<TableRow> rowIterable4 = element.getValue().getAll(tupleTag4);
						
						TableRow tableRow;
						
						for(TableRow tableRow1 : rowIterable1){
							for(TableRow tableRow2 : rowIterable2){
								for(TableRow tableRow3 : rowIterable3){
									for (TableRow tableRow4 : rowIterable4){
										tableRow = new TableRow();
										for(String field : tableRow1.keySet()){
											tableRow.set(table1Prefix + field, tableRow1.get(field));
										}
										for(String field : tableRow2.keySet()){
											tableRow.set(table2Prefix + field, tableRow2.get(field));
										}
										for(String field : tableRow3.keySet()){
											tableRow.set(table3Prefix + field, tableRow3.get(field));
										}
										for(String field : tableRow4.keySet()){
											tableRow.set(table4Prefix + field, tableRow4.get(field));
										}
										context.output(tableRow);
									}
								}
							}
						}
					}
				}));
		
		
		return resultPCollection;
	}
	
	
	
}
