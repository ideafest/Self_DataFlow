package com.Practice;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.io.BigQueryIO;
import com.google.cloud.dataflow.sdk.options.DataflowPipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.options.PipelineOptionsFactory;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.values.PCollection;
import org.junit.Test;

public class MainTestClass {
	
	@Test
	public void test1(){
		
		PipelineOptions options = PipelineOptionsFactory.create().as(DataflowPipelineOptions.class);
		Pipeline pipeline = Pipeline.create(options);
		
		TableDetails tableDetails = new TableDetails();
		String tableName = "vantage-167009:Xtaas.PC_PCI";
		
		class ReadFromTable extends DoFn<TableRow, TableRow>{
			
			@Override
			public void processElement(ProcessContext context) throws Exception {
				context.output(context.element());
			}
		}
		
		PCollection<TableRow> rowPCollection = pipeline.apply(BigQueryIO.Read.from(tableName))
				.apply(ParDo.of(new ReadFromTable()));
		
		tableDetails.setRowPCollection(rowPCollection);
	}
	
}
