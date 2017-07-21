package com.example.Something;

import com.google.cloud.dataflow.sdk.transforms.Combine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CreateSchema extends Combine.CombineFn<SchemaDetails, CreateSchema.Accum, FieldArrayList>{
	private static final Logger LOG = LoggerFactory.getLogger(CreateSchema.class);
	
	public static class Accum{
		FieldArrayList fieldArrayList = new FieldArrayList();
	}
	
	@Override
	public Accum createAccumulator() {
		return new Accum();
	}
	
	@Override
	public Accum addInput(Accum accumulator, SchemaDetails input) {
		accumulator.fieldArrayList.getSchemaDetailsList().add(input);
		return accumulator;
	}
	
	@Override
	public Accum mergeAccumulators(Iterable<Accum> accumulators) {
		Accum accumulator = createAccumulator();
		for(Accum acc : accumulators){
			accumulator.fieldArrayList.getSchemaDetailsList().addAll(acc.fieldArrayList.getSchemaDetailsList());
		}
		return accumulator;
	}
	
	@Override
	public FieldArrayList extractOutput(Accum accumulator) {
		FieldArrayList outputList = accumulator.fieldArrayList;
		outputList.getSchemaDetailsList().sort(new compareSchemaDetails());
		return outputList;
	}

	

}

class compareSchemaDetails implements java.util.Comparator<SchemaDetails>
{
	private static final Logger LOG = LoggerFactory.getLogger(compareSchemaDetails.class);
	public int compare(SchemaDetails o1, SchemaDetails o2)
	{
		int o1Index, o2Index;
		o1Index = o1.getFieldIndex();
		o2Index = o2.getFieldIndex();
		
		if(o1Index==o2Index)
			return 0;
		
		if(o1Index>o2Index)
			return 1;
		return -1;
	}
}