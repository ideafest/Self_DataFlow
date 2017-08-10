package com.Practice.Basic;

public class FieldMetaData {
	
	public String name;
	public String dataType;
	
	public FieldMetaData() {
	}
	
	public FieldMetaData(String name, String dataType) {
		this.name = name;
		this.dataType = dataType;
	}
	
	public String getName() {
		return name;
	}
	
	public void setName(String name) {
		this.name = name;
	}
	
	public String getDataType() {
		return dataType;
	}
	
	public void setDataType(String dataType) {
		this.dataType = dataType;
	}
	
	@Override
	public String toString() {
		return "FieldMetaData{" +
				"name='" + name + '\'' +
				", dataType='" + dataType + '\'' +
				'}';
	}
}
