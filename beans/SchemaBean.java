package edu.buffalo.cse562.beans;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.ColumnDefinition;
import edu.buffalo.cse562.globals.GlobalConstants;

public class SchemaBean {

	public File file;
	public String jF1 = null;
	public String jF2 = null;
	public List<ColumnDefinition> colNames;
	public Map<String,Integer> colIdx;
	public Map<String,String> coltype;
	public SchemaBean(String fileName) {
		System.out.println("Filename is "+fileName);
		file = new File(GlobalConstants.PREFIX_DATA_PATH + fileName + GlobalConstants.DAT_SUFFIX);
	}
	public SchemaBean(String fileName, List<ColumnDefinition> colNames){
		System.out.println("Filename is "+fileName);
		file = new File(GlobalConstants.PREFIX_DATA_PATH + fileName + GlobalConstants.DAT_SUFFIX );
		System.out.println("Filename is "+file.getName());		;
		colIdx = new HashMap<String, Integer>();
		coltype = new HashMap<String, String>();
		this.colNames = colNames;
		int count = 0;
		
		for (ColumnDefinition col:colNames){
			ColDataType coDataType = col.getColDataType();
			String type = coDataType.getDataType();
			System.out.println("Type of data is "+type);
			coltype.put(fileName+ "." + col.getColumnName(), type);
			colIdx.put(fileName+ "." + col.getColumnName(), count);
			count++;
		}
	}
	public SchemaBean(String fileName1,String fileName2, List<ColumnDefinition> colNames1,List<ColumnDefinition> colNames2,List<ColumnDefinition> colNamesComb){
		System.out.println("Filename is "+fileName1+" "+fileName2);
		String newName = fileName1 + fileName2 + GlobalConstants.JOIN_FILE;
		file = new File(GlobalConstants.PREFIX_DATA_PATH + newName );
		jF1 = fileName1;
		jF2 = fileName2;
		colIdx = new HashMap<String, Integer>();
		coltype = new HashMap<String, String>();
		this.colNames = colNamesComb;
		int count = 0;
		for (ColumnDefinition col:colNames1){
			ColDataType coDataType = col.getColDataType();
			String type = coDataType.getDataType();
			coltype.put(fileName1+ "." + col.getColumnName(), type);
			colIdx.put(fileName1+ "." + col.getColumnName(), count);
			count++;
		}
		for (ColumnDefinition col:colNames2){
			ColDataType coDataType = col.getColDataType();
			String type = coDataType.getDataType();
			coltype.put(fileName2+ "." + col.getColumnName(), type);
			colIdx.put(fileName2+ "." + col.getColumnName(), count);
			count++;
		}
	}
	public SchemaBean() {
		// TODO Auto-generated constructor stub
	}
	public SchemaBean(SchemaBean leftSchema, SchemaBean rightSchema) {
		
		jF1 = leftSchema.file.getName();
		jF2 = rightSchema.file.getName();
		System.out.println("Filename is "+jF1+" "+jF2);
		String newName = jF1 + jF2 + GlobalConstants.JOIN_FILE;
		file = new File(GlobalConstants.PREFIX_DATA_PATH + newName );
		colNames = new ArrayList<ColumnDefinition>();
		colNames.addAll(leftSchema.colNames);
		colNames.addAll(rightSchema.colNames);
		colIdx = new HashMap<String, Integer>(leftSchema.colIdx);
		colIdx.putAll(leftSchema.colIdx);
		coltype = new HashMap<String, String>(leftSchema.coltype);
		coltype.putAll(leftSchema.coltype);
		coltype.putAll(rightSchema.coltype);
		//colIdx.putAll(rightSchema.colIdx);
		int offset = leftSchema.colNames.size();
		System.out.println("offset is :" + offset);
		Iterator it = rightSchema.colIdx.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry<String, Integer> pair = (Map.Entry)it.next();
	        colIdx.put(pair.getKey() , (pair.getValue()+offset));
	        //it.remove(); 
	    }
	    
	    it = colIdx.entrySet().iterator();
	    while (it.hasNext()) {
	        Map.Entry pair = (Map.Entry)it.next();
	        System.out.println("Key is "+pair.getKey() + " = " + pair.getValue());
	        //it.remove(); // avoids a ConcurrentModificationException
	    }
	    
	    
/*		for (ColumnDefinition col:leftSchema.colNames){
			colIdx.put(jF1+ "." + col.getColumnName(), count);
			count++;
		}
		for (ColumnDefinition col:rightSchema.colNames){
			colIdx.put(jF2+ "." + col.getColumnName(), count);
			count++;
		}*/
	}

}
