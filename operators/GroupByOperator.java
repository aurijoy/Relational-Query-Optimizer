package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.interfaces.Operator;

public class GroupByOperator implements Operator{
	
	SchemaBean schema;
	Operator input;
	Boolean aggregator;
	List<Column> groupbyReferences;
	List<SelectItem> items;
	Map<String,ArrayList<LeafValue[]>> groups;
	Iterator<Entry<String, List<LeafValue[]>>> groupItr;
	Iterator<LeafValue[]> currItr;
	ArrayList<LeafValue[]> tuple_data;	
	HashMap<String, Integer> Aggregation;
	ArrayList<Integer> grp_list;
	ArrayList<LeafValue[]> last_data;
	Iterator<LeafValue[]> grpItr;

	public GroupByOperator() {
		// TODO Auto-generated constructor stub
	}
	public GroupByOperator(Operator input, SchemaBean schema, List<Column> groupByReferences,List<SelectItem> items) {
		this.input = input;
		this.groupbyReferences = groupByReferences;
		this.schema = schema;
		this.items = items;
		groups = new HashMap<String, ArrayList<LeafValue[]>>();
		tuple_data = new ArrayList<LeafValue[]>();
		Aggregation = new HashMap<String, Integer>();
		grp_list = new ArrayList<Integer>();
		genGroupList();
		
	}
	private void genGroupList(){
		//GET ALL TUPLES
		LeafValue[] tuple = null;
		//Buffer all the tuples then do a groupby
		int i=0;
		for(SelectItem item:items){
			System.out.println("The items to be selected are "+item+" "+i);
			Expression exp = ((SelectExpressionItem)item).getExpression();
						
			
			if(exp instanceof Function)
			{
				Function func = (Function)exp;
				if(func.getName().matches("SUM|AVG|MIN|MAX|COUNT"))
				{
					System.out.println("Sum found");
					Aggregation.put(func.getName(), i);
				}
				ExpressionList expList=func.getParameters();				
				System.out.println("Expression list is "+expList.getExpressions());
				List exp_list = expList.getExpressions();
				for (Object object : exp_list) {
					System.out.println("Expressions are "+object);
					System.out.println("Expression class is "+object.getClass());
				}
				System.out.println("Expression list class is "+expList.getExpressions().getClass());
				
			}
			else
			{
				for(Column col:groupbyReferences){
					System.out.println("Groupby references are "+ col);
					System.out.println("Expression to string is "+exp.toString());
					if(col.getWholeColumnName().equals(exp.toString()))
					{
						System.out.println("Column groupby is"+item);
						grp_list.add(i);
					}
				}
			}
			i++;
		}
					
		
		do{
			tuple = input.getNext();						
			ArrayList<LeafValue[]> test_data = new ArrayList<LeafValue[]>();
			while(tuple!=null)
			{
				ArrayList<LeafValue[]> grp_data = new ArrayList<LeafValue[]>();
				String grp="";
				//if(tuple!=null)
				LeafValue[] test =tuple;
				for (Integer value: grp_list){
					grp+=test[value].toString();
				}
				System.out.println("Groupby data is "+grp);
				//groups.get()
				test_data =groups.get(grp);
				if(test_data == null)
					{						
						grp_data.add(tuple);
					}
				else
				{
					grp_data = test_data;
					grp_data.add(tuple);
				}
				groups.put(grp, grp_data);
				tuple_data.add(tuple);												
				tuple=input.getNext();				
			}
						
			this.last_data = new ArrayList<LeafValue[]>();			
			ArrayList<LeafValue[]> data = new ArrayList<LeafValue[]>();
			//iterate over map
			Iterator it = groups.entrySet().iterator();						
		    while (it.hasNext()) {
		        Map.Entry pair = (Map.Entry)it.next();
		        LeafValue [] data_write;
		        System.out.println("Key is"+pair.getKey());
		        ArrayList<LeafValue[]> data1 = (ArrayList<LeafValue[]>) pair.getValue();
		        
		        for (LeafValue[] leafValues : data1) {
		        	for (LeafValue val : leafValues) {
						//System.out.println("Tuple is "+leafValue.toString());		        					        					        		
						
							if(val instanceof StringValue){
								String stringVal = val.toString();
								System.out.print(stringVal.substring(1, stringVal.length()-1) + "|");
							}else
								System.out.print(val+ "|");
					}
					System.out.println("");					
				}
				System.out.println("Groups");					
		        data_write = data1.get(0);
		        //Iterate over data
		        Iterator agg_fun = Aggregation.entrySet().iterator();
		        while(agg_fun.hasNext())
		        {
		        	Map.Entry<String, Integer> agg_list = (Entry<String, Integer>) agg_fun.next();
		        	Integer index = agg_list.getValue();
		        	String operation = agg_list.getKey();
		        	if(operation.equalsIgnoreCase("SUM")){
		        		Long value=0L;
		        		for (LeafValue[] leafValues : data1) {
							value+=Long.parseLong(leafValues[index].toString());
						}
		        		data_write[index]=new StringValue("'" + value.toString() + "'");
		        		}
		        	else if(operation.equalsIgnoreCase("AVG")){
		        		Long size = (long) data1.size();
		        		Long value=0L;
		        		for (LeafValue[] leafValues : data1) {
		        			value+=Long.parseLong(leafValues[index].toString());
						}
		        		Long avg = value/size;
		        		data_write[index]= new StringValue("'" + avg.toString() + "'");
		        	}
		        	else if(operation.equalsIgnoreCase("MIN")){
		        		Long min_data=Long.parseLong(data_write[index].toString());
		        		Long test_dat;
		        		for (LeafValue[] leafValues : data1) {
		        			test_dat = Long.parseLong(leafValues[index].toString());
							if(min_data<test_dat)
							{
								min_data=test_dat;
							}
						}
		        		data_write[index]= new StringValue("'" + min_data.toString() + "'");
		        	}
		        	else if(operation.equalsIgnoreCase("MAX")){
		        		Long max_data=Long.parseLong(data_write[index].toString());
		        		Long test_dat;
		        		for (LeafValue[] leafValues : data1) {
		        			test_dat = Long.parseLong(leafValues[index].toString());
		        			if(max_data>test_dat)
							{
								max_data=test_dat;
							}
						}
		        		data_write[index]= new StringValue("'" + max_data.toString() + "'");
		        	}
		        	else if(operation.equalsIgnoreCase("COUNT")){
		        		
		        		int size = data1.size();
		        		data_write[index]=new StringValue("'" + size + "'");
		        	}
		        }
		        this.last_data.add(data_write);		        		       
		    }
		    
		    //Perform groupby on the data
		    /*for (LeafValue[] leafValues : last_data) {
	        	for (LeafValue val : leafValues) {
					//System.out.println("Tuple is "+leafValue.toString());		        					        					        		
					
						if(val instanceof StringValue){
							String stringVal = val.toString();
							System.out.print(stringVal.substring(1, stringVal.length()-1) + "|");
						}else
							System.out.print(val+ "|");
				}
				System.out.println("");					
			}*/			
									
		}while(tuple!=null);	
		 this.grpItr = this.last_data.iterator();
		 this.getNext();
	}

	@Override
	public LeafValue[] getNext() {				
		if(this.grpItr.hasNext())
			return this.grpItr.next();
		else{			
				return null;
			}
		}	

	@Override
	public void reset() {
		genGroupList();
		
	}

}
