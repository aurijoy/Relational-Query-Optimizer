package edu.buffalo.cse562.operators;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.LeafValue.InvalidLeaf;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.interfaces.Operator;

public class OrderbyRAOperator implements Operator{
	
	SchemaBean schema;
	Operator input;
	List<SelectItem> items;
	TreeMap<String,LeafValue[]> ordered_list;
	ArrayList<LeafValue[]> tuple_list;
	Iterator<Entry<String, List<LeafValue[]>>> groupItr;
	Iterator<Entry<String, LeafValue[]>> currItr;
	List<OrderByElement> orderByElements;
	ArrayList<Boolean> AscorDesc;
	ArrayList<Integer> orderbyID;
	ArrayList<String> SchemaInfo;
	HashMap<String,ArrayList<LeafValue[]>> groups;
	private Iterator<LeafValue[]> grpItr;
	
	public OrderbyRAOperator() {
		// TODO Auto-generated constructor stub
	}
	public OrderbyRAOperator(Operator input, SchemaBean schema, List<OrderByElement> orderByElements,List<SelectItem> items) {
		this.input = input;
		this.orderByElements = orderByElements;
		this.schema = schema;
		this.items = items;
		ordered_list = new TreeMap<String, LeafValue[]>();
		AscorDesc = new ArrayList<Boolean>();
		orderbyID = new ArrayList<Integer>();
		groups = new HashMap<String, ArrayList<LeafValue[]>>();
		SchemaInfo = new ArrayList<String>();
		tuple_list = new ArrayList<LeafValue[]>();

		genOrderList();
	}
	private void genOrderList(){
		//GET ALL TUPLES
		System.out.println("Generating the orderby list");
		LeafValue[] tuple = null;
		ArrayList<LeafValue[]> test_data = new ArrayList<LeafValue[]>();
		int i=0;
		ArrayList<Integer> orderbylist = new ArrayList<Integer>() ;
		for(SelectItem item:items){
			System.out.println("The items to be selected are "+item+" "+i);
			SelectExpressionItem type = (SelectExpressionItem) item;
			String alias = type.getAlias();
			Expression exp = type.getExpression();
			System.out.println("Alias is "+alias);
			String col_name =null;
			if(exp instanceof Column)
			{
				Column col1= (Column) exp;
				col_name=col1.getWholeColumnName();
				System.out.println("Whole column name is "+col_name);
			}			
			
			for(OrderByElement oByElement:orderByElements){				
				System.out.println("The orderby expression is "+oByElement.getExpression());				
				Expression expr = oByElement.getExpression();		
				Boolean asc;
				if(alias!=null)
				{
					
					if(expr.toString().equals(alias))
						{
						asc = oByElement.isAsc();
						AscorDesc.add(asc);
						System.out.println("The expr"+expr+" exp: "+alias);
						orderbyID.add(i);
						SchemaInfo.add("int");
						}
				}
				
				if(col_name!= null)
				{

					Column col = (Column) expr;
					String whole_col_name = col.getWholeColumnName();
					System.out.println("The whole column name is "+col.getWholeColumnName());
					if(whole_col_name.equals(col_name))
					{
						System.out.println("The expr"+expr+" exp: "+col_name);
						asc = oByElement.isAsc();
						orderbyID.add(i);
						AscorDesc.add(asc);
						SchemaInfo.add(schema.coltype.get(whole_col_name));
						//schema.colNames;
					}
				}
				
				}			
				i++;
			}
		
		for(Integer integer:orderbyID)
			System.out.println("Order elements are "+integer);
		
		for(Boolean bool:AscorDesc)
			System.out.println("Boolean value is "+bool);
		
		tuple = input.getNext();
		if(tuple==null )
			return;
		
		//Buffered all the tuples
		do{					
			tuple_list.add(tuple);
			tuple=input.getNext();
		}while(tuple!=null);
		
		HashMap<String, ArrayList<LeafValue[]>> groups_new = new HashMap<String, ArrayList<LeafValue[]>>();
		//Sort by the order
		Integer tuple_loc=orderbyID.get(0);
		Boolean order;
		String schemainfo;
		
		
		
		for (LeafValue[] tupl : tuple_list) {
			ArrayList<LeafValue[]> test_dat  = new ArrayList<LeafValue[]>();
			String key = tupl[tuple_loc].toString();
			if(groups_new.get(key)==null)
			{
				test_dat.add(tupl);
				groups_new.put(key, test_dat);
			}
			else
			{
				test_dat = groups_new.get(key);
				test_dat.add(tupl);
				groups_new.put(key, test_dat);
			}			
			//set_tuple.add(key);
		}
		
		
		

		
		for(i =1;i<orderbyID.size();i++)
		{
			Iterator it = groups_new.entrySet().iterator();	
			tuple_loc=orderbyID.get(i);
			order = AscorDesc.get(i);
			schemainfo = SchemaInfo.get(i);
			while(it.hasNext())
			{
				Map.Entry pair = (Entry) it.next();
				ArrayList<LeafValue[]> data1 = (ArrayList<LeafValue[]>) pair.getValue();
				ArrayList<String> tuple_dat = new ArrayList<String>();
				String pair_key = (String) pair.getKey();
				ArrayList<LeafValue[]> data_for_next_iteration = new ArrayList<LeafValue[]>();
				TreeMap<Long, LeafValue[]> hash_tuple_long = new TreeMap<Long, LeafValue[]>();
				TreeMap<String, LeafValue[]> hash_tuple_string = new TreeMap<String, LeafValue[]>();
				for (LeafValue[] leafValues : data1) {
					if(schemainfo.equalsIgnoreCase("int"))
					{						
						Long key = 0L;
						try {
							key =leafValues[tuple_loc].toLong();
						} catch (InvalidLeaf e) {
							// TODO Auto-generated catch block
							e.printStackTrace();
						} 
						if(hash_tuple_long.get(key)==null)
						{
							hash_tuple_long.put(key, leafValues);
						}
						else
						{
							key=(long) (key+0.1);							
							hash_tuple_long.put(key, leafValues);
						}
						
					}
					else
					{
					TreeMap<Long, LeafValue[]> hash_tuple = new TreeMap<Long, LeafValue[]>();	
					String key = leafValues[tuple_loc].toString();
					if(hash_tuple.get(key)==null)
					{
						hash_tuple_string.put(key, leafValues);
					}
					else
					{
						key+=".";
						hash_tuple_string.put(key, leafValues);
					}
					}										
				}
				
				if(order)
				{
					if(schemainfo.equalsIgnoreCase("int"))
					{
						while(!hash_tuple_long.isEmpty())
						{
						Long first_key = hash_tuple_long.firstKey();
						LeafValue[] data_to_be_put=hash_tuple_long.get(first_key);
						hash_tuple_long.remove(first_key);
						data_for_next_iteration.add(data_to_be_put);
						}
					
					}
					else {
						while(!hash_tuple_string.isEmpty())
						{
						String first_key = hash_tuple_string.firstKey();
						LeafValue[] data_to_be_put=hash_tuple_string.get(first_key);
						hash_tuple_string.remove(first_key);
						data_for_next_iteration.add(data_to_be_put);
						}
					}
						
					
				}
				else
				{
					if(schemainfo.equalsIgnoreCase("int"))
					{
						while(!hash_tuple_long.isEmpty())
						{
						Long last_key = hash_tuple_long.lastKey();
						LeafValue[] data_to_be_put=hash_tuple_long.get(last_key);
						hash_tuple_long.remove(last_key);
						data_for_next_iteration.add(data_to_be_put);
						}
					
					}
					else {
						while(!hash_tuple_string.isEmpty())
						{
						String last_key = hash_tuple_string.lastKey();
						LeafValue[] data_to_be_put=hash_tuple_string.get(last_key);
						hash_tuple_string.remove(last_key);
						data_for_next_iteration.add(data_to_be_put);
						}						
					}
						
					}
				groups_new.put(pair_key, data_for_next_iteration);

				
			}									
		}
		
		Iterator<Entry<String, ArrayList<LeafValue[]>>> it = groups_new.entrySet().iterator();
		Set<String>key_set =  groups_new.keySet();
		
		ArrayList<LeafValue[]> last_data= new ArrayList<LeafValue[]>();
		
		
		order = AscorDesc.get(0);
		schemainfo = SchemaInfo.get(0);
		if(schemainfo.equalsIgnoreCase("int"))
		{
			System.out.println("Inside the schema info");
			ArrayList<Long> list_int = new ArrayList<Long>();
			for (String string : key_set) {
				list_int.add(Long.parseLong(string));				
			}
			if(order)
			{
				Collections.sort(list_int);
			}
			else
			{
				Collections.sort(list_int, Collections.reverseOrder());
			}
			for (Long long1 : list_int) {
				System.out.println("Value is "+long1);
				ArrayList<LeafValue[]> data1 = groups_new.get(long1.toString());
				System.out.println("Data is :");
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
				for (LeafValue[] leafValues : data1) {
					last_data.add(leafValues);
				}
			}
		}
		else
		{
			ArrayList<String> list_string = new ArrayList<String>();
			for (String string : key_set) {
				list_string.add(string);				
			}
			if(order)
			{
				Collections.sort(list_string);
			}
			else
			{
				Collections.sort(list_string, Collections.reverseOrder());
			}
			
			for (String string1 : list_string) {
				ArrayList<LeafValue[]> data1 = groups_new.get(string1);
				for (LeafValue[] leafValues : data1) {
					last_data.add(leafValues);
				}
			}
			
		}
		
		for (LeafValue[] leafValues : last_data) {
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
		
		//System.exit(0);

		 this.grpItr = last_data.iterator();
		 System.out.println("Group iterator is "+this.grpItr);
		 System.exit(0);
		 this.getNext();
		
	
		//CALL AGGREGATE ON THOSE TUPLES
		//MAKE LIST
		 //groupItr = groups.entrySet().iterator();
		 //currItr = groupItr.next().getValue().iterator();
		//RETURN ONE BY ONE TO PROJECT
		//this.currItr =  this.ordered_list.entrySet().iterator();
	}

	@Override
	public LeafValue[] getNext() {
		System.out.println("Reached the orderby getnext");
		if(this.grpItr.hasNext())
			return this.grpItr.next();
		else{			
				return null;
			}
		}

	@Override
	public void reset() {
		genOrderList();
		
	}

}
