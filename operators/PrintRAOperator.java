package edu.buffalo.cse562.operators;

import net.sf.jsqlparser.expression.LeafValue;
import net.sf.jsqlparser.expression.StringValue;
import edu.buffalo.cse562.interfaces.Operator;

public class PrintRAOperator implements Operator {
	Operator input;
	static int i=0;
	public PrintRAOperator(Operator input)
	{
		this.input= input;
		
	}
	

	@Override
	public LeafValue[] getNext() {
		// TODO Auto-generated method stub
		
		
		LeafValue[] tuple=input.getNext();
		//while(input.getNext()!=null)
	
		do{			
		if(tuple!=null)
		{
		for (LeafValue val : tuple) {
			//System.out.println("Tuple is "+leafValue.toString());
			
				if(val instanceof StringValue){
					String stringVal = val.toString();
					System.out.print(stringVal.substring(1, stringVal.length()-1) + "|");
				}else
					System.out.print(val+ "|");
		}
		System.out.println("");		
		}
		i++;
		tuple=input.getNext();
		if((tuple!=null)&&(tuple.length==0))
			continue;
		}while(tuple!=null);
		//System.out.println(x);
		

		return tuple;
	}

	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}
