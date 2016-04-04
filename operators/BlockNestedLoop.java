/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.buffalo.cse562.operators;
import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.beans.SchemaBean;
import edu.buffalo.cse562.eval.Evaluator;
import static edu.buffalo.cse562.globals.GlobalConstants.countrel1;
import static edu.buffalo.cse562.globals.GlobalConstants.countrel2;
import static edu.buffalo.cse562.globals.GlobalConstants.hashblock1;
import static edu.buffalo.cse562.globals.GlobalConstants.hashblock2;
import static edu.buffalo.cse562.globals.GlobalConstants.hybridhashtable;
import static edu.buffalo.cse562.globals.GlobalConstants.iterator1;
import static edu.buffalo.cse562.globals.GlobalConstants.iterator2;
import edu.buffalo.cse562.interfaces.Operator;
import java.sql.SQLException;
import java.util.HashMap;
import net.sf.jsqlparser.expression.BooleanValue;
import net.sf.jsqlparser.expression.Expression;
/**
 *
 * @author dhwajjani
 */
public class BlockNestedLoop {
    Operator rel1;
    SchemaBean schema1; 
    Operator rel2;
    SchemaBean schema2;
    String[] joiningAttributes;
     int blocksize=50;
     Expression exp;
//    HashMap<LeafValue[],LeafValue[]> smalltable=new  HashMap<LeafValue[],LeafValue[]>();
//     HashMap<LeafValue[],LeafValue[]> largetable=new  HashMap<LeafValue[],LeafValue[]>();
  public BlockNestedLoop(Operator rel1,SchemaBean schema1,Operator rel2,SchemaBean schema2,String[] joiningAttributes,Expression exp)
  {
      this.rel1=rel1;
      this.schema1=schema1;
      this.rel2=rel2;
      this.schema2=schema2;
      this.joiningAttributes=joiningAttributes;
      this.exp=exp;
  }
  
  public LeafValue[] blockNestedJoin()
  {
     
      int stuples=schema1.colNames.size(),ltuples=schema2.colNames.size();
        int returnsize=stuples+ltuples;
        LeafValue[] returntuple=new LeafValue[returnsize];
        SchemaBean returnschema=null;
//       int colid=schema2.colIdx.get(joiningAttribute);
     for(int i=0;i<stuples;i++)
     { 
         returnschema.colNames.add(schema1.colNames.get(i));
         returnschema.colIdx.put(schema1.file.getName()+"."+schema1.colNames.get(i).getColumnName(), schema1.colIdx.get(schema1.file.getName()+"."+schema1.colNames.get(i)));
     }
     for(int i=0;i<ltuples;i++)
     {
             returnschema.colNames.add(schema2.colNames.get(i));
             returnschema.colIdx.put(schema2.file.getName()+"."+schema2.colNames.get(i).getColumnName(), schema2.colIdx.get(schema2.file.getName()+"."+schema2.colNames.get(i))); 
     }
//      int no_of_joiningAttributess=2;
//       String file1=schema1.file.getName();
//        String file2=schema2.file.getName();
//        int smallcolid;
//        int largecolid;
//       
        
        LeafValue[] cachedtuplerel1;
        LeafValue[] cachedtuplerel2;
        
//        if(joiningAttributes[0].startsWith(file1))
//        {
//            smallcolid=schema1.colIdx.get(joiningAttributes[0]);
//            largecolid=schema2.colIdx.get(joiningAttributes[1]);
//        }
//        else
//        {
//            smallcolid=schema1.colIdx.get(joiningAttributes[1]);
//            largecolid=schema2.colIdx.get(joiningAttributes[0]);
//        }
     if(iterator1)
     {
        hashblock1=this.getNextBlock(rel1);
        iterator1=false;
     }   
       if(!(hashblock1.isEmpty()))
       {
           int i=countrel1;
           while(i<hashblock1.size())
           {
                if(iterator2)
               {
              hashblock2=this.getNextBlock(rel2);
              iterator2=false;
               }
              cachedtuplerel1=hashblock1.get(i);
              System.arraycopy(returntuple, 0, cachedtuplerel1,0, cachedtuplerel1.length);
              if(!(hashblock2.isEmpty()))
              {
                  for(int j=countrel2;j<hashblock2.size();j++)
                  {
                       cachedtuplerel2=hashblock2.get(j);
                       System.arraycopy(returntuple, hashblock1.size(), cachedtuplerel2, 0,cachedtuplerel2.length);
                       countrel2++;
                       Evaluator lt = new Evaluator(returnschema, returntuple);
				try {
					BooleanValue result = (BooleanValue) lt.eval(exp);
					if(result.getValue())
					return returntuple;	
				} catch (SQLException e) {
					System.err.println("Error in Eval");
					e.printStackTrace();	
				}
                       
                  }
                   countrel2=0;
                  iterator2=true;
              }
              else
              {
                  rel2.reset();
                  countrel2=0;
                  iterator2=true;
                  countrel1++;
              i=countrel1;
              }
             
           }
           countrel1=0;
           iterator1=true;
           
           
       }
       else
       {
           rel1.reset();
          return null; 
       }
     
      
     return null;
  }
  public HashMap<String,LeafValue[]> getNextBlock(Operator rel)
  {
      HashMap<String,LeafValue[]> hashblock=new HashMap<String,LeafValue[]>();
      LeafValue[] cachedtuple;
      for(int i=0;i<blocksize;i++)
      {
          cachedtuple=rel.getNext();
          if(cachedtuple!=null)
          {
          hashblock.put(Integer.toString(i), cachedtuple);
          }
          else
          {
              return hashblock;
          }
      }
      return hashblock;
  }
    
}
