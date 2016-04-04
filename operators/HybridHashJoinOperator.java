/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.buffalo.cse562.operators;


import net.sf.jsqlparser.expression.LeafValue;
import edu.buffalo.cse562.beans.SchemaBean;
import static edu.buffalo.cse562.globals.GlobalConstants.DAT_SUFFIX;
import static edu.buffalo.cse562.globals.GlobalConstants.counthybrid;
import edu.buffalo.cse562.interfaces.Operator;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import javax.management.relation.Relation;

//import edu.buffalo.cse562.operators.XProductOperator;
/**
 *
 * @author dhwajjani
 */
public class HybridHashJoinOperator  implements Operator{
//****************** CALL METHOD HYBRIDJOIN FROM OUTSIDE TO JOIN AND GET ONE TUPLE IN RETURN WHICH IS THE RESULT OF JOIN***************

    FileReaderOperator rel1,rel2;
    int indexrel1=0,indexrel2=0;
    SchemaBean schema1,schema2;
    String[] joiningAttribute;
    LeafValue[] cachedRel1Tuple;
    LeafValue[] cachedRel2Tuple;
    int Nbuckets=35;
    File outputfile;
    BufferedReader output;
    ArrayList<BufferedReader> brrel1;
    ArrayList<SchemaBean> schemarel1;
    ArrayList<SchemaBean> schemarel2;
    ArrayList<BufferedReader> brrel2;
    ArrayList<BufferedWriter> bwrel1;
    ArrayList<BufferedWriter> bwrel2;
    int hashSize = 1499;
    int hashSize2 = 401;
    int count=0;
    File swapDirectory;
    ArrayList<Operator> operatorrel1;
    ArrayList<Operator> operatorrel2;
    HashMap<Integer,LeafValue[]> jointable=new HashMap<Integer,LeafValue[]>();
    
    public HybridHashJoinOperator(FileReaderOperator rel1,SchemaBean schema1,FileReaderOperator rel2,SchemaBean schema2,String[] joiningAttribute,File swapDirectory) throws IOException
    {
        this.joiningAttribute=joiningAttribute;
        this.rel1=rel1;
        this.rel2=rel2;
        this.schema1=schema1;
        this.schema2=schema2;
        this.swapDirectory=swapDirectory;
        String file1,file2;
        System.out.println("Filename is "+schema1.file.getName());
        file1=schema1.file.getName();
        file2=schema2.file.getName();
        System.out.println("File1 is "+file1);
        System.out.println("File1 is "+file2);
        System.out.println("Joining attributes are "+joiningAttribute[0]+" "+joiningAttribute[1]);
        if(joiningAttribute[0].startsWith(file1))
        {
             indexrel1=schema1.colIdx.get(joiningAttribute[0]);
             indexrel2=schema2.colIdx.get(joiningAttribute[1]);
        }
        else        	
        {
        	 System.out.println("Schema1 is "+schema1.colIdx);
        	 System.out.println("Schema2 is "+schema2.colIdx);
        	 System.out.println("Joining attribute is "+joiningAttribute[0]);
             indexrel1=schema1.colIdx.get(joiningAttribute[1]);
             indexrel2=schema2.colIdx.get(joiningAttribute[0]);
        }
        brrel1=new ArrayList<BufferedReader>(hashSize);
        brrel2=new ArrayList<BufferedReader>(hashSize);
        bwrel1=new ArrayList<BufferedWriter>(hashSize);
        bwrel2=new ArrayList<BufferedWriter>(hashSize);
        schemarel1=new ArrayList<SchemaBean>(hashSize);
        schemarel2=new ArrayList<SchemaBean>(hashSize);
        operatorrel1=new ArrayList<Operator>(hashSize);
        operatorrel2=new ArrayList<Operator>(hashSize);
        if(swapDirectory==null)
        {
            HashMap<Integer,ArrayList<LeafValue[]>> build;
           build= build();
            probe( build);
        }
        else
        {
         tempFiles(bwrel1,file1);
         tempFiles(bwrel2,file2);
         System.out.println("Relation is "+rel1.schema.file.getAbsolutePath());
         buildphase(rel1,indexrel1,bwrel1);
         buildphase(rel2,indexrel2,bwrel2);
        
         Writerclose(bwrel1);
         Writerclose(bwrel2);
          
          readers(brrel1,file1,schema1,schemarel1);
          readers(brrel2,file2,schema2,schemarel2);
          
           outputfile=File.createTempFile("Result", "tmp", swapDirectory);
           
           hybirdJoin();
           
           Readerclose(brrel1);
           Readerclose(brrel2);
           
           cleanup();
        }
    }
    public void probe( HashMap<Integer,ArrayList<LeafValue[]>> build) throws IOException
    {
        int count=0;
        cachedRel2Tuple=rel2.getNext();
       
        LeafValue[] temptuple;
        BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputfile, true));
        do
        {
             int hashkey=cachedRel2Tuple[indexrel2].hashCode();
                if(hashkey<0)
                   {
                     hashkey=hashkey*-1;
                   }
             int bucket_no=hashkey%hashSize2;
            if(build.containsKey(bucket_no))
            {
                ArrayList<LeafValue[]> templist=new ArrayList<LeafValue[]>();
                templist=build.get(bucket_no);
                for(int i=0;i<templist.size();i++)
                {
                    temptuple=templist.get(i);
//                    String str1=StringRecord(temptuple);
//                    String str2=StringRecord(cachedRel2Tuple);
                    LeafValue[] temp=null;
                    System.arraycopy(temptuple, 0, temp, 0, temptuple.length);
                    System.arraycopy(cachedRel2Tuple,0, temp, temptuple.length, cachedRel2Tuple.length);
                     jointable.put(count, temp);
                     count++;
                }
               
            }
             cachedRel2Tuple=rel2.getNext();
        }while(cachedRel2Tuple!=null);
    }
    public  HashMap<Integer,ArrayList<LeafValue[]>> build()
    {
         HashMap<Integer,ArrayList<LeafValue[]>> build=new HashMap<Integer,ArrayList<LeafValue[]>>();
           
            LeafValue[] cachedTuplerel1=rel1.getNext();
            do
            {
                int hashkey=cachedTuplerel1[indexrel1].hashCode();
                if(hashkey<0)
                   {
                     hashkey=hashkey*-1;
                   }
                int bucket_no=hashkey%hashSize2;
                if(build.containsKey(bucket_no))
                {
                    build.get(bucket_no).add(cachedTuplerel1);
                }
                else
                {
                    ArrayList<LeafValue[]> newlist=new ArrayList<LeafValue[]>();
                    newlist.add(cachedTuplerel1);
                    build.put(bucket_no, newlist);
                }
               
                
            }while(cachedTuplerel1!=null);
        return build;
    }
    
    public int hashCode() {
        return super.hashCode();
    }

     //creates hash table for the smaller sized table in hybrid hash joins
   
    public void cleanup() {
        for (int i = 0; i < hashSize; i++) {
            File file = new File(swapDirectory.getAbsolutePath() + "/R" + i+DAT_SUFFIX);
            file.delete();
            file = new File(swapDirectory.getAbsolutePath() + "/S" + i+DAT_SUFFIX);
            file.delete();
        }
    }
    public void hybirdJoin() throws IOException
    {
       
       
         BufferedWriter resultWriter = new BufferedWriter(new FileWriter(outputfile, true));
//         for(int i=0;i<hashSize;i++)
//        {
//            Operator temp=operatorrel1.get(i);
//            temp=new FileReaderOperator(schemarel1.get(i));
//            operatorrel1.add(temp);
//            
//            Operator temp1=operatorrel2.get(i);
//            temp1=new FileReaderOperator(schemarel2.get(i));
//            operatorrel1.add(temp1);
//        }
//         
        HashMap<Integer,ArrayList<LeafValue[]>> build=new HashMap<Integer,ArrayList<LeafValue[]>>();
        for(int i=0;i<hashSize;i++)
        {
            //Operator rel1=operatorrel1.get(i);
            rel1=new FileReaderOperator(schemarel1.get(i));
            //System.out.println("Relation1 is "+rel1.schema.file.getPath());
            LeafValue[] cachedTuplerel1=rel1.getNext();
            if(cachedTuplerel1==null)
            	{
            		rel1.close();
            		continue;
            	}
            do
            {
            	
                int hashkey=cachedTuplerel1[indexrel1].hashCode();
                if(hashkey<0)
                   {
                     hashkey=hashkey*-1;
                   }
                int bucket_no=hashkey%hashSize2;
                if(build.containsKey(bucket_no))
                {
                    build.get(bucket_no).add(cachedTuplerel1);
                }
                else
                {
                    ArrayList<LeafValue[]> newlist=new ArrayList<LeafValue[]>();
                    newlist.add(cachedTuplerel1);
                    build.put(bucket_no, newlist);
                }
                cachedTuplerel1=rel1.getNext();
                
            }while(cachedTuplerel1!=null);
            rel1.close();
        } 
        for(int i=0;i<hashSize;i++)
        {
           // Operator rel2=operatorrel2.get(i);
            rel2=new FileReaderOperator(schemarel2.get(i));
            LeafValue[] cachedTuplerel2=rel2.getNext();
            if(cachedTuplerel2==null)
        	{
        		rel2.close();
        		continue;
        	}
            do
            {
                int hashkey=cachedTuplerel2[indexrel2].hashCode();
                if(hashkey<0)
                   {
                     hashkey=hashkey*-1;
                   }
                int bucket_no=hashkey%hashSize2;
                if(build.containsKey(bucket_no))
                {  
                    ArrayList<LeafValue[]> templist=build.get(bucket_no);
                    for(int j=0;j<templist.size();j++)
                    {
                        LeafValue[] temptuple=templist.get(j);
                        String s=StringRecord(temptuple);
                        String s1=StringRecord(cachedTuplerel2);
                       resultWriter.write(s+"|"+s1);
                       resultWriter.newLine();
                       count++;
                    }
                }
               cachedTuplerel2= rel2.getNext();
            }while(cachedTuplerel2!=null);
            rel2.close();
        }
        
        
    }
    
    
    
    public String StringRecord(LeafValue[] tuple)
    {
        int size=tuple.length;
        StringBuilder sb=new StringBuilder();
        for(int i=0;i<size;i++)
        {
            if(i==size-1)
            {
                sb.append(tuple[i].toString());
            }
            else
            {
                sb.append(tuple[i].toString()+"|");
            }
        }
        return sb.toString();
    }
    public void tempFiles(ArrayList<BufferedWriter> bw,String tablename) throws IOException
    {
        for(int i=0;i<hashSize;i++)
        {
        	 //System.out.println("Swap directory path is "+swapDirectory.getPath()+"/"+tablename+i+DAT_SUFFIX);
             BufferedWriter temp=new BufferedWriter(new FileWriter(swapDirectory.getPath()+"/"+tablename+i+DAT_SUFFIX,true));
             bw.add(temp);
        }
    }
    
    
    
    public void buildphase(FileReaderOperator rel,int colid,ArrayList<BufferedWriter> bw) throws IOException
    {
        LeafValue[] tuple;      
        while((tuple=rel.getNext())!=null)
        {        	
            int hashcode=tuple[colid].hashCode();
            if(hashcode<0)
            {
                hashcode=hashcode*-1;
            }
            int bucketno=(hashcode)%hashSize;
            BufferedWriter temp=bw.get(bucketno);
            String record=StringRecord(tuple);
            temp.write(record);
            temp.newLine();
        }
    }
    
    public void Writerclose(ArrayList<BufferedWriter> bw) throws IOException
    {
        for(int i=0;i<bw.size();i++)
        {
            bw.get(i).close();
        }
    }
    public void Readerclose(ArrayList<BufferedReader> br) throws IOException
    {
         for(int i=0;i<br.size();i++)
        {
            br.get(i).close();
        }
    }
    public void readers(ArrayList<BufferedReader> br,String tablename,SchemaBean schema,ArrayList<SchemaBean> schemarel) throws FileNotFoundException
    {
        SchemaBean tempschema= new SchemaBean();
        for(int i=0;i<hashSize;i++)
        {
            BufferedReader temp=new BufferedReader(new FileReader(swapDirectory.getPath()+"/"+tablename+i+DAT_SUFFIX));
            br.add(temp);
            tempschema.colNames=schema.colNames;
            tempschema.colIdx=schema.colIdx;
            tempschema.file=new File(swapDirectory.getPath()+"/"+tablename+i+DAT_SUFFIX);
            schemarel.add(tempschema);
        }
    }
    
    
    public LeafValue[] hybridHashiterator()
    {
        if(swapDirectory!=null)
        {
       SchemaBean joinschema=null;
       LeafValue[] cachedtuple=null;
       for(int i=0;i<schema1.colNames.size();i++)
       {
           joinschema.colNames.add(schema1.colNames.get(i));
           joinschema.colIdx.put(schema1.colNames.get(i).getColumnName(), schema1.colIdx.get(schema1.colNames.get(i).getColumnName()));
       }
        for(int i=0;i<schema2.colNames.size();i++)
       {
           joinschema.colNames.add(schema2.colNames.get(i));
           joinschema.colIdx.put(schema2.colNames.get(i).getColumnName(), schema2.colIdx.get(schema2.colNames.get(i).getColumnName()));
       }
        joinschema.file=outputfile;
        FileReaderOperator output;
        output=new FileReaderOperator(joinschema);
        cachedtuple=output.getNext();
       if(cachedtuple==null)
       {
    	   output.reset();
    	   //System.exit(0);
    	   return null;
       }
       else
       {
        return cachedtuple;
       }
        
        }
        else
        {
           if(counthybrid>jointable.size())
           {
               counthybrid=0;
               return null;
           }
           else
           {
           LeafValue[] temp= jointable.get(counthybrid);
            counthybrid++;
            return temp;
           }
        }
    }
	@Override
	public LeafValue[] getNext() {
		// TODO Auto-generated method stub
		return this.hybridHashiterator();
	}
	@Override
	public void reset() {
		// TODO Auto-generated method stub
		
	}

}

