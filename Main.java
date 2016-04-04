package edu.buffalo.cse562;

import net.sf.jsqlparser.parser.ParseException;
import edu.buffalo.cse562.globals.GlobalConstants;
import edu.buffalo.cse562.operators.SqlReader;

public class Main {

	public static void main(String[] args) {
		//GlobalConstants.PREFIX_DATA_PATH = GlobalConstants.DOT + GlobalConstants.SLASH +args[1] + GlobalConstants.SLASH;
		//GlobalConstants.PREFIX_DATA_PATH = "./Sanity_Check_Examples/data/";
		GlobalConstants.PREFIX_DATA_PATH = "./Sanity_Check_Examples/tbl/";
		String sqlFilename ;//= args[2];
		//sqlFilename = "./Sanity_Check_Examples/GBAGG01.SQL";
		sqlFilename = "./Sanity_Check_Examples/tpch_schemas3.sql";
		
		/*String argument = new String();
		sqlFilename = GlobalConstants.DOT+GlobalConstants.SLASH+args[args.length-1];
		for (int i = 0; i < args.length; i++) {
			argument=args[i];
			if(argument.equals("--data"))
			{
				System.out.println("The data directory is "+args[i+1]);
				GlobalConstants.PREFIX_DATA_PATH=GlobalConstants.DOT+GlobalConstants.SLASH+args[i+1]+GlobalConstants.SLASH;
				System.out.println(GlobalConstants.PREFIX_DATA_PATH);
			}
			if(argument.equals("--swap"))
			{
				System.out.println("The swap directory is "+args[i+1]);
				GlobalConstants.SWAP_DATA_PATH=GlobalConstants.DOT+GlobalConstants.SLASH+args[i+1]+GlobalConstants.SLASH;
				System.out.println(GlobalConstants.SWAP_DATA_PATH);
			}
		}
		
		System.out.println("SQL filename is "+sqlFilename);
		
		System.exit(0);*/
		GlobalConstants.SWAP_DATA_PATH="./Sanity_Check_Examples/Swap/";
				
		SqlReader sqlReader = new SqlReader();
		try {
			sqlReader.parseAndExecute(sqlFilename);
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		

		

	}

}
