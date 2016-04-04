package edu.buffalo.cse562.globals;

import java.util.HashMap;

import net.sf.jsqlparser.expression.LeafValue;

public class GlobalConstants {
	//FILE CONSTANTS
	public static int counthybrid=0;
	public static String PREFIX_DATA_PATH = "./Sanity/data/";
	public static String SWAP_DATA_PATH = null;
	public static final String DOT = ".";
	public static final String SLASH = "/";
	public static final String DAT_SUFFIX = ".tbl";
	public static final String JOIN_FILE = "JOIN_FILE";
	//DATA TYPE NAMES
	public static final String INTEGER_TYPE = "int";
	public static final String LONG_TYPE = "LONG";
	public static final String STRING_TYPE = "STRING";
	public static final String VARCHAR_TYPE = "VARCHAR";
	public static final String CHAR_TYPE = "CHAR";
	public static final String FLOAT_TYPE = "FLOAT";
	public static final String DATE_TYPE = "DATE";
	public static final String DECIMAL_TYPE = "DECIMAL";
	//RA Operators
	public static enum RAOperator{
		SELECT,
		EXTENDED_PROJECT,
		JOIN,
		RELATION,
		GROUP_BY,
		AGGREGATE,
		UNION,
		PRINT,
		ORDER_BY,
		DISTINCT,
		LIMIT,
		HYBRID_HASH,
		BLOCK_NESTED
	};
	//AGGREGATE FUNCTIONS
	public static final String SUM = "SUM";
	public static final String COUNT = "COUNT";
	public static final String AVG = "AVG";
	public static final String MIN = "MIN";
	public static final String MAX = "MAX";
	
	
	
	public static boolean hybriditerator=true;
    public static int counthybrid1=0;
    public static int counthybrid2=0;
    public static int countrel1=0;
    public static int countrel2=0;
    public static boolean hybirdflag=false;
    public static HashMap<String,LeafValue[]> hashbucket=new HashMap<String,LeafValue[]>();
    public static  boolean iterator1=true;
    public static  boolean iterator2=true;
    public static final HashMap<String,LeafValue[]> hybridhashtable=new HashMap<String,LeafValue[]>();
    public static HashMap<String,LeafValue[]> hashblock1=new HashMap<String,LeafValue[]>();
    public static HashMap<String,LeafValue[]> hashblock2=new HashMap<String,LeafValue[]>();

}
