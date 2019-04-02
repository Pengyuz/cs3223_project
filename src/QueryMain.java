/** This is main driver program of the query processor **/

import java.io.*;

import qp.utils.*;
import qp.operators.*;
import qp.optimizer.*;
import qp.parser.*;



public class QueryMain{

    static PrintWriter out;
    static int numAtts;

    public static void main(String[] args){
	args = new String[3];
	args[0] = "querytest";
	args[1] = "outGBRo.txt";
	args[2]  ="outGBDP.txt";
	if(args.length !=3){
	    System.out.println("usage: java QueryMain <queryfilename> <resultfile>");
	    System.exit(1);
	}


	/** Enter the number of bytes per page **/

	System.out.println("enter the number of bytes per page");
	    BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
	    String temp;
	    try {
		temp = in.readLine();
		int pagesize = Integer.parseInt(temp);
		Batch.setPageSize(pagesize);
		} catch (Exception e) {
		e.printStackTrace();
	 }




	String queryfile = args[0];
	String resultfile = args[1];
	String resultfile1 = args[2];
	FileInputStream source = null;
	try{
	   source = new FileInputStream(queryfile);
	}catch(FileNotFoundException ff){
	    System.out.println("File not found: "+queryfile);
	    System.exit(1);
	}


	/** scan the query **/

	Scaner sc = new Scaner(source);
	parser p = new parser();
	p.setScanner(sc);


	/** parse the query **/

	try{
	    p.parse();
	}catch(Exception e){
	    System.out.println("Exception occured while parsing");
	    System.exit(1);
	}

	/** SQLQuery is the result of the parsing **/

	SQLQuery sqlquery = p.getSQLQuery();
	int numJoin = sqlquery.getNumJoin();
	int numGroupby = (sqlquery.getGroupByList()==null? 0 : sqlquery.getGroupByList().size());


	/** If there are joins then assigns buffers to each join operator
	    while preparing the plan
	**/
	/** As buffer manager is not implemented, just input the number of
	    buffers available
	**/


	if(numJoin !=0 || numGroupby!=0){
	    System.out.println("enter the number of buffers available");

	    try {
		temp = in.readLine();
		int numBuff = Integer.parseInt(temp);
		BufferManager bm = new BufferManager(numBuff,numJoin);
	    } catch (Exception e) {
		e.printStackTrace();
	 }

		/** Let check the number of buffers available is enough or not **/
		if (numJoin != 0) {
			int numBuff = BufferManager.getBuffersPerJoin();
			if(numJoin>0 && numBuff<3){
				System.out.println("Minimum 3 buffers are required per a join operator ");
				System.exit(1);
			}
		}

	}







/** This part is used When some random initial plan is required instead of comple optimized plan **/
/**

	RandomInitialPlan rip = new RandomInitialPlan(sqlquery);
	Operator logicalroot = rip.prepareInitialPlan();
	PlanCost pc = new PlanCost();
	int initCost = pc.getCost(logicalroot);
	Debug.PPrint(logicalroot);
	System.out.print("   "+initCost);
	System.out.println();
**/


	/** Use random Optimization algorithm to get a random optimized
	    execution plan
**/

	RandomOptimizer ro = new RandomOptimizer(sqlquery);
	Operator logicalroot = ro.getOptimizedPlan();
	if(logicalroot==null){
		System.out.println("root is null");
		System.exit(1);
	}

	DPOptimizer  dp = new DPOptimizer(sqlquery);
		dp.prepareInitialPlan();
	Operator dpRoot = dp.getOptimizedPlan();
	if(dpRoot == null){
		System.out.println("root is null");
		System.exit(1);
	}
		Operator DProot = DPOptimizer.makeExecPlan(dpRoot);



	/** preparing the execution plan **/

	Operator Roroot = RandomOptimizer.makeExecPlan(logicalroot);

/** Print final Plan **/
	System.out.println("----------------------Execution Plan for Rondom----------------");
	Debug.PPrint(Roroot);
	System.out.println();

		System.out.println("----------------------Execution Plan for DP----------------");
		Debug.PPrint(DProot);
		System.out.println();


/** Ask user whether to continue execution of the program **/

System.out.println("enter 1 to continue, 0 to abort ");


	    try {
		temp = in.readLine();
		int flag = Integer.parseInt(temp);
		if(flag==0){
			System.exit(1);
		}

	    } catch (Exception e) {
		e.printStackTrace();
	 }

long starttime1 = System.currentTimeMillis();



	if(Roroot.open()==false){
	    System.out.println("Root: Error in opening of root");
	    System.exit(1);
	}
	try{
	    out = new PrintWriter(new BufferedWriter(new FileWriter(resultfile)));
	}catch(IOException io){
	    System.out.println("QueryMain:error in opening result file: "+resultfile);
	    System.exit(1);
	}




	/** print the schema of the result **/
	Schema schema = Roroot.getSchema();
	numAtts = schema.getNumCols();
	printSchema(schema);
	Batch resultbatch;


	/** print each tuple in the result **/


	while((resultbatch=Roroot.next())!=null){
	    for(int i=0;i<resultbatch.size();i++){
		printTuple(resultbatch.elementAt(i));
	    }
	}
		Roroot.close();
	out.close();

long endtime1 = System.currentTimeMillis();
double executiontime1 = (endtime1 - starttime1)/1000.0;
System.out.println("Execution time = "+ executiontime1);
////-------------------------------------------------------------------
		long starttime2 = System.currentTimeMillis();



		if(DProot.open()==false){
			System.out.println("Root: Error in opening of root");
			System.exit(1);
		}
		try{
			out = new PrintWriter(new BufferedWriter(new FileWriter(resultfile1)));
		}catch(IOException io){
			System.out.println("QueryMain:error in opening result file: "+resultfile1);
			System.exit(1);
		}




		/** print the schema of the result **/
		Schema schemadp = DProot.getSchema();
		numAtts = schemadp.getNumCols();
		printSchema(schemadp);
		Batch resultbatchDp;


		/** print each tuple in the result **/


		while((resultbatchDp=DProot.next())!=null){
			for(int i=0;i<resultbatchDp.size();i++){
				printTuple(resultbatchDp.elementAt(i));
			}
		}
		DProot.close();
		out.close();

		long endtime2 = System.currentTimeMillis();
		double executiontime2 = (endtime2 - starttime2)/1000.0;
		System.out.println("Execution time = "+ executiontime2);


    }


    protected static void printTuple(Tuple t){
	for(int i=0;i<numAtts;i++){
	    Object data = t.dataAt(i);
	    if(data instanceof Integer){
	       out.print(((Integer)data).intValue()+"\t");
	    }else if(data instanceof Float){
		out.print(((Float)data).floatValue()+"\t");
	    }else{
		out.print(((String)data)+"\t");
	    }
	}
	out.println();
    }

    protected static void printSchema(Schema schema){
	for(int i=0;i<numAtts;i++){
	    Attribute attr = schema.getAttribute(i);
	    out.print(attr.getTabName()+"."+attr.getColName()+"  ");
	}
	out.println();
    }

}







