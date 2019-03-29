package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;
public class ExternalSort extends Operator {
    int runNo;
    int batchSize;//number of tuples per batch

    Schema baseSchema;
    String sourceFile;
    int numOfBuffers;
    Vector<Attribute> attrset;
    int[] attrIndex;
    Vector<Vector<Tuple>> runs;
    Vector<String> fileNames;

    ObjectInputStream in;

    boolean eos;
    public ExternalSort(String filename,Schema sc, Vector as,int type,int numBuff){
        super(type);
       this.baseSchema = sc;
       this.sourceFile = filename;
       this.attrset =as;
       this.numOfBuffers = numBuff;
    }

    public boolean open(){

        runNo = 0;
        int tuplesize = base.getSchema().getTupleSize();
        batchSize = Batch.getPageSize()/tuplesize;
        String fileName = "EStemp-"+String.valueOf(runNo);

        Schema sc = base.getSchema();
        attrIndex = new int[attrset.size()];
        for(int i = 0;i<attrset.size();i++){
            Attribute attr = (Attribute) attrset.elementAt(i);
            int index = sc.indexOf(attr);
            attrIndex[i]=index;
        }
        if(!base.open()){
            return false;
        }else{
            return true;
        }
    }

    private void generateSortedRuns( String filename){
        int tuplesize = schema.getTupleSize();
        batchSize= Batch.getPageSize()/tuplesize;
        int numOfTuples = batchSize * numOfBuffers;
        Vector<Tuple> tuplesInRun;

        //System.out.println("Scan:----------Scanning:"+tabname);
        eos = false;

        try {
            in = new ObjectInputStream(new FileInputStream(filename));
        } catch (Exception e) {
            System.err.println(" Error reading " + filename);
        }
        while(!eos){
            try {
                tuplesInRun = new Vector<>(numOfTuples);
                for (int i = 0; i < numOfTuples; i++) {
                    Tuple data = (Tuple) in.readObject();
                    tuplesInRun.add(data);
                }
                TupleComparator  tc = new TupleComparator(attrIndex);
                Collections.sort(tuplesInRun,tc);
                runs.add(tuplesInRun);
            } catch(ClassNotFoundException cnf){
                System.err.println("Scan:Class not found for reading file  "+filename);
                System.exit(1);
            }catch (EOFException EOF) {
                /** At this point incomplete page is sent and at next call it considered
                 ** as end of file
                 **/
                eos=true;
            } catch (IOException e) {
                System.err.println("Scan:Error reading " + filename);
                System.exit(1);
            }
        }

    }
    private Vector<Tuple> sort(Vector<Tuple> input){
    Comparator<Tuple> tupleComparator = new Comparator<Tuple>( ) {
        @Override
        public int compare(Tuple o1, Tuple o2) {
            return 0;
        }
    }
    }

    class TupleComparator implements Comparator<Tuple> {

       private int[] attIndex;
        TupleComparator(int[] att) {
          this.attIndex = att;
        }
        @Override
        public int compare(Tuple t1, Tuple t2) {
            for (int i = 0; i < attIndex.length; i++) {
                int a = Tuple.compareTuples(t1, t2, attIndex[i]);
                if (a != 0) {
                    return a;
                }
            }
            return 0;
        }
    }

}
