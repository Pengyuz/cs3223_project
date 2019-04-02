package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class ExternalSort extends Operator {
    int runNo;
    int batchSize;//number of tuples per batch

    Schema baseSchema;//schema of original table
    String sourceFile;//given filename
    int numOfBuffers;//number of avaliable buffer
    Vector<Attribute> attrset;//attributes used to compare
    int[] attrIndex;//index of comparator
    Vector<String> fileNames = new Vector<>();// stores the names of temp files for sorted run names

    ObjectInputStream in;
    ObjectOutputStream out;

    boolean eos;
    public ExternalSort(String filename,Schema sc, Vector as,int type,int numBuff){
        super(type);
       this.baseSchema = sc;
       this.sourceFile = filename;
       this.attrset =as;
       this.numOfBuffers = numBuff;
       attrIndex = new int[as.size()];
        /**Find the indexes of the attributes used to
         *  sort the runs.
         */
       for(int i=0;i<attrset.size();i++){
           Attribute a = attrset.get(i);
           int index = baseSchema.indexOf(a);
           attrIndex[i] = index;
       }

    }
/** dosort is called by Groupby Operator and SortMergeJoin Operator
 * to sort tables.
 */
    public void doSort(){
        Boolean eof=false;
        generateSortedRuns(sourceFile); // thsi step generate sorted run
        String finalFile = Merge(fileNames);//this step merge the sorted runs
        /**The following steps read from the temp file that sotres all the
         * sorted result, and write them into the source file provided by
         * External Operator and Groupby Operator.
         *
         */
        try{
            in = new ObjectInputStream(new FileInputStream(finalFile));
            out = new ObjectOutputStream(new FileOutputStream(sourceFile));
        }catch (Exception e){
            System.out.println(e.getMessage()+"50");
        }

        while(!eof){
            try {

                Batch present = (Batch) in.readObject();
                out.writeObject(present);
            }catch (EOFException e){
                eof=true;
            }catch (Exception e){
                System.out.println(e.getMessage());
                System.exit(1);
            }
        }
        try{
            in.close();
            out.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        File f = new File(finalFile);
        f.delete();
    }

    /**
     * Most of the functionality of doSortForDistinct are similar to doSort.
     * It just deals with the case that Select Distinct *, which doesn't have
     * a attribute list but need to sort based on all the attributes.
     */
    public void doSortForDistinct(){
        Boolean eof=false;
        if(attrIndex.length==0){
            attrIndex = new int[baseSchema.getNumCols()];
            for(int i=0;i<attrIndex.length;i++){
                attrIndex[i] = i;
            }
        }
        generateSortedRuns(sourceFile);
        String finalFile = Merge(fileNames);
        try{
            in = new ObjectInputStream(new FileInputStream(finalFile));
            out = new ObjectOutputStream(new FileOutputStream(sourceFile));
        }catch (Exception e){
            System.out.println(e.getMessage()+"50");
        }

        while(!eof){
            try {

                Batch present = (Batch) in.readObject();
                out.writeObject(present);
            }catch (EOFException e){
                eof=true;
            }catch (Exception e){
                System.out.println(e.getMessage());
                System.exit(1);
            }
        }
        try{
            in.close();
            out.close();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
        File f = new File(finalFile);
        f.delete();
    }

    /**
     * Merge takes in the file names of all the sorted run, dive them
     * into number of buffer - 1 per group, call helper function to merge
     * each group and get temp file of longer runs, then call itself to merge
     * these longer runs. Recursive do this until all tuple are write to one
     * file.
     * @param runs
     * @return
     */
    private String Merge(Vector<String> runs){

        int inputNum = numOfBuffers-1;
        int outputNum = 1;
        if(runs.size()==1){
            return runs.get(0);
        }else{
            Vector<String> newRuns= new Vector<>();
            Vector<String> thisRun = new Vector<>();
            for(int i=0;i<runs.size();i++){
                thisRun.add(runs.get(i));
                if(thisRun.size()==inputNum||i==runs.size()-1){
                    String newRun = helper(thisRun);
                    newRuns.add(newRun);
                    thisRun = new Vector<>();
                }
            }
            for(String s:runs){
                File f =new File(s);
                f.delete();
            }
            return Merge(newRuns);
        }

    }

    /**
     * helper takes in maximum  B-1 sorted runs, merge them  into
     * one run.
     * @param f
     * @return
     */
   private String helper(Vector<String> f){
        int inputNum = f.size();
        if(inputNum == 1){
            return f.get(0);
        }
        int outputNum = 1;
        Batch[] inputBuffers = new Batch[f.size()];
        Batch outputBuffer = new Batch(Batch.getPageSize()/baseSchema.getTupleSize());
        Vector<ObjectInputStream> inputStreams= new Vector<>();
        String file = "EXTTemp-"+String.valueOf(runNo);
        runNo++;
        try{
             out = new ObjectOutputStream(new FileOutputStream(file));
        }catch (Exception e){
            System.out.println(e.getMessage()+"104");
        }

        for(String s:f){
            try {
                ObjectInputStream current = new ObjectInputStream(new FileInputStream(s));
                inputStreams.add(current);
            }catch (Exception e){
                System.out.println(e.getMessage()+"112");
            }
        }

        StreamManager sm = new StreamManager(inputStreams);
        int index=-1;
        while(!sm.allEOF()||!outputBuffer.isEmpty()||index!=-1){
            try{
                if(outputBuffer.isFull()||(sm.allEOF()&&index==-1&&!outputBuffer.isEmpty())){
                    out.writeObject(outputBuffer);
                    outputBuffer = new Batch(Batch.getPageSize()/baseSchema.getTupleSize());
                }
            }catch (Exception e){
                System.out.println(e.getMessage()+"124");
                System.exit(1);
            }

            for(int i=0;i<f.size();i++) {
                if (inputBuffers[i] == null||inputBuffers[i].isEmpty()) {
                    inputBuffers[i] =sm.getNext(i);
                }
            }
            Tuple t = new Tuple(new Vector());
            index = -1;
            for(int i = 0;i<f.size();i++){
                if(inputBuffers[i] != null && !inputBuffers[i].isEmpty()){
                    t=inputBuffers[i].elementAt(0);
                    index = i;
                    break;
                }
            }
            if(index==-1){
                continue;
            }
            for(int i = 0;i<f.size();i++){
                if(inputBuffers[i] != null &&!inputBuffers[i].isEmpty()){
                    if(compare(t,inputBuffers[i].elementAt(0))>=0){
                        t = inputBuffers[i].elementAt(0);
                        index=i;
                    }
                }
            }
            outputBuffer.add(t);
            inputBuffers[index].remove(0);

        }
        try{
            out.close();
            for(ObjectInputStream obj: inputStreams){
                obj.close();
            }
        }catch (Exception e){
            System.out.println(e.getMessage()+"163");
        }
        return file;
    }

    /** takes in two tuples, determine their order
     * according to the attrIndex.
     * */
    private int compare(Tuple t1, Tuple t2) {
        for (int i = 0; i < attrIndex.length; i++) {
            int a = Tuple.compareTuples(t1, t2, attrIndex[i]);
            if (a != 0) {
                return a;
            }
        }
        return 0;
    }

    /**
     *  Takes in the source file and generate sorted runs,
     *  the sorted runs are stored in a temp file per run.
     * @param filename
     */
    private void generateSortedRuns(String filename){
        runNo=0;
        int tuplesize = baseSchema.getTupleSize();
        batchSize= Batch.getPageSize()/tuplesize;
        int numOfTuples = batchSize * numOfBuffers;
        Vector<Tuple> tuplesInRun;
        Vector<Batch> batches = new Vector<>();
        TupleComparator  tc = new TupleComparator(attrIndex);


        eos = false;

        try {
            in = new ObjectInputStream(new FileInputStream(filename));
        } catch (Exception e) {
            System.err.println(" Error reading " + filename);
        }
        while(!eos || batches.size()!=0){
            try {
                if(batches.size() == numOfBuffers){
                    //convert batches to tuples

                    tuplesInRun = batchToTuple(batches);

                    Collections.sort(tuplesInRun,tc);
                    //convert tuple to batches
                    Vector<Batch> thisRun = tupleToBatch(tuplesInRun);
                    //write batches to file
                    filename = "EXTTemp-"+String.valueOf(runNo);
                    fileNames.add(filename);
                    out = new ObjectOutputStream(new FileOutputStream(filename));
                    for(Batch b:thisRun){
                        out.writeObject(b);
                    }
                    out.close();
                    runNo++;
                    batches = new Vector<>();
                }else{
                    try{
                        Batch present = (Batch) in.readObject();
                        batches.add(present);
                    }catch (EOFException eof){
                        eos = true;
                    }

                }
                if(eos){
                    //convert batches to tuples
                    tuplesInRun = batchToTuple(batches);
                    Collections.sort(tuplesInRun,tc);
                    //convert tuple to batches
                    Vector<Batch> thisRun = tupleToBatch(tuplesInRun);
                    //write batches to file
                    filename = "EXTTemp-"+String.valueOf(runNo);
                    fileNames.add(filename);
                    out = new ObjectOutputStream(new FileOutputStream(filename));
                    for(Batch b:thisRun){
                        out.writeObject(b);
                    }
                    out.close();
                    runNo++;
                    batches = new Vector<>();
                }
//                filename = "BLJTemp-"+String.valueOf(runNo);
//                fileNames.add(filename);
//                out = new ObjectOutputStream(new FileOutputStream(filename));
//                for(Tuple tuple:tuplesInRun){
//                    out.wri
//                }
            } catch(ClassNotFoundException cnf){
                System.out.println("Scan:Class not found for reading file  "+filename);
                System.exit(1);
            } catch (IOException e) {
                System.out.println("Scan:Error reading " + filename);
                System.exit(1);
           } //catch (Exception e){
//                System.out.println(e.getStackTrace()+"235");
//                System.exit(1);
//            }
        }
        try{
            in.close();
        }catch (Exception e){
            System.out.println(e.getMessage()+"243");
            System.exit(1);
        }


    }

    /**
     * Read batches from file.
     * @param file
     * @return
     */
    private Vector<Batch> fileToBatch(String file){
        boolean EOF=false;
        Vector<Batch> result = new Vector<>();
        try{
            in = new ObjectInputStream(new FileInputStream(file));
            while(!EOF){
                Batch present = (Batch) in.readObject();
                result.add(present);
            }
            in.close();

        }catch (EOFException eof){
            EOF =true;
            System.out.println("External sort eof");
        }catch (FileNotFoundException fnf){
            System.out.println("External sort fnf");
            System.exit(1);
        }catch (IOException ioe){
            System.out.println("External Sort" + ioe.getMessage());
            System.exit(1);
        }catch (Exception e){
            System.out.println(e.getMessage()+"270");
            System.exit(1);
        }

        return result;
    }

    /**
     * convert batches to tuples in one vector, in order to
     * facilitate future sorting.
     * @param batches
     * @return
     */
    private Vector<Tuple> batchToTuple(Vector<Batch> batches){
        Vector<Tuple> tuples = new Vector<>();
        for(Batch batch:batches){

            for(int i = 0;i<batch.size();i++){
                tuples.add(batch.elementAt(i));
            }
        }
        return tuples;
    }

    /**
     * convert sorted tuples to batches.
     * @param tuples
     * @return
     */
    private Vector<Batch> tupleToBatch(Vector<Tuple> tuples){
        Vector<Batch> batches = new Vector<>();
        Batch present = new Batch(Batch.getPageSize()/baseSchema.getTupleSize());
        for(Tuple tuple:tuples){
            if(present.isFull()){
                batches.add(present);
                present = new Batch(Batch.getPageSize()/baseSchema.getTupleSize());
                present.add(tuple);
            }else{
                present.add(tuple);
            }
        }
        if(!present.isEmpty()){
            batches.add(present);
        }
        return batches;
    }

    /**
     * Comparator used for collections.sort.
     */
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

    /**
     * Manage the B-1 input stream during the merging period,
     * which is called by helper.
     */
    class StreamManager{
        private Vector<ObjectInputStream> allStreams;
        private Vector<Integer> cursors;
        private int size;
        StreamManager(Vector<ObjectInputStream> streams){
            allStreams = streams;
            size = allStreams.size();
            cursors = new Vector<>();
            for(int i = 0;i<size;i++){
                cursors.add(0);
            }
        }
        public Batch getNext(int i){
            Batch next = new Batch(Batch.getPageSize()/baseSchema.getTupleSize());
            if(i>=size){
                return null;
            }
            ObjectInputStream current  = allStreams.get(i);
            if(cursors.get(i)==2){
                return null;
            }else if(cursors.get(i) ==0 || cursors.get(i)==1){
                try{
                    next = (Batch) current.readObject();
                    cursors.set(i,1);
                }catch (EOFException e){
                    cursors.set(i,2);
                    return null;
                }catch (Exception e){
                    System.out.println(e.getMessage()+"343");
                    System.exit(1);
                }
            }
            return next;
        }
        public int NextAvaliable(){
            for(int i=0;i<size;i++){
                if(cursors.get(i)==0){
                    return i;
                }
            }
            return -1;
        }
        public Boolean allEOF(){
            boolean flag = true;
            for(int i=0;i<size;i++){
                if(cursors.get(i)==0 || cursors.get(i)==1){
                    flag =false;
                    break;
                }
            }
            return flag;
        }
    }

}
