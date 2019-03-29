package qp.operators;

import com.sun.javafx.collections.MappingChange;
import qp.utils.*;

import javax.print.DocFlavor;
import javax.swing.plaf.basic.BasicTableHeaderUI;
import java.awt.*;
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
    Vector<String> fileNames = new Vector<>();//sorted run names

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
       for(int i=0;i<attrset.size();i++){
           Attribute a = attrset.get(i);
           int index = baseSchema.indexOf(a);
           attrIndex[i] = index;

       }
    }

    public void doSort(){
        Boolean eof=false;
        generateSortedRuns(sourceFile);
        for(String s:fileNames){
            System.out.println(s);
        }
        String finalFile = Merge(fileNames);
        try{
            in = new ObjectInputStream(new FileInputStream(finalFile));
            out = new ObjectOutputStream(new FileOutputStream(sourceFile));
            while(!eof){
                Batch present =(Batch) in.readObject();
                out.writeObject(present);
            }
        }catch (EOFException e){
            eof=true;

        }catch (Exception e){
            System.out.println(e.getMessage()+"57");
            System.exit(1);
        }
        File f = new File(finalFile);
        f.delete();

    }

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
   private String helper(Vector<String> f){
        int inputNum = f.size();
        if(inputNum == 1){
            return f.get(0);
        }
        int outputNum = 1;
        Batch[] inputBuffers = new Batch[f.size()];
        Batch outputBuffer = new Batch(Batch.getPageSize()/baseSchema.getTupleSize());
        Vector<ObjectInputStream> inputStreams= new Vector<>();
        String file = "EXTemp-"+String.valueOf(runNo);
        runNo++;
        try{
            ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(file));
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
        while(!sm.allEOF()){
            try{
                if(outputBuffer.isFull()){
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
            int Index = -1;
            for(int i = 0;i<f.size();i++){
                if(inputBuffers[i] != null &&!inputBuffers[i].isEmpty()){
                    t=inputBuffers[i].elementAt(0);
                    Index = i;
                    break;
                }
            }
            if(Index==-1){
                continue;
            }
            for(int i = 0;i<f.size();i++){
                if(inputBuffers[i] != null &&!inputBuffers[i].isEmpty()){
                    if(compare(t,inputBuffers[i].elementAt(0))>=0){
                        t = inputBuffers[i].elementAt(0);
                        Index=i;
                    }
                }
            }
            outputBuffer.add(t);
            inputBuffers[Index].remove(0);

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

    private int compare(Tuple t1, Tuple t2) {
        for (int i = 0; i < attrIndex.length; i++) {
            int a = Tuple.compareTuples(t1, t2, attrIndex[i]);
            if (a != 0) {
                return a;
            }
        }
        return 0;
    }

    private void generateSortedRuns(String filename){
        runNo=0;
        int tuplesize = baseSchema.getTupleSize();
        batchSize= Batch.getPageSize()/tuplesize;
        int numOfTuples = batchSize * numOfBuffers;
        Vector<Tuple> tuplesInRun;
        Vector<Batch> batches = new Vector<>();
        TupleComparator  tc = new TupleComparator(attrIndex);

        //System.out.println("Scan:----------Scanning:"+tabname);
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
                    batches.add((Batch)in.readObject());
                }
                if(eos){
                    //convert batches to tuples
                    tuplesInRun = batchToTuple(batches);
                    Collections.sort(tuplesInRun,tc);
                    System.out.println("221");
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
                System.out.println(String.valueOf(eos)+ "   " +String.valueOf(batches.size()));

//                filename = "BLJTemp-"+String.valueOf(runNo);
//                fileNames.add(filename);
//                out = new ObjectOutputStream(new FileOutputStream(filename));
//                for(Tuple tuple:tuplesInRun){
//                    out.wri
//                }
            } catch(ClassNotFoundException cnf){
                System.out.println("Scan:Class not found for reading file  "+filename);
                System.exit(1);
            }catch (EOFException EOF) {
                /** At this point incomplete page is sent and at next call it considered
                 ** as end of file
                 **/
                eos=true;
                System.out.println("should read all");
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
    private Vector<Tuple> batchToTuple(Vector<Batch> batches){
        Vector<Tuple> tuples = new Vector<>();
        for(Batch batch:batches){
            for(int i = 0;i<batch.size();i++){
                tuples.add(batch.elementAt(i));
            }
        }
        return tuples;
    }
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
        return batches;
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
    class StreamManager{
        private Vector<ObjectInputStream> allStreams;
        private Vector<Integer> cursors;
        private int size;
        StreamManager(Vector<ObjectInputStream> streams){
            allStreams = streams;
            size = allStreams.size();
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
            if(cursors.get(i)==1){
                return null;
            }else if(cursors.get(i) ==0){
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
