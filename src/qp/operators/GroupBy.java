package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;

import java.io.*;
import java.util.Vector;

public class GroupBy extends Operator{
    Operator base;
    Vector attrSet;
    int batchsize;  // number of tuples per outbatch


    /** The following fields are requied during execution
     ** of the Project Operator
     **/

    Batch inbatch;
    Batch outbatch;
    int curIndex;
    static int filenum=0;   // To get unique filenum for this operation
    String filename;
    int numbuffer;
    ObjectInputStream in;// File pointer to the sorted materialized file
    Vector<Tuple> cachedSameGroupTuples;
    int[] attrIndex;
    Tuple lastTuple = null;
    public GroupBy(Operator base, Vector as,int type){
        super(type);
        this.base=base;
        this.attrSet=as;
        /**
         ** at this point, all join operations has finished
         ** can use all buffers available
         **/
        this.numbuffer = BufferManager.getAllAvailableBuffers();
    }

    public void setBase(Operator base){
        this.base = base;
    }

    public Operator getBase(){
        return base;
    }

    public Vector getGroupByAttr(){
        return attrSet;
    }


    /** Opens the connection to the base operator
     ** Also figures out what are the columns to be
     ** grouped from the base operator
     **/
    public boolean open(){
        /** setnumber of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize=Batch.getPageSize()/tuplesize;

        try{
            Schema baseSchema = base.getSchema();
            attrIndex = new int[attrSet.size()];
            //System.out.println("Project---Schema: ----------in open-----------");
            //System.out.println("base Schema---------------");
            //Debug.PPrint(baseSchema);
            for(int i=0;i<attrSet.size();i++){
                Attribute attr = (Attribute) attrSet.elementAt(i);
                int index = baseSchema.indexOf(attr);
                attrIndex[i]=index;

                //  Debug.PPrint(attr);
                //System.out.println("  "+index+"  ");
            }
        } catch(ArrayIndexOutOfBoundsException e){
            System.out.println("array index out of bound.");
            return false;
        }

        Batch nextpage;

        if(!base.open()){
            return false;
        }else{
            /** Materialize the operator from base
             ** into a file
             **/

            filenum++;
            filename = "GrpBytemp-" + String.valueOf(filenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
                while( (nextpage = base.next()) != null){
                    out.writeObject(nextpage);
                }
                out.close();
            }catch(IOException io){
                System.out.println("GroupBy:writing the temporay file error");
                return false;
            }

            if(!base.close())
                return false;
        }

        ExternalSort s = new ExternalSort(filename, base.getSchema(), attrSet, 4, numbuffer);
        s.doSort();

        /** Scan of sorted table
         ** into inputstreams
         **/
        try {
            in = new ObjectInputStream(new FileInputStream(filename));
            inbatch = (Batch) in.readObject();
        } catch (EOFException e){
            e.printStackTrace();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        cachedSameGroupTuples = new Vector<Tuple>();
        curIndex = -2;

        return true;
    }

    public Batch next(){

        if(inbatch == null && cachedSameGroupTuples.size() == 0){
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull() && !(cachedSameGroupTuples.size() == 0)) {
            outbatch.add(cachedSameGroupTuples.remove(0));
        }

        while (!outbatch.isFull() && inbatch != null) {
            if (curIndex == -2) {
                lastTuple = inbatch.elementAt(curIndex +2);
                curIndex++;
                curIndex++;
                outbatch.add(lastTuple);
            }
            curIndex = advanceCurIndex(inbatch);
            if (curIndex == -1) {
                break;
            }
            int compareRes = compareTuplesInIndexs(lastTuple, inbatch.elementAt(curIndex), attrIndex);

            if (compareRes == 0) { //same group
                Tuple toAdd = inbatch.elementAt(curIndex);
                if (!outbatch.isFull()) {
                    outbatch.add(toAdd);
                } else {
                    cachedSameGroupTuples.add(toAdd);
                }
                lastTuple = toAdd;
            } else { // new group
                Vector<String> empties = new Vector<>();
                for (int i = 0; i < base.getSchema().getAttList().size(); i++) {
                    empties.add("");
                }
                Tuple dummy = new Tuple(empties);
                if (!outbatch.isFull()) {
                    outbatch.add(dummy);
                } else {
                    cachedSameGroupTuples.add(dummy);
                }
                lastTuple = inbatch.elementAt(curIndex);
                if (!outbatch.isFull()) {
                    outbatch.add(lastTuple);
                } else {
                    cachedSameGroupTuples.add(lastTuple);
                }
            }
        }

        return outbatch;
    }

    private int compareTuplesInIndexs( Tuple left,Tuple right, int[] indexs){
        if(indexs.length==0){
            return 0;
        }
        for(int i=0;i<indexs.length;i++){
            if(Tuple.compareTuples(left,right,indexs[i])!=0){
                return 1;
            }
        }
        return 0;
    }

    private int advanceCurIndex(Batch inba) {
        if(curIndex < inba.size() - 1){
            return curIndex + 1;
        } else {
            try {
                inbatch = (Batch) in.readObject();
            }catch(EOFException e){
                try{
                    in.close();
                }catch (IOException io){
                    System.out.println("SortMerge:Error in temporary file reading");
                }
                inbatch = null;
                return -1;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }


    /** Close the operator */
    public boolean close(){
        File f = new File(filename);
        //f.delete();
        return true;
		/*
	if(base.close())
	    return true;
	else
	    return false;
	    **/
    }


    public Object clone(){
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for(int i=0;i<attrSet.size();i++)
            newattr.add((Attribute) ((Attribute)attrSet.elementAt(i)).clone());
        GroupBy newproj = new GroupBy(newbase,newattr,optype);
        Schema newSchema = newbase.getSchema().subSchema(newattr);
        newproj.setSchema(newSchema);
        return newproj;
    }
}
