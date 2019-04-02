/** distinct operation**/

package qp.operators;

import qp.optimizer.BufferManager;
import qp.utils.*;

import java.io.*;
import java.util.Vector;

public class Distinct extends Operator{
    Operator base;
    Vector attrSet; // which is same as the projectlist
    int batchsize;  // number of tuples per outbatch


    /** The following fields are requied during execution
     ** of the Distinct Operator
     **/

    Batch inbatch; // input buffer
    Batch outbatch; // output buffer
    int curIndex; // iterator of the input buffer
    static int filenum=0;   // To get unique filenum for this operation
    String filename; //temp file
    int numbuffer;

    ObjectInputStream in;// File pointer to the sorted materialized file
    Vector<Tuple> cachedTuples; //used to store overflow tuples, if there is
    Tuple lastTuple = null; // previous tuple to compare

    public Distinct(Operator base, Vector as,int type){
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

    public Vector getDistinctAttr(){
        return attrSet;
    }

    /** Materialize the operator from base to temp file
     * Sort and write back to temp file
     * Opens the connection to the sorted file
     **/
    public boolean open(){
        /** setnumber of tuples per batch **/
        int tuplesize = schema.getTupleSize();
        batchsize=Batch.getPageSize()/tuplesize;

        Batch nextpage;

        if(!base.open()){
            return false;
        }else{
            /** Materialize the operator from base
             ** into a file
             **/

            filenum++;
            filename = "Disdincttemp-" + String.valueOf(filenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(filename));
                while( (nextpage = base.next()) != null){
                    out.writeObject(nextpage);
                }
                out.close();
            }catch(IOException io){
                System.out.println("Distinct:writing the temporay file error");
                return false;
            }

            if(!base.close())
                return false;
        }
        /** sort the material file
         **/
        ExternalSort s = new ExternalSort(filename, base.getSchema(), attrSet, 4, numbuffer);
        s.doSortForDistinct();

        /** Scan of sorted table
         ** into inputstream
         **/
        try {
            in = new ObjectInputStream(new FileInputStream(filename));
            inbatch = (Batch) in.readObject();
        } catch (EOFException e){
            e.printStackTrace();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        cachedTuples = new Vector<Tuple>();
        curIndex = -2;

        return true;
    }


    /** from inputstream selects the distinct tuples
     ** And returns a page of output tuples
     **/
    public Batch next(){

        if(inbatch == null && cachedTuples.size() == 0){
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull() && !(cachedTuples.size() == 0)) {
            outbatch.add(cachedTuples.remove(0));
        }

        while (!outbatch.isFull() && inbatch != null) {
            if (curIndex == -2) {
                lastTuple = inbatch.elementAt(curIndex + 2);
                curIndex++;
                curIndex++;
                outbatch.add(lastTuple);
            }
            curIndex = advanceCurIndex(inbatch);
            if (curIndex == -1) {
                break;
            }
            int compareRes = compareTuples(lastTuple, inbatch.elementAt(curIndex));

            if (compareRes == 0) { //duplicate
                continue;
            } else { // to add
                Tuple toAdd = inbatch.elementAt(curIndex);
                if (!outbatch.isFull()) {
                    outbatch.add(toAdd);
                } else {
                    cachedTuples.add(toAdd);
                    return outbatch;
                }
                lastTuple = toAdd;
            }
        }
        return outbatch;
    }

    /** Check if two tuples contains the same records
     **/
    private int compareTuples( Tuple left,Tuple right){
        if(left.data().size()==0 || right.data().size() == 0 || left.data().size() != right.data().size()){
            return 1;
        }
        for(int i=0;i<left.data().size(); i++){
            if(Tuple.compareTuples(left,right,i) != 0){
                return 1;
            }
        }
        return 0;
    }

    /** advance the cursor for input buffer
     **/
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
                    System.out.println("Distinct:Error in temporary file reading");
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
        f.delete();
        return true;
    }


    public Object clone(){
        Operator newbase = (Operator) base.clone();
        Vector newattr = new Vector();
        for(int i=0;i<attrSet.size();i++)
            newattr.add((Attribute) ((Attribute)attrSet.elementAt(i)).clone());
        Distinct newproj = new Distinct(newbase,newattr,optype);
        Schema newSchema = newbase.getSchema();
        newproj.setSchema(newSchema);
        return newproj;
    }
}

