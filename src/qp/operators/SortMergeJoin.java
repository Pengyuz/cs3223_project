/** page nested join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class SortMergeJoin extends Join{


    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the NestedJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String lfname;    // The file name where the left table is materialize
    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Vector<Tuple> curEqualSetLeft;
    Vector<Tuple> curEqualSetRight;
    Vector<Tuple> overflowTuples;   // Overflow tuples
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream inl;
    ObjectInputStream inr;// File pointer to the right hand materialized file

    int lcurs;    // Cursor for left side buffer
    int rcurs;    // Cursor for right side buffer

    public SortMergeJoin(Join jn){
        super(jn.getLeft(),jn.getRight(),jn.getCondition(),jn.getOpType());
        schema = jn.getSchema();
        jointype = jn.getJoinType();
        numBuff = jn.getNumBuff();
    }


    /** During open finds the index of the join attributes
     **  Materializes the right hand side into a file
     **  Opens the connections
     **/



    public boolean open(){

        /** select number of tuples per batch **/
        int tuplesize=schema.getTupleSize();
        batchsize=Batch.getPageSize()/tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        Batch leftpage;
        /** initialize the cursors of input buffers **/

        lcurs = 0; rcurs =0;

        /** Both Left and Right hand side tables are to be materialized
         ** for the Sort Merge join to perform
         **/

        if(!right.open()){
            return false;
        }else{
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/

            //if(right.getOpType() != OpType.SCAN){
            filenum++;
            rfname = "SMJrtemp-" + String.valueOf(filenum);
            try{
                ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(rfname));
                while( (rightpage = right.next()) != null){
                    out.writeObject(rightpage);
                }
                out.close();
            }catch(IOException io){
                System.out.println("SortMergeJoin:writing the temporay file error");
                return false;
            }
            //}
            if(!right.close())
                return false;
        }
        Vector<Attribute> vsr = new Vector<>();
        vsr.add(rightattr);
        ExternalSort s = new ExternalSort(rfname, right.getSchema(), vsr, 4,numBuff);
        s.doSort();

        if(!left.open()){
            return false;
        }else{
            /** If the right operator is not a base table then
             ** Materialize the intermediate result from right
             ** into a file
             **/

            //if(right.getOpType() != OpType.SCAN){
            filenum++;
            lfname = "SMJltemp-" + String.valueOf(filenum);
            try{
                ObjectOutputStream out2 = new ObjectOutputStream(new FileOutputStream(lfname));
                while( (leftpage = left.next()) != null){
                    out2.writeObject(leftpage);
                }
                out2.close();
            }catch(IOException io){
                System.out.println("SortMergeJoin:writing the temporay file error");
                return false;
            }
            //}
            if(!left.close())
                return false;
        }

        Vector<Attribute> vsl = new Vector<>();
        vsl.add(leftattr);
        ExternalSort s2 = new ExternalSort(lfname, left.getSchema(), vsl, 4,numBuff);
        s2.doSort();

        // Scan of left and right sorted table
        try {
            inl = new ObjectInputStream(new FileInputStream(lfname));
            inr = new ObjectInputStream(new FileInputStream(rfname));
            leftbatch = (Batch) inl.readObject();
            rightbatch = (Batch) inr.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        overflowTuples = new Vector<Tuple>();
        curEqualSetLeft = new Vector<Tuple>();
        curEqualSetRight = new Vector<Tuple>();
        return true;
    }



    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     **/


    public Batch next(){
        //System.out.print("SortMergeJoin:--------------------------in next----------------");
        //Debug.PPrint(con);
        //System.out.println();

        //no more result, end of join
        if((leftbatch == null || rightbatch == null) && overflowTuples.size() == 0){
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        while (!outbatch.isFull() && !(overflowTuples.size() == 0)) {
            outbatch.add(overflowTuples.remove(0));
        }

        while(!outbatch.isFull() && leftbatch != null && rightbatch != null) {
            int hasmatch = Tuple.compareTuples(leftbatch.elementAt(lcurs), rightbatch.elementAt(rcurs), leftindex, rightindex);

            if (hasmatch == 0) {
                curEqualSetLeft.add(leftbatch.elementAt(lcurs));
                curEqualSetRight.add(rightbatch.elementAt(rcurs));
                Tuple firstEqualTupleLeft = curEqualSetLeft.get(0);
                Tuple firstEqualTupleRight = curEqualSetRight.get(0);

                lcurs = advancelcurs(lcurs, leftbatch);
                while (lcurs != -1 && Tuple.compareTuples(leftbatch.elementAt(lcurs),firstEqualTupleRight, leftindex, rightindex) == 0) {
                    curEqualSetLeft.add(leftbatch.elementAt(lcurs));
                    lcurs = advancelcurs(lcurs, leftbatch);
                }

                rcurs = advancercurs(rcurs, rightbatch);
                while (rcurs != -1 && Tuple.compareTuples(firstEqualTupleLeft,rightbatch.elementAt(rcurs), leftindex, rightindex) == 0) {
                    curEqualSetRight.add(rightbatch.elementAt(rcurs));
                    rcurs = advancercurs(rcurs, rightbatch);
                }

                for (Tuple tuplel : curEqualSetLeft) {
                    for (Tuple tupler : curEqualSetRight) {
                        if (!outbatch.isFull()) {
                            outbatch.add(tuplel.joinWith(tupler));
                        } else {
                            overflowTuples.add(tuplel.joinWith(tupler));
                        }
                    }
                }

                curEqualSetLeft.clear();
                curEqualSetRight.clear();

            } else if (hasmatch > 0) {
                rcurs = advancercurs(rcurs, rightbatch);
            } else {
                lcurs = advancelcurs(lcurs, leftbatch);
            }
        }
        return outbatch;
    }

    private int advancelcurs(int lcurs, Batch leftbatch) {
        if(leftbatch == null){
            return -1;
        } else {
            if(lcurs != leftbatch.size()-1){
                return lcurs+1;
            } else {
                try {
                    leftbatch = (Batch) inl.readObject();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        }
    }

    private int advancercurs(int rcurs, Batch rightbatch) {
        if(rightbatch == null){
            return -1;
        } else {
            if(rcurs != rightbatch.size()-1){
                return rcurs+1;
            } else {
                try {
                    rightbatch = (Batch) inr.readObject();
                } catch (IOException e) {
                    e.printStackTrace();
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
                return 0;
            }
        }
    }

    /** Close the operator */
    public boolean close(){

        File f = new File(rfname);
        File f2 = new File(lfname);
        f.delete();
        f2.delete();
        return true;

    }


}











































