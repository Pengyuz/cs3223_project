/** sort merge join algorithm **/

package qp.operators;

import qp.utils.*;
import java.io.*;
import java.util.*;
import java.lang.*;

public class SortMergeJoin extends Join{


    int batchsize;  //Number of tuples per out batch

    /** The following fields are useful during execution of
     ** the SortMergJoin operation
     **/
    int leftindex;     // Index of the join attribute in left table
    int rightindex;    // Index of the join attribute in right table

    String lfname;    // The file name where the left table is materialize
    String rfname;    // The file name where the right table is materialize

    static int filenum=0;   // To get unique filenum for this operation

    Batch outbatch;   // Output buffer
    Vector<Tuple> curEqualSetLeft; // store left equal set to be joined
    Vector<Tuple> curEqualSetRight; // store right equal set to be joined
    Vector<Tuple> cachedEqualTuples;   // Overflowed equal tuples
    Batch leftbatch;  // Buffer for left input stream
    Batch rightbatch;  // Buffer for right input stream
    ObjectInputStream inl;// File pointer to the left hand materialized file
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
     **  Materializes both left and right hand side into a file
     **  Perform External Sort on the two files
     **  Opens the connections
     **/

    public boolean open(){

        /** select number of tuples per batch **/
        int tuplesize=schema.getTupleSize();
        batchsize=Batch.getPageSize()/tuplesize;

        Attribute leftattr = con.getLhs();
        Attribute rightattr =(Attribute) con.getRhs();
        leftindex = left.getSchema().indexOf(leftattr);
        if (leftindex == -1) {
            Attribute tmp = leftattr;
            leftattr = rightattr;
            rightattr = tmp;
        }
        leftindex = left.getSchema().indexOf(leftattr);
        rightindex = right.getSchema().indexOf(rightattr);
        Batch rightpage;
        Batch leftpage;

        /** initialize the cursors of input streams **/
        lcurs = 0;
        rcurs =0;

        /** Both Left and Right hand side tables are to be materialized
         ** for the Sort Merge join to perform
         **/

        if(!right.open()){
            return false;
        }else{
             /** Materialize the operator from right
             ** into a file
             **/

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

            if(!right.close())
                return false;
        }


        /** Sort the right materialized table on
         ** given artribute sets, one attr in this case
         **/
        Vector<Attribute> vsr = new Vector<>();
        vsr.add(rightattr);
        ExternalSort s = new ExternalSort(rfname, right.getSchema(), vsr, 4,numBuff);
        s.doSort();



        if(!left.open()){
            return false;
        }else{
            /** Materialize the operator from left
             ** into a file
             **/

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
            if(!left.close())
                return false;
        }


        /** Sort the left materialized table on
         ** given artribute sets, one attr in this case
         **/
        Vector<Attribute> vsl = new Vector<>();
        vsl.add(leftattr);
        ExternalSort s2 = new ExternalSort(lfname, left.getSchema(), vsl, 4,numBuff);
        s2.doSort();


        /** Scan of left and right sorted table
         ** into inputstreams
        **/
        try {
            inl = new ObjectInputStream(new FileInputStream(lfname));
            inr = new ObjectInputStream(new FileInputStream(rfname));
            leftbatch = (Batch) inl.readObject();
            rightbatch = (Batch) inr.readObject();
        }catch (EOFException e) {
            leftbatch  =null;
            rightbatch = null;
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        cachedEqualTuples = new Vector<Tuple>();
        curEqualSetLeft = new Vector<Tuple>();
        curEqualSetRight = new Vector<Tuple>();
        return true;
    }



    /** from input buffers selects the tuples satisfying join condition
     ** And returns a page of output tuples
     ** The merge process for the join
     **/


    public Batch next(){

        /** no more matches and no more cached matched tuples, end of join
         **/
        if((leftbatch == null || rightbatch == null) && cachedEqualTuples.size() == 0){
            close();
            return null;
        }
        outbatch = new Batch(batchsize);

        /** read matched tuple from cached result if there are
         **/
        while (!outbatch.isFull() && !(cachedEqualTuples.size() == 0)) {
            outbatch.add(cachedEqualTuples.remove(0));
        }

        /** merge process
         **/
        while(!outbatch.isFull() && leftbatch != null && rightbatch != null) {
            // check if tuples match
            int hasmatch = Tuple.compareTuples(leftbatch.elementAt(lcurs), rightbatch.elementAt(rcurs), leftindex, rightindex);

            if (hasmatch == 0) {// matched tuples
                Tuple firstEqualTupleLeft = leftbatch.elementAt(lcurs);
                Tuple firstEqualTupleRight = rightbatch.elementAt(rcurs); //tuples to be compared later
                curEqualSetLeft.add(firstEqualTupleLeft);
                curEqualSetRight.add(firstEqualTupleRight);
                rcurs = advancercurs(rightbatch);
                lcurs = advancelcurs(leftbatch);

                // find matched left tuple and firstEqualTupleRight, until not match
                while (lcurs != -1) {
                    if (Tuple.compareTuples(leftbatch.elementAt(lcurs),firstEqualTupleRight, leftindex, rightindex) == 0) {
                        curEqualSetLeft.add(leftbatch.elementAt(lcurs));
                        lcurs = advancelcurs(leftbatch);
                    } else {
                        break;
                    }
                }

                // find matched right tuple and firstEqualTupleLeft, until not match
                while (rcurs != -1) {
                    if (Tuple.compareTuples(firstEqualTupleLeft,rightbatch.elementAt(rcurs), leftindex, rightindex) == 0) {
                        curEqualSetRight.add(rightbatch.elementAt(rcurs));
                        rcurs = advancercurs(rightbatch);
                    } else {
                        break;
                    }
                }

                // write out the joined tuples to outbatch otherwise cache it
                for (Tuple tuplel : curEqualSetLeft) {
                    for (Tuple tupler : curEqualSetRight) {
                        Tuple res = tuplel.joinWith(tupler);
                        if (outbatch.isFull()) {
                            cachedEqualTuples.add(res);
                        } else {
                            outbatch.add(res);
                        }
                    }
                }

                // renew the equal sets
                curEqualSetLeft.clear();
                curEqualSetRight.clear();

            } else if (hasmatch > 0) {
                rcurs = advancercurs(rightbatch);
            } else {
                lcurs = advancelcurs(leftbatch);
            }
        }
        return outbatch;
    }

    /** advance lcurs **/
    private int advancelcurs(Batch leftba) {
        if(lcurs < leftba.size() - 1){
            return lcurs + 1;
        } else {
            try {
                leftbatch = (Batch) inl.readObject();
            }catch(EOFException e){
                try{
                    inl.close();
                }catch (IOException io){
                    System.out.println("SortMerge:Error in temporary file reading");
                }
                leftbatch = null;
                return -1;
            } catch (IOException e) {
                e.printStackTrace();
            } catch (ClassNotFoundException e) {
                e.printStackTrace();
            }
            return 0;
        }
    }
    /** advance rcurs **/
    private int advancercurs(Batch rightba) {
        if(rcurs < rightba.size() - 1){
            return rcurs + 1;
        } else {
            try {
                rightbatch = (Batch) inr.readObject();
            }catch(EOFException e){
                try{
                    inr.close();
                }catch (IOException io){
                    System.out.println("SortMerge:Error in temporary file reading");
                }
                rightbatch = null;
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

        File f = new File(rfname);
        File f2 = new File(lfname);
        f.delete();
        f2.delete();
        return true;

    }


}
