package qp.operators;

import qp.utils.*;
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

    /** index of the attributes in the base operator
     ** that are to be projected
     **/

    int[] attrIndex;


    public GroupBy(Operator base, Vector as,int type){
        super(type);
        this.base=base;
        this.attrSet=as;

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


        /** The followingl loop findouts the index of the columns that
         ** are required from the base operator
         **/

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

        if(base.open())
            return true;
        else
            return false;
    }
}
