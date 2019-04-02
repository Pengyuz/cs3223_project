/** prepares a random initial plan for the given SQL query **/
/** see the ReadMe file to understand this **/

package qp.optimizer;

import jdk.nashorn.internal.ir.Block;
import qp.utils.*;
import qp.operators.*;

import java.sql.SQLOutput;
import java.util.*;
import java.io.*;

public class DPOptimizer{

    SQLQuery sqlquery;

    Vector projectlist;
    Vector fromlist;
    Vector selectionlist;     //List of select conditons
    Vector joinlist;          //List of join conditions
    Vector groupbylist;
    boolean isDistinct;
    int numJoin;    // Number of joins in this query
    int MINCOST;


    Hashtable tab_op_hash;          //table name to the Operator
    Operator root; // root of the query plan tree
    HashMap<String,Vector<String>> edgeList;



    public DPOptimizer(SQLQuery sqlquery){
        this.sqlquery=sqlquery;

        projectlist=(Vector) sqlquery.getProjectList();
        fromlist=(Vector) sqlquery.getFromList();
        selectionlist= sqlquery.getSelectionList();
        joinlist = sqlquery.getJoinList();
        groupbylist = sqlquery.getGroupByList();
        numJoin = joinlist.size();
        isDistinct = sqlquery.isDistinct();
        edgeList = new  HashMap<String,Vector<String>>();
        for(int i = 0;i<fromlist.size();i++){
            Vector<String> neighbors = new Vector<>();
            String currentTab = (String)fromlist.elementAt(i);
            edgeList.put(currentTab,neighbors);
        }
        buildEdgeList();

    }


    /** number of join conditions **/

    public int getNumJoins(){
        return numJoin;
    }


    /** prepare initial plan for the query **/

    public Operator prepareInitialPlan(){

        tab_op_hash = new Hashtable();

        createScanOp();
        createSelectOp();
        if(numJoin !=0){
            createJoinOp();
        }
        createProjectOp();
        if(isDistinct) {
            createDistinctOp();
        }
        if (groupbylist != null && groupbylist.size() != 0){
            createGroupByOp();
        }

        return root;
    }
    public void buildEdgeList(){
        for(int i=0;i<joinlist.size();i++){
            Condition c = (Condition) joinlist.elementAt(i);
            Attribute left = c.getLhs();
            Attribute right  = (Attribute) c.getRhs();
            String leftTab = left.getTabName();
            String rightTab = right.getTabName();
            Vector<String> leftNb = edgeList.get(leftTab);
            leftNb.add(rightTab);
            edgeList.put(leftTab,leftNb);
            Vector<String> rightNb = edgeList.get(rightTab);
            rightNb.add(leftTab);
            edgeList.put(rightTab,rightNb);
        }
    }


    public void createDistinctOp() {
        Operator base = root;
        if ( projectlist == null )
            projectlist = new Vector();

        root = new Distinct(base, projectlist, OpType.DISTINCT);
        Schema newSchema = base.getSchema();
        root.setSchema(newSchema);
    }


    public void createGroupByOp(){
        Operator base = root;
        if ( groupbylist == null )
            groupbylist = new Vector();

        if(!groupbylist.isEmpty()){
            root = new GroupBy(base,groupbylist,OpType.GROUPBY);
            Schema newSchema = base.getSchema();
            root.setSchema(newSchema);
        }
    }





    /** Create Scan Operator for each of the table
     ** mentioned in from list
     **/

    public void createScanOp(){
        int numtab = fromlist.size();
        Scan tempop = null;

        for(int i=0;i<numtab;i++){  // For each table in from list


            String tabname = (String) fromlist.elementAt(i);
            Scan op1 = new Scan(tabname,OpType.SCAN);
            tempop = op1;


            /** Read the schema of the table from tablename.md file
             ** md stands for metadata
             **/

            String filename = tabname+".md";
            try {
                ObjectInputStream _if = new ObjectInputStream(new FileInputStream(filename));
                Schema schm = (Schema) _if.readObject();
                op1.setSchema(schm);
                _if.close();
            } catch (Exception e) {
                System.err.println("RandomInitialPlan:Error reading Schema of the table" + filename);
                System.exit(1);
            }
            Plan pl = new Plan(op1);
            HashSet<String> s = new HashSet<String>();
            s.add(tabname);
            tab_op_hash.put(s,pl);
        }

        // 12 July 2003 (whtok)
        // To handle the case where there is no where clause
        // selectionlist is empty, hence we set the root to be
        // the scan operator. the projectOp would be put on top of
        // this later in CreateProjectOp
        if ( selectionlist.size() == 0 ) {
            root = tempop;
            return;
        }

    }


    /** Create Selection Operators for each of the
     ** selection condition mentioned in Condition list
     **/


    public void createSelectOp(){
        Select op1 = null;

        for(int j=0;j<selectionlist.size();j++){

            Condition cn = (Condition) selectionlist.elementAt(j);
            if(cn.getOpType() == Condition.SELECT){
                String tabname = cn.getLhs().getTabName();
                //System.out.println("RandomInitial:-------------Select-------:"+tabname);

                Operator tempop = (Operator)tab_op_hash.get(tabname);
                op1 = new Select(tempop,cn,OpType.SELECT);
                /** set the schema same as base relation **/
                op1.setSchema(tempop.getSchema());

                modifyHashtable(tempop,op1);
                //tab_op_hash.put(tabname,op1);

            }
        }
        /** The last selection is the root of the plan tre
         ** constructed thus far
         **/
        if(selectionlist.size() != 0)
            root = op1;
    }

    /** create join operators **/

    public void createJoinOp(){

        if(numJoin !=0)
            root = DP();
    }

    public Operator getOptimizedPlan() {
        PlanCost pc = new PlanCost();
        MINCOST  = pc.getCost(root);
        System.out.println("\n\n\n");
        System.out.println("---------------------------Final Plan----------------");
        Debug.PPrint(root);
        System.out.println("  "+MINCOST);
        return root;
    }

    private Operator DP(){
        for(int i=2;i<=fromlist.size();i++){
            Set<HashSet<String>> allSets = powerSet( fromlist, i);
            for (HashSet<String> s : allSets){
                Plan bestPlan = new Plan(new Operator(OpType.JOIN), Integer.MAX_VALUE);
                Set<HashSet<String>> allsubsets = allSubsets(new Vector<>(s));
                for (HashSet<String> set: allsubsets) {
                    HashSet<String> rightset = removeSets((HashSet<String>) s.clone(), set);

                    Vector<String> rjNb = edgeList.get(set);

                    if (checkJoin(set, rightset)) {
                        if (tab_op_hash.containsKey(set)) {
                            Plan planleft = (Plan) tab_op_hash.get(set);
                            int planleft_cost = planleft.getCost();
                            Plan planright = (Plan) tab_op_hash.get(rightset);
                            if (planleft == null || planright == null) {
                                continue;
                            }
                            Operator ol = planleft.getRoot();
                            Operator or = planright.getRoot();
                            Plan curP = joinPlan(planleft, planright);
                            if (curP.getCost() < bestPlan.getCost()) {
                                bestPlan = curP;
                            }
                        }

                    }

                }
                if (bestPlan.getCost() != Integer.MAX_VALUE) {
                    tab_op_hash.put(s, bestPlan);
                }

            }
        }
        HashSet<String> alltab = new HashSet<>();
        for (int k = 0; k < fromlist.size(); k++) {
            alltab.add((String)fromlist.elementAt(k));
        }
        MINCOST = ((Plan)tab_op_hash.get(alltab)).getCost();
        return ((Plan)tab_op_hash.get(alltab)).getRoot();
    }

    private boolean checkJoin(HashSet<String> left, HashSet<String> right) {
        for (String ls: left) {
            Vector<String> lnb = edgeList.get(ls);
            for (String ln: lnb) {
                if (right.contains(ln)) {
                    return true;
                }
            }
        }
        return false;
    }

    private Plan joinPlan(Plan left, Plan right){
        Schema leftSchema = left.getRoot().getSchema();
        Schema rightSchema = right.getRoot().getSchema();
        Condition con = null;
        Vector<Condition> cons = new Vector<>();
        for (int i = 0; i < joinlist.size(); i++) {
            Condition cur = (Condition) joinlist.elementAt(i);
            Attribute leftattr = cur.getLhs();
            Attribute rightattr = (Attribute) cur.getRhs();
            if(leftSchema.indexOf(leftattr) != -1 && rightSchema.indexOf(rightattr) != -1) {
                cons.add(cur);
            } else if (leftSchema.indexOf(rightattr) != -1 && rightSchema.indexOf(leftattr) != -1) {
                cur.flip();
                cons.add(cur);
            }
        }
        if (cons.size() != 0) {
            Plan minPlan = new Plan(new Operator(OpType.JOIN), Integer.MAX_VALUE);
            Condition minCondi = null;
            for (Condition condi : cons) {
                Join in = new Join(left.getRoot(), right.getRoot(), condi, 3);
                Schema newsche = left.getRoot().getSchema().joinWith(right.getRoot().getSchema());
                in.setSchema(newsche);
                NestedJoin nj = new NestedJoin(in);
                nj.setJoinType(JoinType.NESTEDJOIN);
                BlockNestedJoin bj = new BlockNestedJoin(in);
                bj.setJoinType(JoinType.BLOCKNESTED);
                SortMergeJoin sj = new SortMergeJoin(in);
                sj.setJoinType(JoinType.SORTMERGE);
                PlanCost pc1 = new PlanCost();
                PlanCost pc2 = new PlanCost();
                PlanCost pc3 = new PlanCost();
                int nj_cost = pc1.getCost(nj);
                int bj_cost = pc2.getCost(bj);
                int sj_cost = pc3.getCost(sj);
                //System.out.println(nj_cost + " " + bj_cost +" "+ sj_cost);
                int min = Math.min(nj_cost, Math.min(bj_cost, sj_cost));
                if (min < minPlan.getCost()) {
                    minCondi = condi;
                    if (min == bj_cost) {
                        minPlan = new Plan(bj, min);
                    } else if (min == sj_cost) {
                        minPlan = new Plan(sj, min);
                    } else {
                        minPlan = new Plan(nj, min);
                    }
                }
            }
            cons.remove(minCondi);
            Operator tempop = minPlan.getRoot();
            for (Condition c: cons) {
                c.setExprType(Condition.SELECT);
                Select sop = new Select(tempop,c, OpType.SELECT);
                sop.setSchema(tempop.getSchema());
                tempop = sop;
            }
            PlanCost p = new PlanCost();
            int finalcost = p.getCost(tempop);
            return new Plan(tempop, finalcost);
        } else {
            return null;
        }
    }

    private Set<HashSet<String>> allSubsets(Vector<String> set) {
        Set<HashSet<String>> res = new HashSet<>();
        for (int i = 1; i < set.size(); i++) {
            Set<HashSet<String>> cur = powerSet(set, i);
            for (HashSet<String> s: cur) {
                res.add(s);
            }
        }
        return res;
    }

    private HashSet<String> removeSets(HashSet<String> origin, HashSet<String> subtract) {
        for (String s:subtract) {
            origin.remove(s);
        }
        return origin;
    }

    private Set<HashSet<String>> powerSet(Vector<String> set, int n) {
        if (n < 0)
            throw new IllegalArgumentException();
        Set<HashSet<String>> temp = new HashSet<>();
        int size = set.size();
        if (n > size)
            return temp;
        List<String> list = new ArrayList<>(set);
        int[] indices = new int[n];
        for (int i = 0; i < n; i++)
            indices[i] = i;
        while (true) {
            HashSet<String> s = new HashSet<>();
            for (int i : indices)
                s.add(list.get(i));
            temp.add(s);
            int r = n - 1;
            for (int m = size; r >= 0 && indices[r] == --m; r--);
            if (r == -1)
                return temp;
            for (int c = indices[r]; r < n;)
                indices[r++] = ++c;
        }
    }



    public void createProjectOp(){
        Operator base = root;
        if ( projectlist == null )
            projectlist = new Vector();

        if(!projectlist.isEmpty()){
            root = new Project(base,projectlist,OpType.PROJECT);
            Schema newSchema = base.getSchema().subSchema(projectlist);
            root.setSchema(newSchema);
        }
    }

    private void modifyHashtable(Operator old, Operator newop){
        Enumeration e=tab_op_hash.keys();
        while(e.hasMoreElements()){
            HashSet<String> key = (HashSet<String>)e.nextElement();
            Plan temp = (Plan) tab_op_hash.get(key);
            Plan oldPl = new Plan(old);
            if(temp==oldPl){
                Plan newPl = new Plan(newop);
                tab_op_hash.put(key,newPl);
            }
        }
    }

    public static Operator makeExecPlan(Operator node){

        if(node.getOpType()==OpType.JOIN){
            Operator left = makeExecPlan(((Join)node).getLeft());
            Operator right = makeExecPlan(((Join)node).getRight());
            int joinType = ((Join)node).getJoinType();
            //int joinType = JoinType.SORTMERGE;
            int numbuff = BufferManager.getBuffersPerJoin();
            switch(joinType){
                case JoinType.NESTEDJOIN:

                    NestedJoin nj = new NestedJoin((Join) node);
                    nj.setLeft(left);
                    nj.setRight(right);
                    nj.setNumBuff(numbuff);
                    return nj;

                /** Temporarity used simple nested join,
                 replace with hasjoin, if implemented **/

                case JoinType.BLOCKNESTED:

                    BlockNestedJoin bj = new BlockNestedJoin((Join) node);
                    bj.setLeft(left);
                    bj.setRight(right);
                    bj.setNumBuff(numbuff);
                    return bj;

                case JoinType.SORTMERGE:

                    SortMergeJoin sm = new SortMergeJoin((Join) node);
                    sm.setLeft(left);
                    sm.setRight(right);
                    sm.setNumBuff(numbuff);
                    return sm;

                case JoinType.HASHJOIN:

                    NestedJoin hj = new NestedJoin((Join) node);
                    /* + other code */
                    return hj;
                default:
                    return node;
            }
        }else if(node.getOpType() == OpType.SELECT){
            Operator base = makeExecPlan(((Select)node).getBase());
            ((Select)node).setBase(base);
            return node;
        }else if(node.getOpType() == OpType.PROJECT){
            Operator base = makeExecPlan(((Project)node).getBase());
            ((Project)node).setBase(base);
            return node;
        }else if(node.getOpType() == OpType.GROUPBY){
            Operator base  = makeExecPlan(((GroupBy)node).getBase());
            ((GroupBy)node).setBase(base);
            return node;
        } else if(node.getOpType() == OpType.DISTINCT){
            Operator base  = makeExecPlan(((Distinct)node).getBase());
            ((Distinct)node).setBase(base);
            return node;
        }
        else{
            return node;
        }
    }


class Plan{
        private Operator root;
        private int cost;
        Plan(Operator op){
            this.root = op;
            PlanCost pl = new PlanCost();
            cost = pl.getCost(root);
        }
    Plan(Operator op, int c){
        this.root = op;
        cost = c;
    }
        public Operator getRoot(){
            return root;
        }
        public int getCost(){
            return cost;
        }
}


}



