package ch.epfl.dias.ops.vector;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.HashMap;

public class Join implements VectorOperator {

    private VectorOperator leftChild;
    private VectorOperator rightChild;
    private int leftFieldNo;
    private int rightFieldNo;

    public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftFieldNo = leftFieldNo;
        this.rightFieldNo = rightFieldNo;
    }

    @Override
    public void open() {
        leftChild.open();
        rightChild.open();
    }

    @Override
    public DBColumn[] next() {
        DBColumn[] rightColumns = rightChild.next();
        DBColumn[] leftColumns = leftChild.next();

        // if no more tuples then return eof=true
        if (rightColumns[0].eof || leftColumns[0].eof) {
            return new DBColumn[]{new DBColumn()};
        }

        // in order to eventually reconstruct the whole columns
        DBColumn[] columns = new DBColumn[leftColumns.length+rightColumns.length-1];
        DataType[] dtArray = new DataType[leftColumns.length+rightColumns.length-1];
        for (int i = 0; i < leftColumns.length+rightColumns.length-1; i++) {
            if (i < leftColumns.length) {
                dtArray[i] = leftColumns[i].type;
            } else {
                if (i != leftColumns.length+rightFieldNo) {
                    dtArray[i] = rightColumns[i-leftColumns.length].type;
                }
            }
        }

        ArrayList<ArrayList<Object>> result = new ArrayList<>();
        for (int i = 0; i < leftColumns.length+rightColumns.length-1; i++) {
            ArrayList<Object> col = new ArrayList<>();
            result.add(col);
        }
        ArrayList<ArrayList<Object>> wholeLeftColumns = new ArrayList<>();
        for (int i = 0; i < leftColumns.length; i++) {
            ArrayList<Object> col = new ArrayList<>();
            wholeLeftColumns.add(col);
        }
        HashMap<Object, ArrayList<Integer>> hashes = new HashMap<>();
        int index = 0;
        while (!leftColumns[0].eof) {
            for (int i = 0; i < leftColumns.length; i++) {
                for (int j = 0; j < leftColumns[0].column.length; j++) {
                    wholeLeftColumns.get(i).add(leftColumns[i].column[j]);
                }
            }
            Object[] leftFields = leftColumns[leftFieldNo].column;
            for (Object lf : leftFields) {
                if (hashes.get(lf) != null) {
                    hashes.get(lf).add(index);
                } else {
                    ArrayList<Integer> listIndices = new ArrayList<>();
                    listIndices.add(index);
                    hashes.put(lf, listIndices);
                }
                index++;
            }
            leftColumns = leftChild.next();
        }

        while(!rightColumns[0].eof) {
            Object[] rightFields = rightColumns[rightFieldNo].column;
            index = 0;
            for (Object rf : rightFields) {
                if (hashes.get(rf) != null) {
                    for (int i = 0; i < wholeLeftColumns.size(); i++) {
                        for (int idx : hashes.get(rf)) {
                            // first add tuples from left column
                            result.get(i).add(wholeLeftColumns.get(i).get(idx)  /* leftColumns[i].column[idx]*/);
                        }
                    }
                    for (int i = wholeLeftColumns.size(); i < wholeLeftColumns.size()+rightColumns.length-1; i++) {
                        if (i != wholeLeftColumns.size()+rightFieldNo) {
                            // then tuples from right column
                            result.get(i).add(rightColumns[i-wholeLeftColumns.size()].column[index]);
                        }
                    }
                }
                index++;
            }
            rightColumns = rightChild.next();
        }


        // reconstruction of the whole columns
        for(int i = 0; i < result.size(); i++) {
            columns[i] = new DBColumn(result.get(i).toArray(), dtArray[i]);
        }

//        for(int i = 0; i < columns[0].column.length; i++) {
//            for (int j = 0; j < columns.length; j++) {
//                System.out.print(columns[j].column[i] +" ");
//            }
//            System.out.println();
//        }

        return columns;
    }

    @Override
    public void close() {
        leftChild.close();
        rightChild.close();
    }
}
