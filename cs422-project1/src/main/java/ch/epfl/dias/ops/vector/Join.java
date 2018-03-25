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

        while (!leftColumns[0].eof) {
            HashMap<Object, ArrayList<Integer>> hashes = new HashMap<>();
            int index = 0;
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
            while(!rightColumns[0].eof) {
                Object[] rightFields = rightColumns[rightFieldNo].column;
                index = 0;
                for (Object rf : rightFields) {
                    if (hashes.get(rf) != null) {
                        for (int i = 0; i < leftColumns.length; i++) {
                            for (int idx : hashes.get(rf)) {
                                // first add tuples from left column
                                result.get(i).add(leftColumns[i].column[idx]);
                            }
                        }
                        for (int i = leftColumns.length; i < leftColumns.length+rightColumns.length-1; i++) {
                            if (i != leftColumns.length+rightFieldNo) {
                                // then tuples from right column
                                result.get(i).add(rightColumns[i-leftColumns.length].column[index]);
                            }
                        }
                    }
                    index++;
                }
                rightColumns = rightChild.next();
            }
            leftColumns = leftChild.next();
        }

        // reconstruction of the whole columns
        for(int i = 0; i < result.size(); i++) {
            columns[i] = new DBColumn(result.get(i).toArray(), dtArray[i]);
        }

		return columns;
	}

	@Override
	public void close() {
        leftChild.close();
        leftChild.close();
	}
}
