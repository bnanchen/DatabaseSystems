package ch.epfl.dias.ops.block;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;

import java.util.ArrayList;
import java.util.HashMap;

public class Join implements BlockOperator {

    private BlockOperator leftChild;
    private BlockOperator rightChild;
    private int leftFieldNo;
    private int rightFieldNo;

	public Join(BlockOperator leftChild, BlockOperator rightChild, int leftFieldNo, int rightFieldNo) {
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftFieldNo = leftFieldNo;
        this.rightFieldNo = rightFieldNo;
	}

	public DBColumn[] execute() {
        HashMap<Object, ArrayList<Integer>> hashes = new HashMap<>();
        DBColumn[] leftColumn = leftChild.execute();
        Object[] leftFields = leftColumn[leftFieldNo].column;
        int index = 0;
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

        DBColumn[] rightColumn = rightChild.execute();
        ArrayList<ArrayList<Object>> result = new ArrayList<>();
        for (int i = 0; i < leftColumn.length+rightColumn.length-1; i++) {
            ArrayList<Object> col = new ArrayList<>();
            result.add(col);
        }
        Object[] rightFields = rightColumn[rightFieldNo].column;
        index = 0;
        for (Object rf : rightFields) {
            if (hashes.get(rf) != null) {
                for (int i = 0; i < leftColumn.length; i++) {
                    for (int idx : hashes.get(rf)) {
                        // first add tuples from left column
                        result.get(i).add(leftColumn[i].column[idx]);
                    }
                }
                for (int i = leftColumn.length; i < leftColumn.length+rightColumn.length-1; i++) {
                    if (i != leftColumn.length+rightFieldNo) {
                        // then tuples from right column
                        result.get(i).add(rightColumn[i-leftColumn.length].column[index]);
                    }
                }
            }
            index++;
        }

        // reconstruction of the whole columns
        DBColumn[] columns = new DBColumn[leftColumn.length+rightColumn.length-1];
        DataType[] dtArray = new DataType[leftColumn.length+rightColumn.length-1];
        for (int i = 0; i < leftColumn.length+rightColumn.length-1; i++) {
            if (i < leftColumn.length) {
                dtArray[i] = leftColumn[i].type;
            } else {
                if (i != leftColumn.length+rightFieldNo) {
                    dtArray[i] = rightColumn[i-leftColumn.length].type;
                }
            }
        }

        for (int i = 0; i < result.size(); i++) {
            columns[i] = new DBColumn(result.get(i).toArray(), dtArray[i]);
        }
		return columns;
	}
}
