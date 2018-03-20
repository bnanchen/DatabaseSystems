package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class Join implements VectorOperator {

	// TODO: Add required structures
	private VectorOperator leftChild;
	private VectorOperator rightChild;
	private int leftFieldNo;
	private int rightFieldNo;

	public Join(VectorOperator leftChild, VectorOperator rightChild, int leftFieldNo, int rightFieldNo) {
		// TODO: Implement
        this.leftChild = leftChild;
        this.rightChild = rightChild;
        this.leftFieldNo = leftFieldNo;
        this.rightFieldNo = rightFieldNo;
	}

	@Override
	public void open() {
		// TODO: Implement
        leftChild.open();
        rightChild.open();
	}

	@Override
	public DBColumn[] next() {
		// TODO: Implement
        DBColumn[] rightColumns = rightChild.next();
        DBColumn[] leftColumns = leftChild.next();
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
                for (Object rf : rightFields) {
                    if (hashes.get(rf) != null) {
                        for (int i = 0; i < leftColumns.length; i++) {
                            for (int idx : hashes.get(rf)) {
                                result.get(i).add() // TODO I was here!
                            }
                        }
                    }
                }
            }
        }
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
        leftChild.close();
        leftChild.close();
	}
}
