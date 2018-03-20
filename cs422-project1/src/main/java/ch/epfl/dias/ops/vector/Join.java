package ch.epfl.dias.ops.vector;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

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
        
		return null;
	}

	@Override
	public void close() {
		// TODO: Implement
        leftChild.close();
        leftChild.close();
	}
}
