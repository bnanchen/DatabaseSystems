package ch.epfl.dias.ops.volcano;

import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.row.DBTuple;

public class Select implements VolcanoOperator {

    // TODO: Add required structures
    private VolcanoOperator child;
    private BinaryOp op;
    private int fieldNo;
    private int value;

    public Select(VolcanoOperator child, BinaryOp op, int fieldNo, int value) {
        // TODO: Implement
        this.child = child;
        this.op = op;
        this.fieldNo = fieldNo;
        this.value = value;
    }

    @Override
    public void open() {
        // TODO: Implement
        child.open();
    }

    @Override
    public DBTuple next() {
        // TODO: Implement
        boolean testResult = false;
        DBTuple next = child.next();
        while (!testResult && !next.eof) {
            int compareTo = next.getFieldAsInt(fieldNo);
            switch (op) {
                case GT: testResult = compareTo > value;
                    break;
                case NE: testResult = compareTo != value;
                    break;
                case EQ: testResult = compareTo == value;
                    break;
                case GE: testResult = compareTo >= value;
                    break;
                case LE: testResult = compareTo <= value;
                    break;
                case LT: testResult = compareTo < value;
                    break;
                default: throw new IllegalArgumentException("Not a correct test operand.");
            }
            if (testResult) {
                return next;
            }
            next = child.next();
        }

        return next;
    }

    @Override
    public void close() {
        // TODO: Implement
        child.close();
    }
}
