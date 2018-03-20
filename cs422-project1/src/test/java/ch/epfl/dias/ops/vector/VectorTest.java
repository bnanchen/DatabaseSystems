package ch.epfl.dias.ops.vector;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;
import ch.epfl.dias.store.row.RowStore;

import org.junit.Before;
import org.junit.Test;

public class VectorTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;

    ColumnStore columnstoreData;
    ColumnStore columnstoreOrder;
    ColumnStore columnstoreLineItem;

    @Before
    public void init() {

        schema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.INT,
                DataType.INT, DataType.INT, DataType.INT, DataType.INT };

        orderSchema = new DataType[] { DataType.INT, DataType.INT, DataType.STRING, DataType.DOUBLE, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.INT, DataType.STRING };

        lineitemSchema = new DataType[] { DataType.INT, DataType.INT, DataType.INT, DataType.INT, DataType.DOUBLE,
                DataType.DOUBLE, DataType.DOUBLE, DataType.DOUBLE, DataType.STRING, DataType.STRING, DataType.STRING,
                DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING, DataType.STRING };

        columnstoreData = new ColumnStore(schema, "input/data.csv", ",");
        columnstoreData.load();

        columnstoreOrder = new ColumnStore(orderSchema, "input/orders_small.csv", "\\|");
        columnstoreOrder.load();

        columnstoreLineItem = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        columnstoreLineItem.load();
    }

    @Test
    public void spTestData(){
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, 4);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 3, 6);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

        agg.open();

        // This query should return only one result
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 3);
    }

    @Test
    public void spTestOrder(){
        /* SELECT COUNT(*) FROM data WHERE col0 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 4);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 6);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

        agg.open();

        // This query should return only one result
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 1);
    }

    @Test
    public void spTestLineItem(){
        /* SELECT COUNT(*) FROM data WHERE col0 == 3 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 4);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

        agg.open();

        // This query should return only one result
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 3);
    }

    @Test
    public void joinTest1(){
        /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 4);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 4);

        /*Filtering on both sides */
        Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
        Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);

        Join join = new Join(selOrder,selLineitem,0,0);
        ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

        agg.open();
        //This query should return only one result
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 3);
    }

    @Test
    public void joinTest2(){
        /* SELECT COUNT(*) FROM lineitem JOIN order ON (o_orderkey = orderkey) WHERE orderkey = 3;*/

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 4);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 4);

        /*Filtering on both sides */
        Select selOrder = new Select(scanOrder, BinaryOp.EQ,0,3);
        Select selLineitem = new Select(scanLineitem, BinaryOp.EQ,0,3);

        Join join = new Join(selLineitem,selOrder,0,0);
        ProjectAggregate agg = new ProjectAggregate(join,Aggregate.COUNT, DataType.INT,0);

        agg.open();
        //This query should return only one result
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 3);
    }
}
