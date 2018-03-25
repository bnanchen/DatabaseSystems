package ch.epfl.dias.ops.vector;

import static org.junit.Assert.*;

import ch.epfl.dias.ops.Aggregate;
import ch.epfl.dias.ops.BinaryOp;
import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.ColumnStore;
import ch.epfl.dias.store.column.DBColumn;

import org.junit.Before;
import org.junit.Test;

public class VectorTest {

    DataType[] orderSchema;
    DataType[] lineitemSchema;
    DataType[] schema;

    ColumnStore columnstoreData;
    ColumnStore columnstoreOrder;
    ColumnStore columnstoreLineItem;
    ColumnStore columnstoreOrderBig;
    ColumnStore columnstoreLineItemBig;

    int vectorSize = 100;

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

        columnstoreOrderBig = new ColumnStore(orderSchema, "input/orders_big.csv", "\\|");
        columnstoreOrderBig.load();

        columnstoreLineItemBig = new ColumnStore(lineitemSchema, "input/lineitem_small.csv", "\\|");
        columnstoreLineItemBig.load();
    }

    @Test
    public void spTestData(){
        /* SELECT COUNT(*) FROM data WHERE col4 == 6 */
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreData, vectorSize);
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
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, vectorSize);
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
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.Select sel = new ch.epfl.dias.ops.vector.Select(scan, BinaryOp.EQ, 0, 3);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(sel, Aggregate.COUNT, DataType.INT, 2);

        agg.open();

        // This query should return only one result
        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 3);
    }

    @Test
    public void testOrderSumInt() {
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.INT, 0);

        agg.open();

        DBColumn[] result = agg.next();
        int output = result[0].getAsInteger()[0];
        assertTrue(output == 17);
    }

    @Test
    public void testOrderSumDouble() {
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.SUM, DataType.DOUBLE, 5);

        agg.open();

        DBColumn[] result = agg.next();
        double output = result[0].getAsDouble()[0];
        assertTrue(output == 454890.80);
    }

    @Test
    public void testOrderAvg() {
        // with double
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.AVG, DataType.DOUBLE, 5);

        // with int
        ch.epfl.dias.ops.vector.Scan scan_ = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg_ = new ch.epfl.dias.ops.vector.ProjectAggregate(scan_, Aggregate.AVG, DataType.INT, 0);


        agg.open();
        agg_.open();

        DBColumn[] result = agg.next();
        DBColumn[] result_ = agg_.next();
        double output = result[0].getAsDouble()[0];
        System.out.println(result_[0].getAsInteger()[0]);
        int output_ = result_[0].getAsInteger()[0];
        assertTrue(output == 45489.08);
        assertTrue(output_ == 1);
    }

    @Test
    public void testOrderMaxDouble() {
        // with double
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MAX, DataType.DOUBLE, 6);

        agg.open();

        DBColumn[] resultDouble = agg.next();
        double outputDouble = resultDouble[0].getAsDouble()[0];
        assertTrue(outputDouble == 0.10);
    }

    @Test
    public void testOrderMaxInt() {
        // with int
        ch.epfl.dias.ops.vector.Scan scanInt = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate aggInt = new ch.epfl.dias.ops.vector.ProjectAggregate(scanInt, Aggregate.MAX, DataType.INT, 0);

        aggInt.open();

        DBColumn[] resultInt = aggInt.next();
        int outputInt = resultInt[0].getAsInteger()[0];
        assertTrue(outputInt == 3);
    }

    @Test
    public void testOrderMaxString() {
        // with string
        ch.epfl.dias.ops.vector.Scan scanString = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate aggString = new ch.epfl.dias.ops.vector.ProjectAggregate(scanString, Aggregate.MAX, DataType.STRING, 15);

        aggString.open();

        DBColumn[] resultString = aggString.next();
        String outputString = resultString[0].getAsString()[0];
        assertTrue(outputString.equals("ven requests. deposits breach a"));
    }

    @Test
    public void testOrderMinDouble() {
        // with double
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 6);

        agg.open();

        DBColumn[] resultDouble = agg.next();
        double outputDouble = resultDouble[0].getAsDouble()[0];
        System.out.println(outputDouble);
        assertTrue(outputDouble == 0.0);
    }

    @Test
    public void testOrderMinInt() {
        // with int
        ch.epfl.dias.ops.vector.Scan scanInt = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate aggInt = new ch.epfl.dias.ops.vector.ProjectAggregate(scanInt, Aggregate.MIN, DataType.INT, 0);

        aggInt.open();

        DBColumn[] resultInt = aggInt.next();
        int outputInt = resultInt[0].getAsInteger()[0];
        assertTrue(outputInt == 1);
    }

    @Test
    public void testOrderMinString() {
        // with string
        ch.epfl.dias.ops.vector.Scan scanString = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate aggString = new ch.epfl.dias.ops.vector.ProjectAggregate(scanString, Aggregate.MIN, DataType.STRING, 15);

        aggString.open();

        DBColumn[] resultString = aggString.next();
        String outputString = resultString[0].getAsString()[0];
        System.out.println(outputString);
        assertTrue(outputString.equals("arefully slyly ex"));
    }

    @Test
    public void testProjectI() {
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, vectorSize);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.vector.Project projectOrder = new ch.epfl.dias.ops.vector.Project(scanOrder, toProject);

        projectOrder.open();

        DBColumn[] result = projectOrder.next();
        assertTrue(result[0].getAsInteger()[0] == 1);
    }

    @Test
    public void testProjectII() {
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreData, vectorSize);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.vector.Project projectOrder = new ch.epfl.dias.ops.vector.Project(scanOrder, toProject);

        projectOrder.open();

        DBColumn[] result = projectOrder.next();
        int index = 1;
        while(!result[0].eof) {
            assertTrue(result[0].getAsInteger()[0] == index);
            result = projectOrder.next();
            index++;
        }
    }

    @Test
    public void testSelectProject() {
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, vectorSize);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.vector.Project project = new ch.epfl.dias.ops.vector.Project(scanOrder, toProject);
        ch.epfl.dias.ops.vector.Select select = new ch.epfl.dias.ops.vector.Select(project,BinaryOp.EQ, 0, 1);

        select.open();

        DBColumn[] result = select.next();
        assertTrue(result[0].getAsInteger()[0] == 1);
    }

    @Test
    public void testSelectProjectBig() {
        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrderBig, vectorSize);
        int[] toProject = {0, 1, orderSchema.length-1};
        ch.epfl.dias.ops.vector.Project project = new ch.epfl.dias.ops.vector.Project(scanOrder, toProject);
        ch.epfl.dias.ops.vector.Select select = new ch.epfl.dias.ops.vector.Select(project,BinaryOp.EQ, 0, 1);

        select.open();

        DBColumn[] result = select.next();
        assertTrue(result[0].getAsInteger()[0] == 1);
    }

    @Test
    public void testOrderMinDoubleBig() {
        // with double
        ch.epfl.dias.ops.vector.Scan scan = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItemBig, vectorSize);
        ch.epfl.dias.ops.vector.ProjectAggregate agg = new ch.epfl.dias.ops.vector.ProjectAggregate(scan, Aggregate.MIN, DataType.DOUBLE, 6);

        agg.open();

        DBColumn[] resultDouble = agg.next();
        double outputDouble = resultDouble[0].getAsDouble()[0];
        assertTrue(outputDouble == 0.0);
    }

    @Test
    public void joinTest1(){
        /* SELECT COUNT(*) FROM order JOIN lineitem ON (o_orderkey = orderkey) WHERE orderkey = 3;*/

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 100);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 100);

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

        ch.epfl.dias.ops.vector.Scan scanOrder = new ch.epfl.dias.ops.vector.Scan(columnstoreOrder, 100);
        ch.epfl.dias.ops.vector.Scan scanLineitem = new ch.epfl.dias.ops.vector.Scan(columnstoreLineItem, 100);

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
