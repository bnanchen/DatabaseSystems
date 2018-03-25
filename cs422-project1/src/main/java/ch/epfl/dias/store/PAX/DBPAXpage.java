package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

    public DBTuple[] minipages;
    public DataType[] types;
    public int minipagenumb;
    public boolean eof;

    public DBPAXpage(DBTuple[] minipages, DataType[] types, int tuplesPerPage) {
        this.minipages = minipages;
        this.types = types;
        this.minipagenumb = tuplesPerPage;
        this.eof = false;
    }

    public DBPAXpage() {
        DBTuple emptyTuple = new DBTuple();
        this.minipages = new DBTuple[]{emptyTuple};
        this.minipagenumb = 1;
        this.eof = true;
    }
}
