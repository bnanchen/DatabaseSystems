package ch.epfl.dias.store.PAX;

import ch.epfl.dias.store.DataType;
import ch.epfl.dias.store.column.DBColumn;
import ch.epfl.dias.store.row.DBTuple;

public class DBPAXpage {

	// TODO: Implement
    public DBTuple[] minipages;
    public DataType[] types;
    public int minipagenumb;
    public boolean eof;

    public DBPAXpage(DBTuple[] minipages, DataType[] types, int tuplesPerPage) {
        this.minipages = minipages;
        this.types = types;
        this.minipagenumb = tuplesPerPage;
    }

    public DBPAXpage() {
        this.eof = true;
    }
}
