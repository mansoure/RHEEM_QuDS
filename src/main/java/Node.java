import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;

import java.util.Collection;

/**
 * Created by emansour on 2/27/17.
 */
public class Node {

    String colName;
    String tableName;
    Collection<Record> colValues;
    Collection<Record> colDistValues;

    Table table;

    public Node(String tName, String cName, JavaPlanBuilder builder){
        tableName =  tName;
        colName =  cName;

        table = new Table(tableName,1,",");
        table.setCOLUMN(colName);

        colValues= table.getColumnValues(builder,colName);
        colDistValues= table.getColumnDistinctValues(builder,colName);

    }
}
