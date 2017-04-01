import org.qcri.rheem.api.FilterDataQuantaBuilder;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.basic.operators.TableSource;
import org.qcri.rheem.basic.types.RecordType;
import org.qcri.rheem.java.operators.JavaTableSource;
import org.qcri.rheem.postgres.operators.PostgresTableSource;
import org.qcri.rheem.spark.operators.SparkTableSource;

import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.StreamSupport;

/**
 * Created by emansour on 2/15/17.
 */
public class Table {

   // public static  String TABLE = createUri("/main/resources/Employee_Salaries2015.csv");
   // public static  String COLUMN = "Department Name";
   // Type: RDB=0, CSV=1,

    private   String TABLE ;
    private   String delimiter;
    private   int Type ;
    private TableSource tableSource;
    public static  String COLUMN;

    public Table(){}


    public Table(String TablePath, int T, String deli){
        TABLE = createUri(TablePath);
       // TABLE = TablePath;
        Type = T;
        delimiter = deli;

          tableSource = new TableSource(TABLE, ",");
        // tableSource = new SparkTableSource(TABLE, ",");
       // tableSource = new JavaTableSource(TABLE, ",");
       // tableSource = new PostgresTableSource(TABLE);

//        if(Type == 1)
//        tableSource = new JavaTableSource(TABLE);
//        else if (Type == 0)
//        tableSource = new PostgresTableSource(TABLE);
    }

    public void setCOLUMN(String name){
        COLUMN = name;
    }

    public void getTableSnippet(JavaPlanBuilder builder, int sampleSize){

        //RecordType table_schema = (RecordType)tableSource.getType().getDataUnitType();
        RecordType table_schema = tableSource.getSchema();

        FilterDataQuantaBuilder<Record> table_i = builder
                .readTable(tableSource)
                .asRecords().filter(r-> r.size()==table_schema.getFieldNames().length);

        int indexCOL_i =   table_schema.getIndex(COLUMN);

        int [] indexesProjectedCol_i = getIndexesProjectedCol(indexCOL_i);

        Collection<Record> results_i =
                table_i.collect();
        //System.out.println("=== Sample of "+TABLE+"==========================");
        //System.out.println("number of results is " + results_i.size());
        //System.out.println(Arrays.toString(table_schema.getFieldNames()));
        //System.out.println(print(results_i,sampleSize));


    }

    public Collection<Record> getColumnDistinctValues(JavaPlanBuilder builder, String ColName){

        COLUMN = ColName;

        //RecordType table_schema = (RecordType)tableSource.getType().getDataUnitType();
        RecordType table_schema = tableSource.getSchema();

        FilterDataQuantaBuilder<Record> table_i = builder
                .readTable(tableSource)
                .asRecords().filter(r-> r.size()==table_schema.getFieldNames().length);

        int indexCOL_i =   table_schema.getIndex(COLUMN);

        Collection<Record> results =
                table_i.map(Tuple -> new Record(Tuple.getField(indexCOL_i)))
                        .distinct()
                        .collect();



//       System.out.println("=== Sample of "+TABLE+"==========================");
//       System.out.println("the count of Distinct Values is " + results.size());
//        System.out.println(print(results,38));

        return results;
    }

    public Collection<Record> getColumnValues(JavaPlanBuilder builder, String ColName){

        COLUMN = ColName;

        //RecordType table_schema = (RecordType)tableSource.getType().getDataUnitType();
        RecordType table_schema = tableSource.getSchema();

        System.out.println(">>> getColumnValues for "+tableSource.getTableName());

        FilterDataQuantaBuilder<Record> table_i = builder
                .readTable(tableSource)
                //.asRecords().filter(r-> r.size()==table_schema.getFieldNames().length);
                .asRecords().filter(r-> true);

        int indexCOL_i =   table_schema.getIndex(COLUMN);

        System.out.println("indexCOL_i is " + indexCOL_i + " of: "+COLUMN);

        Collection<Record> results =
                table_i.map(Tuple -> new Record(Tuple.getField(indexCOL_i)))
                        .collect();

        System.out.println(">>> getColumnValues ...Done");

        System.out.println("=== Sample of "+TABLE+"==========================");
        System.out.println("the count of Column Values is " + results.size());
//        System.out.println(print(results,30));

        return results;

    }

    private String createUri(String resourcePath) {
        try {
            return Table.class.getResource(resourcePath).toURI().toString();
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Illegal URI.", e);
        }

    }

    public static String print(Collection<Record> results, int flag ){
        // if flag is -1, print all otherwise print the sample size
        String rows="";
        int no_printed_rows=0;
        for (Record r : results){ // loop over the results
            int no_results = r.size();

            // print row
            for(int x=0; x < no_results; x++){
                rows = rows+ r.getField(x);
                if(x+1<no_results)
                    rows = rows+ ",";
                else
                    rows = rows+ "\n";
            }

            no_printed_rows++;
            if (flag!=-1 && no_printed_rows >flag )
                break;

        }
        return rows;
    }

    private static int [] getIndexesProjectedCol(int indexCOL){
        // we project the first 2 columns plus the join column
        // if the join column one of the first 2, we project the first 3
        int [] IndexesProjectedCol = new int[3];
        IndexesProjectedCol[0]=0;
        IndexesProjectedCol[1]=1;

        if (indexCOL <2)
            IndexesProjectedCol[2]=2;
        else
            IndexesProjectedCol[2]=indexCOL;

        return IndexesProjectedCol;
    }
}
