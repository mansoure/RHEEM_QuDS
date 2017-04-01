import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.api.LoadCollectionDataQuantaBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.api.RheemContext;
import org.qcri.rheem.java.Java;
import org.qcri.rheem.postgres.Postgres;
import org.qcri.rheem.spark.Spark;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;

/**
 * Created by emansour on 2/15/17.
 */
public class QuDS_Master {



    public static void main (String... args) {

        // How to allow RHEEM to connect to more than one Database??
        //Connection to a single Database
        Configuration postgresConfiguration = new Configuration();
        postgresConfiguration.setProperty("rheem.postgres.jdbc.url", "jdbc:postgresql://localhost:5432/rheemdb");
        postgresConfiguration.setProperty("rheem.postgres.jdbc.user", "postgres");
        postgresConfiguration.setProperty("rheem.postgres.jdbc.password", "postgres");

        // Execute job.
        final RheemContext rheemCtx = new RheemContext(postgresConfiguration)
                .with(Java.basicPlugin())
                //.with(Postgres.plugin())
                .with(Spark.basicPlugin()
                );
        JavaPlanBuilder builder = new JavaPlanBuilder(rheemCtx, "rheemQuDS");

//        test(builder);

 //*
        long startTime = System.currentTimeMillis();

        FKCFeaturesManager fManager=null;

        String FKEdge="";
        int lineNo=0;
        String outputFile="";

if (args.length >=3) {

    System.out.println("========================Features for one Join Path at a time=============================");
//    System.out.println("args[0] " +args[0]);
//    System.out.println("args[0] " +args[1]);
//    System.out.println("args[0] " +args[2]);
//    System.exit(0);
    FKEdge = args[0];
    lineNo = Integer.parseInt(args[1]);
    outputFile = args[2];
    fManager = new FKCFeaturesManager("", "/Chembl_csv/", outputFile);
    System.out.println("main 3 params");
    try {

        System.out.println("========================Calculate the features for the give path=============================");
        fManager.evaluateEdgeFeatures(FKEdge, lineNo, builder);

    } catch (IOException e) {
        e.printStackTrace();
    }

}
else try {
    System.out.println("============================Features for given file==========================================");
    fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/mitdw_test_edges.txt", "/Chembl_csv/", "/Users/emansour/elab/DAGroup/DataCivilizer/Aurum-GitHub/aurum-datadiscovery/ML_FKC_Detection/temp.csv");
//        fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FKC_Edges_test.txt", "/Chembl_csv/","/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_TEST_CASES_features_values.csv" );
//       FKCFeaturesManager fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FKC_Edges.txt", "/Chembl_csv/","/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FKC_Edges_features_values.csv" );

//        FKCFeaturesManager fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_TP_sample_Human.txt", "/Chembl_csv/","/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_TP_sample_Human_features_values.csv" );

//        FKCFeaturesManager fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FP_sample.txt", "/Chembl_csv/","/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FP_sample_features_values.csv" );


//        fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FKC_Edges_69.txt", "/Chembl_csv/","/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FKC_Edges_69_features_values.csv" );

//        FKCFeaturesManager fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_TP_sample_Human.txt", "/Chembl_csv/","/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_TP_sample_Human_features_values.csv" );

//        FKCFeaturesManager fManager = new FKCFeaturesManager("/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FP_sample.txt", "/Chembl_csv/","/Users/emansour/elab/DAGroup/Rheem/github/RHEEM_QuDS/src/main/resources/chembl_FP_sample_features_values.csv" );

 //        EdgePKFK edge = new  EdgePKFK("compound_properties.csv", "molregno", "molecule_dictionary.csv", "molregno", builder);
//        EdgePKFK edge = new  EdgePKFK(1,"/Chembl_csv/target_relations.csv", "related_tid", "/Chembl_csv/target_dictionary.csv", "tid", builder);

//        EdgePKFK edge = new  EdgePKFK(1,"/Chembl_csv/target_relations.csv", "related_tid", "/Chembl_csv/target_dictionary.csv", "tid", builder);
//
//        System.out.println(edge.getPKFKFeaturesHeader());
//        System.out.println(edge.getPKFKFeatures());

    fManager.evaluateEdgesFeatures(builder);

} catch (IOException e) {
    e.printStackTrace();
}


        long endTime   = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        System.out.println(">>> total time of evaluating the features is "+(float)totalTime/1000);
        //*/


    }


    private static void test(JavaPlanBuilder builder){
        //String colName= "Department";
        //colName = "Department Name";


        String tableName;
        tableName = "Employee_Salaries2015.csv";
//        tableName ="molecule_dictionary.csv";

        String colName;
//        colName= "Gender";
//        colName= "molregno";
        colName = "Department";

        Table table_1 = new Table(tableName,1,",");
        table_1.setCOLUMN(colName);

        //table_1.getTableSnippet(builder,5);
        Collection<Record> colValues= table_1.getColumnValues(builder,colName);
        Collection<Record> colDistValues= table_1.getColumnDistinctValues(builder,colName);

        int colCount = colValues.size();
        int colDist  = colDistValues.size();
        float colDistRatio = (float)colDist/colCount;


        System.out.println(colName+", " + colCount+", " + colDist+", " + colDistRatio );

//        PKFKFeatures.getAveLen(builder,colDistValues);

//         getIntersect(builder,colValues, colDistValues);


        /*
        Table table_2 = new Table("Employee_Compensation.csv",1,",");
        Collection<Record> colDistValues1= table_2.getColumnDistinctValues(builder,"Other Salaries");
        Collection<Record> colDistValues2= table_2.getColumnDistinctValues(builder,"Salaries");


        double KSTest = PKFKFeatures.getKolmogorovSmirnovTest(builder,colDistValues1, colDistValues2);



        System.out.println("KSTest "+ KSTest);


        float ed = PKFKFeatures.getSimilarityBetween("Code","Dept Code");
        System.out.println("ED Similarity is "+ ed);
        //*/


    }

}
