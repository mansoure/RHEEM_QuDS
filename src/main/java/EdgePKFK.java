import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.basic.data.Record;

/**
 * Created by emansour on 2/27/17.
 */
public class EdgePKFK {

    int EID;
    String Edge ;
    Node node1;
    Node node2;

    int col1ValuesSize;
    int col1DistValuesSize;


    int col2ValuesSize;
    int col2DistValuesSize;


    float uniquenessRatioN1;
    float uniquenessRatioN2;
    float colNameSimilarityRatio;
    int   beSubsetString;
    float coverageRatio;
    float averageLenDiffRatio;
    float tableSizeRatio;
    double KolmogorovSmirnovTest;

    public EdgePKFK(int ID, String tName1, String cName1, String tName2, String cName2,JavaPlanBuilder builder){

        EID = ID;
        Edge = tName1.substring(tName1.lastIndexOf("/")+1,tName1.length())
                +"@"+cName1+";"+
                tName2.substring(tName2.lastIndexOf("/")+1,tName2.length())
                +"@"+cName2;


        System.out.println(">>> for EID:"+ EID+" create nodes");

        node1 = new Node(tName1, cName1,builder);
        node2 = new Node(tName2, cName2,builder);

        System.out.println(">>> for EID:"+ EID+" nodes has created");

        uniquenessRatioN1 = PKFKFeatures.getUniquenessRatio(node1.colValues.size(), node1.colDistValues.size());
        uniquenessRatioN2 = PKFKFeatures.getUniquenessRatio(node2.colValues.size(), node2.colDistValues.size());

        if (uniquenessRatioN1 > uniquenessRatioN2)
        {//swap to have the assumed FK column first
            Node temp = node1;
            node1 = node2;
            node2 = temp;
            temp = null;

            float tempUniq = uniquenessRatioN1;

            uniquenessRatioN1 = uniquenessRatioN2;
            uniquenessRatioN2 = tempUniq;

            String EdgePart1 = Edge.substring(0, Edge.indexOf(";"));
            String EdgePart2 = Edge.substring(Edge.indexOf(";")+1, Edge.length());
            Edge = EdgePart2+";"+EdgePart1;
            System.out.println(">>> we did sawp in PairID: "+ EID);

        }

        col1ValuesSize = node1.colValues.size();
        col1DistValuesSize = node1.colDistValues.size();

        System.out.println(">>> for EID:"+ EID+" we got col size");

        col2ValuesSize = node2.colValues.size();
        col2DistValuesSize = node2.colDistValues.size();

        node1.colValues = null;
        node2.colValues = null;

        coverageRatio = PKFKFeatures.getCoverageRatio (builder, node1.colDistValues, node2.colDistValues);
        averageLenDiffRatio = PKFKFeatures.getRatioOfValueLenDiff( builder, node1.colDistValues, node2.colDistValues);

        System.out.println(">>> for EID:"+ EID+" we got col size");

        node1.colDistValues = null;
        node2.colDistValues = null;

        colNameSimilarityRatio = PKFKFeatures.getSimilarityBetween(node1.colName, node2.colName);
        beSubsetString = PKFKFeatures.isSubsetString(node1.colName, node2.colName);

        tableSizeRatio = PKFKFeatures.getTableSizeRatio(col1ValuesSize, col2ValuesSize);



//        KolmogorovSmirnovTest = PKFKFeatures.getKolmogorovSmirnovTest(builder,node1.colDistValues, node2.colDistValues);

    }

    public static String getPKFKFeaturesHeader(){
        String header =
                "JPID" + ", " +
                "JoinPath" + ", " +
                "uniquenessRatioN1" + ", " +
                "uniquenessRatioN2" + ", " +
                "colNameSimilarityRatio" + ", " +
                "isSubsetString" + ", " +
                "coverageRatio" + ", " +
                "averageLenDiffRatio" + ", " +
                "tableSizeRatio"
//                + ", " +"KolmogorovSmirnovTest"
                ;

        return header;
    }

    public String getPKFKFeatures(){


        String featuresValues =
                        EID + ", " +
                        "\""+Edge + "\""+ ", " +
                        uniquenessRatioN1 + ", " +
                        uniquenessRatioN2 + ", " +
                        colNameSimilarityRatio + ", " +
                        beSubsetString+ ", " +
                        coverageRatio + ", " +
                        averageLenDiffRatio + ", " +
                        tableSizeRatio
//                        + ", " +KolmogorovSmirnovTest
                        ;

        return featuresValues;
    }


}
