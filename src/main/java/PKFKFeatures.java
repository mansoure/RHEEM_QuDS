import org.apache.commons.lang3.StringUtils;
import org.apache.commons.math3.stat.inference.KolmogorovSmirnovTest;
import org.qcri.rheem.api.JavaPlanBuilder;
import org.qcri.rheem.api.LoadCollectionDataQuantaBuilder;
import org.qcri.rheem.basic.data.Record;
import org.qcri.rheem.basic.data.Tuple2;

import java.util.Arrays;
import java.util.Collection;

/**
 * Created by emansour on 2/27/17.
 */
public class PKFKFeatures {






    public static double[] convertToDArray( Collection<Record> set1){
        int ratio = 100;
        int num = set1.size()*ratio/100;
        double[] target = new double[num];
        int i = 0;
        while (set1.iterator().hasNext()) {
            target[i] = Double.parseDouble(set1.iterator().next().getString(0));
            i++;
            if (i>=num)
                break;

        }

        return target;
    }


    public static double getKolmogorovSmirnovTest (JavaPlanBuilder builder, Collection<Record> set1, Collection<Record> set2){


//        double[] array1 = {1.3,2.4,3.6,4,5,6,7,8,9,10};
//        double[] array2 = {1,2,3,4,5,6,7,8,9,10};

        double[] array1 = convertToDArray(set1);
        double[] array2 = convertToDArray(set2);


        KolmogorovSmirnovTest test = new KolmogorovSmirnovTest();

        return test.kolmogorovSmirnovTest(array1,array2);

    }



    public static float getTableSizeRatio (int totalSize1, int totalSize2){


        float ratio = -1;

        if(totalSize1>=totalSize2)
            ratio = (float) (totalSize1-totalSize2)/totalSize1;
        else
            ratio = (float) (totalSize2-totalSize1)/totalSize2;

//        System.out.println("ratio, "+ratio);
        return ratio;
    }

    public static float getRatioOfValueLenDiff (JavaPlanBuilder builder, Collection<Record> set1, Collection<Record> set2){


        float aveLen1 = getAveLen (builder, set1);
        float aveLen2 = getAveLen (builder, set2);
        float ratio = -1;

        if(aveLen1>=aveLen2)
            ratio = (aveLen1-aveLen2)/aveLen1;
        else
            ratio = (aveLen2-aveLen1)/aveLen2;

//        System.out.println("ratio, "+ratio);
        return ratio;
    }

    public static float getAveLen (JavaPlanBuilder builder, Collection<Record> set1){


        // Execute the job.
        final Collection<Tuple2<String, Integer>> outputValues = builder.loadCollection(set1)
                .mapPartitions(partition -> {
                    int TotalLen = 0;
                    for (Record r : partition) {
                        TotalLen = TotalLen + r.getString(0).length();
                    }
                    return Arrays.asList(
                            new Tuple2<>("TotalLen", TotalLen)
                    );
                })
                .reduceByKey(Tuple2::getField0, (t1, t2) -> new Tuple2<>(t1.getField0(), t1.getField1() + t2.getField1()))
                .collect();

        int tLen = outputValues.iterator().next().field1.intValue();

        float aveLen = (float)tLen/set1.size();

//        System.out.println(outputValues.size()+ " total len, "+tLen+" set1.size(), "+set1.size());

//        System.out.println("aveLen, "+aveLen);
        return aveLen;
    }


    public static float getCoverageRatio (JavaPlanBuilder builder, Collection<Record> set1, Collection<Record> set2){

        Collection<Record> intersection = getIntersect(builder,set1, set2);
        int max = set1.size();
        if (max < set2.size())
            max = set2.size();

        return  ((float) intersection.size()/max);
    }

    public static Collection<Record> getIntersect (JavaPlanBuilder builder, Collection<Record> set1, Collection<Record> set2){


        // Execute the job.
        final LoadCollectionDataQuantaBuilder<Record> dataQuanta1 = builder.loadCollection(set1);
        final LoadCollectionDataQuantaBuilder<Record> dataQuanta2 = builder.loadCollection(set2);
        final Collection<Record> result = dataQuanta1.intersect(dataQuanta2).collect();

//        System.out.println(Table.print(set2,30));
//        System.out.println("result.size() is "+result.size());
//        System.out.println(Table.print(result,result.size()));

//        System.out.println(set1.size()+", "+ set2.size()+", "+ (float)result.size()/set1.size()+", "+ (float)result.size()/set2.size() );
        return result;
    }


    public static float getSimilarityBetween(String colName1, String colName2){

        float edR= StringUtils.getLevenshteinDistance(colName1,colName2);

        int maxLen =  colName1.length();
        if (colName2.length() > colName1.length())
            maxLen = colName2.length();

        return 1- edR/maxLen;
    }

    public static int isSubsetString(String colName1, String colName2){

        int flag=-1;

        if (colName1.contains(colName2) || colName2.contains(colName1))
            flag = 1;
        else
            flag = 0;



        return flag;
    }

    public static float getUniquenessRatio(int colSize, int distinctValuesSize ){

        float uniquenessRatio = (float) distinctValuesSize/colSize;

        return uniquenessRatio;
    }
}
