import org.qcri.rheem.api.JavaPlanBuilder;

import java.io.*;

/**
 * Created by emansour on 2/27/17.
 */
public class FKCFeaturesManager {

   private String dataPath;
   private String Edgesfilepath ;
   private String outFPath;

    public FKCFeaturesManager(String EdgesFile, String dPath, String oFile){


        Edgesfilepath = EdgesFile;
        dataPath =dPath;
        outFPath =oFile;

    }


    public void evaluateEdgeFeatures(String JB, int lineNo, JavaPlanBuilder builder) throws IOException {

        String[] twoColumns; //Table@Col;Table@Col
        String[] Part1;
        String[] Part2;
        String Table1 = "";
        String Table2 = "";
        String Column1="";
        String Column2="";

        File file = new File(outFPath);

        //file.delete();
        // if file does not exists, then create it
        //*
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("output file is created at "+file.getAbsoluteFile());
        }
        //*/

        FileWriter fwriter = new FileWriter(outFPath, true);
        PrintWriter outputfile = new PrintWriter(fwriter);

        if(lineNo==1) {
            outputfile.println(EdgePKFK.getPKFKFeaturesHeader());
            outputfile.flush();
        }

        EdgePKFK edge;

//                System.out.println("## " + JB);
//                outputfile.println(); //new line
//                outputfile.flush();

                //outputfile.print("\""+JB+"\"");

                twoColumns = JB.split(";");
                Part1=twoColumns[0].split("@");
                Table1 = Part1[0];
                Column1 = Part1[1].replace("#"," ");

                Part2=twoColumns[1].split("@");
                Table2 = Part2[0];
                Column2 = Part2[1];



                System.out.println(">>> PairID: "+lineNo+", "+dataPath+Table1+", "+ Column1+", "+dataPath+Table2+", "+ Column2);

                edge = new  EdgePKFK(lineNo,dataPath+Table1, Column1, dataPath+Table2, Column2, builder);

                outputfile.println(edge.getPKFKFeatures());
                System.out.println(">>> the features of PairID: "+lineNo+" are "+edge.getPKFKFeatures());

                lineNo++;

                edge.node1=null;
                edge.node2=null;
                edge = null;

                outputfile.flush();


            //fwriter.close();
            outputfile.close();

        return ;
    }



    public void evaluateEdgesFeatures(JavaPlanBuilder builder) throws IOException {

        /*
        String dataPath= "/Chembl_csv/";
        String filepath = "/Users/emansour/elab/DAGroup/Rheem/github/dCiv-rheem/out/production/dCiv-rheem/main/resources/dataGovSmall36/uniq_edgs/pair13_uniq.txt";
        String outFPath= "/Users/emansour/elab/DAGroup/Rheem/github/dCiv-rheem/out/production/dCiv-rheem/main/resources/dataGovSmall36/_evaluation_Out.csv";
        //*/

        String[] twoColumns; //Table@Col;Table@Col
        String[] Part1;
        String[] Part2;
        String Table1 = "";
        String Table2 = "";
        String Column1="";
        String Column2="";

        File file = new File(outFPath);

        file.delete();
        // if file does not exists, then create it
        //*
        if (!file.exists()) {
            file.createNewFile();
            System.out.println("output file is created at "+file.getAbsoluteFile());
        }
        //*/

        FileWriter fwriter = new FileWriter(outFPath);
        PrintWriter outputfile = new PrintWriter(fwriter);
        outputfile.println(EdgePKFK.getPKFKFeaturesHeader());
        outputfile.flush();

        EdgePKFK edge;


        try(BufferedReader br = new BufferedReader(new FileReader(Edgesfilepath))) {

            int lineNo=0;

            for(String JB; (JB = br.readLine()) != null; ) { // process the line.

//                System.out.println("## " + JB);
                outputfile.println(); //new line
                outputfile.flush();

                //outputfile.print("\""+JB+"\"");

                twoColumns = JB.split(";");
                Part1=twoColumns[0].split("@");
                Table1 = Part1[0];
                Column1 = Part1[1].replace("#"," ");

                Part2=twoColumns[1].split("@");
                Table2 = Part2[0];
                Column2 = Part2[1];

                lineNo++;

                System.out.println(">>> PairID: "+lineNo+", "+dataPath+Table1+", "+ Column1+", "+dataPath+Table2+", "+ Column2);

                edge = new  EdgePKFK(lineNo,dataPath+Table1, Column1, dataPath+Table2, Column2, builder);

                outputfile.println(edge.getPKFKFeatures());

                edge.node1=null;
                edge.node2=null;
                edge = null;

                outputfile.flush();



            }

            //fwriter.close();
            outputfile.close();

        }


        return ;
    }

}
