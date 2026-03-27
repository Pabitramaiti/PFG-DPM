package com.broadridge.ebcdic;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

//import com.fasterxml.jackson.databind.ObjectMapper;


@SuppressWarnings("unchecked")
public class EBCDICRecordToASCIItask implements Callable<RecordResult> {



    private String strEBCDIC = null;
    private String clientID = null;

    private int index = 0;

    private int record = 0;

    private long lengthOfChar = 0L;

    private RecordResult RR = new RecordResult();

    private volatile static boolean indexerFlag = false;

    private static int SEG_LENGTH_STARTIDX;

    private static int SEG_LENGTH_ENDIDX;

    private static int SEG_ID_STARTIDX;
    private static final Queue<JSONObject> records27200 = new ConcurrentLinkedQueue<>();
    // private static final Queue<JSONObject> records11200 = new ConcurrentLinkedQueue<>();
    private static final Queue<JSONObject> records50300 = new ConcurrentLinkedQueue<>();
    private static final Queue<JSONObject> records30100 = new ConcurrentLinkedQueue<>();
    private static Map<String, BufferedWriter> recordWriters = new ConcurrentHashMap<>();
    private static Map<String, AtomicInteger> recordCounters = new ConcurrentHashMap<>();
    // private static String tempDir = System.getProperty("java.io.tmpdir") + "records_temp/";
    private static String tempDir = System.getProperty("java.io.tmpdir") + "/records_temp/";
    private static boolean initialized = false;
    private static final ConcurrentHashMap<String, Set<String>> processedKeys = new ConcurrentHashMap<>();
    private static final Logger logger = LogManager.getLogger(EBCDICRecordToASCIItask.class);
    private final AccountFilterService accountFilterService;
	private final CurrencySuppressionService currencySuppressionService;


    static {

        SEG_LENGTH_STARTIDX = 1;

        SEG_LENGTH_ENDIDX = 2;

        SEG_ID_STARTIDX = 3;

    }



    public EBCDICRecordToASCIItask (String strIn, int index, int record, long lengthOfChar, Globals global,AccountFilterService accountFilterService) {

        this.strEBCDIC = strIn;

        this.index = index;

        this.record = record;

        this.lengthOfChar = lengthOfChar;
        this.accountFilterService = accountFilterService;
		this.currencySuppressionService = CurrencySuppressionService.getInstance();

    }

    public static int XBASE_REC_LENGTH_ENDIDX;

    public static int XBASE_TRID12_STARTIDX;

    public static int XBASE_TRID12_ENDIDX;

    static {

        XBASE_REC_LENGTH_ENDIDX = ConfigSplit.detailConfigMap.get(FileConverter_MT.XBASERECLEN).getEndIdx();

        XBASE_TRID12_STARTIDX = ConfigSplit.detailConfigMap.get(FileConverter_MT.XBASETRID).getStartIdx();

        XBASE_TRID12_ENDIDX = ConfigSplit.detailConfigMap.get(FileConverter_MT.XBASESUBTRID).getEndIdx();

    }


    @Override

    public RecordResult call() throws Exception {

        //NEW
        try {

            byte[] bytesEBCDIC = strEBCDIC.getBytes("CP819");

            String convertedASCII = new String(bytesEBCDIC, "Cp1047");

            String printOutput = "";

            StringBuilder sbOutput2 = new StringBuilder();

            String recordType = convertedASCII.substring(XBASE_TRID12_STARTIDX-1, XBASE_TRID12_ENDIDX);
            String accountNum = convertedASCII.substring(4, 12);

            // Extract currency from XBASE header (positions 13-15, indices 12-14)
            String currency = "";
            if (convertedASCII.length() >= 15) {
                currency = convertedASCII.substring(12, 15).trim();
                RR.currency = currency;
            }

            FileConverter_MT.totalRecords++;



            RR.recordType = "ETS" + recordType;



            if(ConfigSplit.dynamicTrids.contains(recordType)) {
                if (recordType.equals("2720")) {

                    JSONObject jObj2720 = new JSONObject();

                    load2720XBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "2720" , jObj2720 );
/////            	loadXBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "2720" , jObj2720 );

                    load2720Record(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "2720" , jObj2720);
                    RR.jObj2720 = jObj2720;
                    //    records27200.add(jObj2720);
                  //  addRecord27200(jObj2720);
                    //added filtering for lookup tables
                    if (shouldEmitAccountRecord(jObj2720)) {
                        addRecord27200(jObj2720);
                    }

                }else  if (recordType.equals("1120")) {
                    JSONObject jObj1120 = new JSONObject();
                    loadTridXBASERecoed(convertedASCII, strEBCDIC, "1120", jObj1120);
                    loadTrid(convertedASCII, strEBCDIC,recordType,jObj1120,ConfigSplit.detailConfigMap);
                    RR.jObj1120 = jObj1120;
                    //    records11200.add(jObj1120);
                } else {

                    JSONObject jObjCLDT = new JSONObject();
                    loadXBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, recordType , jObjCLDT);
                    loadCLDTRecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, recordType , jObjCLDT);
                }

            }

            else if(recordType.equals("0000")) {
                JSONObject jObjXbase = new JSONObject();
                JSONObject jObj0000 = new JSONObject();
                boolean trid0000 = false;
                for(String p : ConfigSplit.detailConfigMap.keySet()) {
                    if(p.contains("00000") && !trid0000) {
                        trid0000 = true;
                    }
                    else if(!p.contains("0000") && trid0000) {
                        break;
                    }
                    if(p.contains("XBASE") ) {
/*                    if(p.equals(FileConverter_MT.XBASERECLEN)) {
                        sbOutput2.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (strEBCDIC.indexOf(strEBCDIC.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETSXBASE")+"~"+"ETSXBASE");

                    }
*/
                        if(ConfigSplit.detailConfigMap.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap.get(p).getInType().equals("N")) {
                            String ss = normalize(convertedASCII.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx()));
//                        sbOutput2.append("~"+ss);
                            jObjXbase.put(p, ss.replace("\u0000", " "));
                        }
                        else if(ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP")) {

                            String ss = strEBCDIC.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }
//                        sbOutput2.append("~"+Integer.parseInt(q,16));
                            jObjXbase.put(p, Integer.parseInt(q,16));

                        }
                        else if(ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP3")) {

                            String ss = strEBCDIC.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                        sbOutput2.append("~" + unpackedData);
                            jObjXbase.put(p, unpackedData);
                        }
                    }
                    if( p.contains("00000")) {
/*                    if(p.matches("00000-.*SEGLEN")) {
                        sbOutput2.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (strEBCDIC.indexOf(strEBCDIC.substring(32+ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS00000")+"~"+"ETS00000");
                    }
*/
                        if(ConfigSplit.detailConfigMap.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap.get(p).getInType().equals("N")) {
                            String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap.get(p).getEndIdx()));
//                        sbOutput2.append("~"+ss);
                            jObj0000.put(p, ss.replace("\u0000", " "));
                            if(ConfigSplit.detailConfigMap.get(p).getFieldName().equals("00000-HDR-PROD-DATE-MM")) {
                                System.out.print("p -> " + p);
                                System.out.print("ss -> " + ss);
                                RR.historical_mm = ss;
                                
                             }
                             if(ConfigSplit.detailConfigMap.get(p).getFieldName().equals("00000-HDR-PROD-DATE-YY")) {
                                 System.out.print("p -> " + p);
                                 System.out.print("ss -> " + ss);
                                 RR.historical_yy = ss;
                             }                            
                        }

                        else if(ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP")) {
                            String ss = strEBCDIC.substring(32+ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                        sbOutput2.append("~"+Integer.parseInt(q,16));
                            jObj0000.put(p, Integer.parseInt(q,16));
                        }
                    }
                }

//            RR.strASCII = sbOutput2.toString();

                RR.jObjXbase0000 = jObjXbase;

                RR.jObj0000 = jObj0000;

                RR.isHeader = true;

            }


            else if(recordType.equals("0520")) {

                boolean trid0520 = false;

                JSONObject jObj0520 = new JSONObject();

                for(String p : ConfigSplit.detailConfigMap.keySet()) {

                    if(p.contains("0520") && !trid0520) {

                        trid0520 = true;

                    }

                    else if(!p.contains("0520") && trid0520) {

                        break;

                    }

                    if( p.contains("0520")) {

                        trid0520 = true;

/*                    if(p.equals("05200-CATCD-SEGLEN")) {

                        sbOutput2.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                                (strEBCDIC.indexOf(strEBCDIC.substring(32+ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap.get(p).getEndIdx())))

                                +(1))+"~"+ConfigSplit.recordCount.get("ETS05200")+"~"+"ETS0520");

                    }
*/
                        if(ConfigSplit.detailConfigMap.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap.get(p).getInType().equals("N")) {

                            String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap.get(p).getEndIdx()));

//                        sbOutput2.append("~"+ss);

                            jObj0520.put(p, ss.replace("\u0000", " "));

                        }

                        else if(ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP")) {

                            String ss = strEBCDIC.substring(32+ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap.get(p).getEndIdx());

                            String q = "";

                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                        sbOutput2.append("~"+Integer.parseInt(q,16));

                            jObj0520.put(p, Integer.parseInt(q,16));

                        }

                    }

                }

//            RR.strASCII = sbOutput2.toString();

                RR.jObj0520 = jObj0520;

                RR.isHeader = true;

            }



            else if(recordType.equals("0420")) {

                JSONObject jObj0420 = new JSONObject();

                JSONObject jObjOccur = null;

                JSONArray occurArray = new JSONArray();

                String occurValue = "";

                String occurKey = "";

                for(String p : ConfigSplit.detailConfigMap0420.keySet()) {

                    if( p.contains("0420")) {

                        if(ConfigSplit.detailConfigMap0420.get(p).isOccurs() && occurKey.equals("")) {
                            occurKey = ConfigSplit.detailConfigMap0420.get(p).getOccursArrayKey();
                        }

/*                    if(p.matches("04200-.*SEGLEN")) {

                        sbOutput2.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                                (strEBCDIC.indexOf(strEBCDIC.substring(32+ConfigSplit.detailConfigMap0420.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap0420.get(p).getEndIdx())))

                                +(1))+"~"+ConfigSplit.recordCount.get("ETS04200")+"~"+"ETS0420");

                    }
 */
                        if(ConfigSplit.detailConfigMap0420.get(p).isOccurs()) {

                            if(!occurValue.equals(p.substring(p.length() -2, p.length()))) {
                                if(jObjOccur != null && !jObjOccur.toString().equals("{}")) {
                                    occurArray.add(jObjOccur);
                                }
                                jObjOccur = new JSONObject();
                                occurValue = p.substring(p.length() -2, p.length());
                            }
                        }

                        if(ConfigSplit.detailConfigMap0420.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap0420.get(p).getInType().equals("N")) {

                            if(convertedASCII.length() > 32+ConfigSplit.detailConfigMap0420.get(p).getEndIdx()) {

                                String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap0420.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap0420.get(p).getEndIdx()));

//                            sbOutput2.append("~"+ss);

                                if(ConfigSplit.detailConfigMap0420.get(p).isOccurs()) {

                                    jObjOccur.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));

                                } else {

                                    jObj0420.put(p, ss.replace("\u0000", " "));

                                }

                            }
                        }

                        else if(ConfigSplit.detailConfigMap0420.get(p).getInType().equals("COMP")) {

                            if(strEBCDIC.length() >= 32+ConfigSplit.detailConfigMap0420.get(p).getEndIdx()) {

                                String ss = strEBCDIC.substring(32+ConfigSplit.detailConfigMap0420.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap0420.get(p).getEndIdx());

                                String q = "";

                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }

//                            sbOutput2.append("~"+Integer.parseInt(q,16));

                                if(ConfigSplit.detailConfigMap0420.get(p).isOccurs()) {

                                    jObjOccur.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));

                                } else {

                                    jObj0420.put(p, Integer.parseInt(q,16));

                                }

                            }

                        }

                        else if(ConfigSplit.detailConfigMap0420.get(p).getInType().equals("COMP3")) {

                            if(strEBCDIC.length() >= 32+ConfigSplit.detailConfigMap0420.get(p).getEndIdx()) {

                                String ss = strEBCDIC.substring(32+ConfigSplit.detailConfigMap0420.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap0420.get(p).getEndIdx());

                                String q = "";

                                for(char l :ss.toCharArray()) {

                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }

                                int dec = 0;

                                int num = 0;

                                if (ConfigSplit.detailConfigMap0420.get(p).getOutType().length() >= 4) {

                                    String[] s1 = ConfigSplit.detailConfigMap0420.get(p).getOutType().split("V");

                                    dec = Integer.parseInt(s1[1]);

                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                                }

                                else {

                                    num = Integer.parseInt(ConfigSplit.detailConfigMap0420.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap0420.get(p).getOutType().length()));

                                }

//                            sbOutput2.append("~" + unpackData(q, dec, num));

                                if(ConfigSplit.detailConfigMap0420.get(p).isOccurs()) {

                                    jObjOccur.put(p.substring(0 , p.length() -2), unpackData(q, dec, num));

                                } else {

                                    jObj0420.put(p, unpackData(q, dec, num));

                                }

                            }

                        }

                    }

                }

                if(!occurValue.equals("")) {

                    if(!jObjOccur.toString().equals("{}")) {
                        occurArray.add(jObjOccur);
                    }
                    jObj0420.put(occurKey, occurArray);
                }

//            RR.strASCII = sbOutput2.toString();

                RR.jObj0420 = jObj0420;

                RR.isHeader = true;

            }



            else if(recordType.equals("0530")) {

                JSONObject jObj0530 = new JSONObject();

                loadXBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "0530" , jObj0530);

                load0530Record(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "0530" , jObj0530);

//            RR.strASCII = sbOutput2.toString();

                RR.jObj0530 = jObj0530;

                RR.isHeader = true;

            }

            else if(recordType.equals("2582")) {

                JSONObject jObj2582 = new JSONObject();

                loadXBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "2582" , jObj2582);

                load2582Record(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "2582" , jObj2582);

//            RR.strASCII = sbOutput2.toString();

                RR.jObj2582 = jObj2582;

            }
/* Commenting this because we are getting this trid value from B1 number via B1_setting file
        else if(recordType.equals("3011")) {

            JSONObject jObj3011 = new JSONObject();

            loadXBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "3011" , jObj3011);

            load3011Record(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "3011" , jObj3011);

//            RR.strASCII = sbOutput2.toString();

            RR.jObj3011 = jObj3011;

        }*/

            else if(recordType.equals("3020")) {

                JSONObject jObj3020 = new JSONObject();

                loadXBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "3020" , jObj3020);

                load3020Record(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "3020" , jObj3020);

//            RR.strASCII = sbOutput2.toString();
                RR.jObj3020 = jObj3020;


            }

            else if(recordType.equals("3050")) {

                JSONObject jObj3050 = new JSONObject();

                loadXBASERecord(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "3050" , jObj3050 );

                load3050Record(convertedASCII, strEBCDIC, record, index, sbOutput2, lengthOfChar, "3050" , jObj3050);

//            RR.strASCII = sbOutput2.toString();

                RR.jObj3050 = jObj3050;

            }

            else {

                printOutput = getOutputConvertedtext(ConfigSplit.detailConfigMap,convertedASCII,strEBCDIC,
//                    record,index,lengthOfChar, recordType);
                        record,index,lengthOfChar, recordType, accountNum);

//            RR.strASCII = printOutput;
            }

///////        if (recordType.equals("2720")) {
///////        	System.out.println("AFTER - recordType -> "+ recordType);
///////        }

            //NEW
            if (RR.jObj3011 != null){
                System.out.println("Need to check the value now");
            }
            return RR;
        }catch (Exception e){

            e.printStackTrace();
        }
        return RR;
    }


    //////////////////////////////////////////////////////////////////////


    public String getOutputConvertedtext(Map<String, InputCFG> detailConfig, String convertedASCII, String ebcdic,

                                         int record, int index, long lengthOfChar, String recordType, String accountNum) {

        StringBuilder sb = new StringBuilder();


        int len = 32;

        int rec0 = 0;

        int rec1 = 0;

        int rec2 = 0 ;

        int rec3 = 0;

        int rec4 = 0;

        int rec5 = 0;

        int rec6 = 0;

        int rec7 = 0;

        int rec8 = 0;

        Integer[] segmentedRecordLength = new Integer[9];
        Arrays.fill(segmentedRecordLength,0);
        if((convertedASCII.length() > len) ) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec0 = Integer.parseInt(q,16);

            segmentedRecordLength[0]=rec0;

            //rec0 = Integer.parseInt(sbQ.toString(),16);

            len = len+Integer.parseInt(q,16);

            //len = len+Integer.parseInt(sbQ.toString(),16);

        }




        if((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("1")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec1 = Integer.parseInt(q,16);

            //rec1 = Integer.parseInt(sbQ.toString(),16);

            len = len+Integer.parseInt(q,16);

            //len = len+Integer.parseInt(sbQ.toString(),16);

        }



        if((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("2")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec2 = Integer.parseInt(q,16);

            //rec2 = Integer.parseInt(sbQ.toString(),16);

            len = len+Integer.parseInt(q,16);

            //len = len+Integer.parseInt(sbQ.toString(),16);

        }



        if((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("3")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec3 = Integer.parseInt(q,16);

            //rec3 = Integer.parseInt(sbQ.toString(),16);

            len = len+Integer.parseInt(q,16);

            //len = len+Integer.parseInt(sbQ.toString(),16);

        }



        if((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("4")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec4 = Integer.parseInt(q,16);

            //rec4 = Integer.parseInt(sbQ.toString(),16);

            len = len+Integer.parseInt(q,16);

            //len = len+Integer.parseInt(sbQ.toString(),16);

        }



        if((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("5")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec5 = Integer.parseInt(q,16);

            //rec5 = Integer.parseInt(sbQ.toString(),16);

            len = len+Integer.parseInt(q,16);

            //len = len+Integer.parseInt(sbQ.toString(),16);

        }



        if((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("6")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec6 = Integer.parseInt(q,16);

            //rec6 = Integer.parseInt(sbQ.toString(),16);

            len = len+Integer.parseInt(q,16);

            //len = len+Integer.parseInt(sbQ.toString(),16);

        }



        if ((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("7")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec7 = Integer.parseInt(q, 16);

            //rec7 = Integer.parseInt(sbQ.toString(), 16);

            len = len + Integer.parseInt(q, 16);

            //len = len + Integer.parseInt(sbQ.toString(), 16);

        }



        if ((convertedASCII.length() > len+SEG_ID_STARTIDX) && convertedASCII.substring(len+SEG_ID_STARTIDX-1,len+SEG_ID_STARTIDX).equals("8")) {

            String ss = ebcdic.substring(len+SEG_LENGTH_STARTIDX-1,len+SEG_LENGTH_ENDIDX);

            String q = "";

            //sbQ.setLength(0);

            for(int l :ss.toCharArray()) {
                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
            }

            rec8 = Integer.parseInt(q, 16);

            //rec8 = Integer.parseInt(sbQ.toString(), 16);

            len = len + Integer.parseInt(q, 16);

            //len = len + Integer.parseInt(sbQ.toString(), 16);

        }




        if(recordType.startsWith("503") || recordType.startsWith("3P3")) {
///			if (accountNum.contains("2MA00922") || accountNum.contains("1AB21577") || accountNum.contains("1AB22924") || accountNum.contains("1AB21024") || accountNum.contains("1BC56843"))
///			{

            JSONObject jObj5030 = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj5030);

            load5030Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , jObj5030);

            RR.jObj5030 = jObj5030;
            //   records50300.add(jObj5030);
        //    addRecord50300(jObj5030);
            if (shouldEmitAccountRecord(jObj5030)) {
                addRecord50300(jObj5030);
            }
        }
///        }

        else if(recordType.equals("0422")) {
            len = 32 + segmentedRecordLength[0];
            for (int i = 1; i <= 8; i++) {

                if ((convertedASCII.length() > len + SEG_ID_STARTIDX) &&
                        convertedASCII.substring(len + SEG_ID_STARTIDX - 1, len + SEG_ID_STARTIDX).equals(String.valueOf(i))) {

                    String ss = ebcdic.substring(len + SEG_LENGTH_STARTIDX - 1, len + SEG_LENGTH_ENDIDX);

                    String q = "";

                    for (int l : ss.toCharArray()) {
                        String hex = Integer.toHexString(l);
                        q += (hex.length() == 1) ? "0" + hex : hex;
                    }
                    segmentedRecordLength[i]=Integer.parseInt(q,16);
                    len = len + segmentedRecordLength[i];
                }
            }


            JSONObject jObj0422 = new JSONObject();


            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj0422);

            load0422Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType,
                    jObj0422,Arrays.asList(segmentedRecordLength));

            RR.isHeader = true;

        }

        /*else if(recordType.equals("5050")) {

        	JSONObject jObj5050 = new JSONObject();

        	loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj5050);

            load5050Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
            		rec5 , rec6 , rec7 , jObj5050);

            RR.jObj5050 = jObj5050;

        }

        else if(recordType.equals("5040")) {

        	JSONObject jObj5040 = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj5040);

            load5040Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
            		rec5 , rec6 , rec7 , jObj5040);

            RR.jObj5040 = jObj5040;

        }*/



        else if(recordType.equals("3010")) {

///			if (accountNum.contains("2MA00922") || accountNum.contains("1AB21577") || accountNum.contains("1AB22924") || accountNum.contains("1AB21024") || accountNum.contains("1BC56843"))
///			{

            JSONObject jObj3010 = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj3010);

            load3010Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , jObj3010);

            RR.jObj3010 = jObj3010;
            //  records30100.add(jObj3010);
        //    addRecord30100(jObj3010);
            if (shouldEmitAccountRecord(jObj3010)) {
                addRecord30100(jObj3010);
            }

///			}
        }

        else if(recordType.equals("2010")) {

            JSONObject jObj2010 = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj2010);

            load2010Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , jObj2010);

            RR.jObj2010 = jObj2010;

        }

        else if(recordType.equals("2410")) {

            JSONObject jObj2410 = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj2410);

            load2410Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , jObj2410);

            RR.jObj2410 = jObj2410;

        }


        else if(recordType.equals("1010")) {

            JSONObject jObj1010 = new JSONObject();

            loadXBASE1010Record(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj1010);

            load1010Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , jObj1010);

            RR.jObj1010 = jObj1010;

        }

        else if(recordType.equals("1013")) {

            JSONObject jObj1013 = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj1013);

            load1010Records(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , jObj1013);

            RR.jObj1013 = jObj1013;

        }

        else if(recordType.matches("22.*")) {

            JSONObject jObj22NN = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj22NN);

            load22NNRecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , jObj22NN);

            RR.jObj22NN = jObj22NN;

        }

        else if(recordType.equals("5085")) {

            JSONObject jObj5085 = new JSONObject();

            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar , recordType , jObj5085);

            load5085Record(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType , rec0 , rec1 , rec2 , rec3 , rec4,
                    rec5 , rec6 , rec7 , rec8 , jObj5085);

            RR.jObj5085 = jObj5085;

        }

        else {

            JSONObject jObj = new JSONObject();

            JSONObject jObjOccurSeg0 = null;

            JSONArray occurArraySeg0 = new JSONArray();

            String occurValueSeg0 = "";

            JSONObject jObjOccurSeg1 = null;

            JSONArray occurArraySeg1 = new JSONArray();

            String occurValueSeg1 = "";

            JSONObject jObjOccurSeg2 = null;

            JSONArray occurArraySeg2 = new JSONArray();

            String occurValueSeg2 = "";

            JSONObject jObjOccurSeg3 = null;

            JSONObject jObjOccurSeg3_Table = null;

            JSONObject jObjOccurSeg3_Table2 = null;

            JSONArray occurArraySeg3 = new JSONArray();

            JSONArray occurArraySeg3_Table = new JSONArray();

            JSONArray occurArraySeg3_Table2 = new JSONArray();

            String occurValueSeg3 = "";

            String occurValueSeg3_Table = "";

            String occurValueSeg3_Table2 = "";

            JSONObject jObjOccurSeg4 = null;

            JSONArray occurArraySeg4 = new JSONArray();

            String occurValueSeg4 = "";

            JSONObject jObjOccurSeg5 = null;

            JSONArray occurArraySeg5 = new JSONArray();

            String occurValueSeg5 = "";

            JSONObject jObjOccurSeg6 = null;

            JSONArray occurArraySeg6 = new JSONArray();

            String occurValueSeg6 = "";
            String occuranceKey = "";

            JSONObject jObjOccurSeg7 = null;

            JSONArray occurArraySeg7 = new JSONArray();

            String occurValueSeg7 = "";

            boolean flag =false;

            String occurKey4300 = "";
            String occurKey4400 = "";
            String occurKey4450 = "";
            String occurKey4500 = "";
            String occurKey5100 = "";
            //NEW
            String occurKey50145 = "";
            String occurKey6100 = "";
            String occurKey5050_Table = "";
            String occurKey5050_Table2 = "";
            boolean isTable =false;
            boolean isTable2 =false;


            loadXBASERecord(convertedASCII, ebcdic, record, index, sb, lengthOfChar, recordType, jObj);

            Map<String, InputCFG> detailConfigMap = null;
            boolean isRecordTypeToB1 = ConfigSplit.clientDefinedTrids.values().stream().anyMatch(value -> value.contains(recordType));
            if(ConfigSplit.clientDefinedTrids.containsKey(recordType) || ConfigSplit.clientDefinedTrids.values().stream().anyMatch(value -> value.contains(recordType))) {
                detailConfigMap = new LinkedHashMap<String, InputCFG>();
                String sectionType = ConfigSplit.clientDefinedTrids.get(recordType);
                //New
                if (sectionType == null || isRecordTypeToB1){
                    ArrayList<String> B1Number= (ArrayList<String>) ConfigSplit.clientDefinedTrids.entrySet().stream()
                            .filter(entry -> entry.getValue().contains(recordType))
                            .map(Map.Entry::getKey).collect(Collectors.toList());
                    sectionType=B1Number.get(0);
                }
                switch(sectionType) {
                    case "504140":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap5040.entrySet()) {
                            if(tempEntry.getKey().contains("5040")) {
                                InputCFG value = tempEntry.getValue();

                                String fieldNameOPEN = value.getFieldName();
                                fieldNameOPEN = fieldNameOPEN.replace("5040", "OPEN");

                                String recordTypeOPEN = value.getRecordType();
                                recordTypeOPEN = recordTypeOPEN.replace("5040", "OPEN");

                                if(null != value.getOccursArrayKey() && !value.getOccursArrayKey().isBlank()) {
                                    String occursKeyOPEN = value.getOccursArrayKey();
                                    occursKeyOPEN = occursKeyOPEN.replace("5040", "OPEN");
                                    value.setOccursArrayKey(occursKeyOPEN);
                                }

                                value.setFieldName(fieldNameOPEN);
                                value.setRecordType(recordTypeOPEN);

                                String key = tempEntry.getKey().replace("5040", "OPEN");
                                detailConfigMap.put(key , value);
                            } else {
                                detailConfigMap.put(tempEntry.getKey() , tempEntry.getValue());
                            }
                        }
                        RR.recordType = "ETSOPEN";
                        break;
                    case "504150":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap504150.entrySet()) {

                            InputCFG value = tempEntry.getValue(); // 3020
                            String key = tempEntry.getKey().replace(ConfigSplit.clientDefinedTrids.get("504150"), "CHEK");
                            detailConfigMap.put(key , value);

                        }
                        RR.recordType = "ETSCHEK";
                        break;
                    case "504151":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap504151.entrySet()) {
                            InputCFG value = tempEntry.getValue();
                            if(null != value.getOccursArrayKey() && !value.getOccursArrayKey().isBlank()) {
                                String occursKeyPETR = value.getOccursArrayKey();
                                occursKeyPETR = occursKeyPETR.replace(recordType, "ANNT");
                                value.setOccursArrayKey(occursKeyPETR);
                            }
                            String key = tempEntry.getKey().replace(recordType, "ANNT");
                            detailConfigMap.put(key , value);
                        }
                        RR.recordType = "ETSANNT";
                        break;
                    case "504141":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap504141.entrySet()) {
                                InputCFG value = tempEntry.getValue();

                                String fieldNamePETR = value.getFieldName();
                                fieldNamePETR = fieldNamePETR.replace(recordType, "PETR");

                                String recordTypePETR = value.getRecordType();
                                recordTypePETR = recordTypePETR.replace(recordType, "PETR");

                                if(null != value.getOccursArrayKey() && !value.getOccursArrayKey().isBlank()) {
                                    String occursKeyPETR = value.getOccursArrayKey();
                                    occursKeyPETR = occursKeyPETR.replace(recordType, "PETR");
                                    value.setOccursArrayKey(occursKeyPETR);
                                }

                                value.setFieldName(fieldNamePETR);
                                value.setRecordType(recordTypePETR);

                                String key = tempEntry.getKey().replace(recordType, "PETR");
                                detailConfigMap.put(key , value);

                        }
                        RR.recordType = "ETSPETR";
                        break;
                    case "504142":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap5060.entrySet()) {
                            if(tempEntry.getKey().contains("5060")) {
                                InputCFG value = tempEntry.getValue();
                                String key = tempEntry.getKey().replace("5060", "WHIS");
                                detailConfigMap.put(key , value);
                            } else {
                                detailConfigMap.put(tempEntry.getKey() , tempEntry.getValue());
                            }
                        }
                        RR.recordType = "ETSWHIS";
                        break;
                    case "504143":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap5062.entrySet()) {
                            if(tempEntry.getKey().contains("5062")) {
                                InputCFG value = tempEntry.getValue();
                                String key = tempEntry.getKey().replace("5062", "PEDI");
                                detailConfigMap.put(key , value);
                            } else {
                                detailConfigMap.put(tempEntry.getKey() , tempEntry.getValue());
                            }
                        }
                        RR.recordType = "ETSPEDI";
                        break;
                    case "504144":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap5070.entrySet()) {
                            if(tempEntry.getKey().contains("5070")) {
                                InputCFG value = tempEntry.getValue();
                                String key = tempEntry.getKey().replace("5070", "GNMA");
                                detailConfigMap.put(key , value);
                            } else {
                                detailConfigMap.put(tempEntry.getKey() , tempEntry.getValue());
                            }
                        }
                        RR.recordType = "ETSGNMA";
                        break;
                    case "504145":
                        for(Map.Entry<String , InputCFG> tempEntry : ConfigSplit.detailConfigMap5080.entrySet()) {
                            if(tempEntry.getKey().contains("5080")) {
                                InputCFG value = tempEntry.getValue();
                                String key = tempEntry.getKey().replace("5080", "IRAA");
                                detailConfigMap.put(key , value);
                            } else {
                                detailConfigMap.put(tempEntry.getKey() , tempEntry.getValue());
                            }
                        }
                        RR.recordType = "ETSIRAA";
                        break;
                }
            }
            else {
                detailConfigMap = new LinkedHashMap<String, InputCFG>(ConfigSplit.detailConfigMap);
            }

            for(String p : detailConfigMap.keySet()) {

                String[] pDetails = p.split("-");

                if (p.contains(recordType) && !flag) {

                    flag = true;

                }

                if(!p.contains(recordType) && flag) {

                    break;

                }
///////////////////////////////////////////////////////////////
///
                if(p.contains(RR.recordType.substring(3)) && (rec0 != 0)) {

                    if(p.matches(RR.recordType.substring(3)+"0-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32+detailConfigMap.get(p).getStartIdx()-1,32+detailConfigMap.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+RR.recordType.substring(3)+"0")+"~"+"ETS"+RR.recordType.substring(3)+"0");

                    }

                    if(detailConfigMap.get(p).isOccurs()) {
                        if(!occurValueSeg0.equals(p.substring(p.length() -2, p.length()))) {
                            if(jObjOccurSeg0 != null && !jObjOccurSeg0.toString().equals("{}")) {
                                occurArraySeg0.add(jObjOccurSeg0);
                            }
                            jObjOccurSeg0 = new JSONObject();
                            occurValueSeg0 = p.substring(p.length() -2, p.length());
                        }
                    }

                    if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {
                        if((32 + rec0) >= (32+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 +detailConfigMap.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring(32+detailConfigMap.get(p).getStartIdx()-1,32+detailConfigMap.get(p).getEndIdx()));
                            sb.append("~"+ss);
                            if(detailConfigMap.get(p).isOccurs()) {
                                jObjOccurSeg0.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                            } else {
                                jObj.put(p, ss.replace("\u0000", " "));
                            }
                        }
                    }
                    else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                        if((32 + rec0) >= (32+detailConfigMap.get(p).getEndIdx())  && convertedASCII.length() >= 32 +detailConfigMap.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+detailConfigMap.get(p).getStartIdx()-1,32+detailConfigMap.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }
                            if(detailConfigMap.get(p).isOccurs()) {
                                jObjOccurSeg0.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                            } else {
                                jObj.put(p, Integer.parseInt(q,16));
                            }
                        }
                    }

                    else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                        if((32 + rec0) >= (32+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 +detailConfigMap.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+detailConfigMap.get(p).getStartIdx()-1,32+detailConfigMap.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            if(detailConfigMap.get(p).isOccurs()) {
                                jObjOccurSeg0.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj.put(p, unpackedData);
                            }
                        }
                    }
                    if(detailConfigMap.get(p).isOccurs()) {
                        if ( RR.recordType.equals("ETSANNT")) {
                            if (occuranceKey.equals("")) {
                                occuranceKey = detailConfigMap.get(p).getOccursArrayKey();
                            }
                        }
                    }
                    if(p.contains(RR.recordType.substring(3)+"0") && (rec0 != 0)) {
                        if(detailConfigMap.get(p).isOccurs()) {
                            switch(recordType) {
                                case "0430":
                                    if(occurKey4300.equals("")) {
                                        occurKey4300 = detailConfigMap.get(p).getOccursArrayKey();
                                    }
                                    break;
                                case "0440":
                                    if(occurKey4400.equals("")) {
                                        occurKey4400 = detailConfigMap.get(p).getOccursArrayKey();
                                    }
                                    break;
                                case "0445":
                                    if(occurKey4450.equals("")) {
                                        occurKey4450 = detailConfigMap.get(p).getOccursArrayKey();
                                    }
                                    break;
                                case "0450":
                                    if(occurKey4500.equals("")) {
                                        occurKey4500 = detailConfigMap.get(p).getOccursArrayKey();
                                    }
                                    break;
                                case "0510":
                                    if(occurKey5100.equals("")) {
                                        occurKey5100 = detailConfigMap.get(p).getOccursArrayKey();
                                    }
                                    break;
                                //NEW
                                case "3011":
                                    if(occurKey50145.equals("")) {
                                        occurKey50145 = detailConfigMap.get(p).getOccursArrayKey();
                                    }
                                    break;
                                case "0610":
                                    if(occurKey6100.equals("")) {
                                        occurKey6100 = detailConfigMap.get(p).getOccursArrayKey();
                                    }
                                    break;
                            }
                        }
                        else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                            if((32 + rec0) >= (32+detailConfigMap.get(p).getEndIdx())  && convertedASCII.length() >= 32 +detailConfigMap.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32+detailConfigMap.get(p).getStartIdx()-1,32+detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }
//                                sb.append("~"+Integer.parseInt(q,16));
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg0.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                                } else {
                                    jObj.put(p, Integer.parseInt(q,16));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                            if((32 + rec0) >= (32+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 +detailConfigMap.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32+detailConfigMap.get(p).getStartIdx()-1,32+detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                    String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg0.put(p.substring(0 , p.length() -2), unpackedData);
                                } else {
                                    jObj.put(p, unpackedData);
                                }
                            }
                        }
                    }

                    else if(p.contains(RR.recordType.substring(3)+"1") && (rec1 != 0)) {

                        if(detailConfigMap.get(p).isOccurs()) {
                            if(!occurValueSeg1.equals(p.substring(p.length() -2, p.length()))) {
                                if(jObjOccurSeg1 != null && !jObjOccurSeg1.toString().equals("{}")) {
                                    occurArraySeg1.add(jObjOccurSeg1);
                                }
                                jObjOccurSeg1 = new JSONObject();
                                occurValueSeg1 = p.substring(p.length() -2, p.length());
                            }
                        }

                        if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {

                            if((32 + rec0 +rec1) >= (32 + rec0 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +detailConfigMap.get(p).getEndIdx()) {
                                String ss = normalize(convertedASCII.substring(32 + rec0 +detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +detailConfigMap.get(p).getEndIdx()));
//                                sb.append("~"+ss);
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg1.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                                } else {
                                    jObj.put(p, ss.replace("\u0000", " "));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                            if((32 + rec0 +rec1) >= (32 + rec0 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +detailConfigMap.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }

                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg1.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                                } else {
                                    jObj.put(p, Integer.parseInt(q,16));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                            try {

                                if((32 + rec0 +rec1) >= (32 + rec0 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +detailConfigMap.get(p).getEndIdx()) {
                                    String ss = ebcdic.substring(32 + rec0 +detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +detailConfigMap.get(p).getEndIdx());
                                    String q = "";
                                    for(char l :ss.toCharArray()) {
                                        q =q+getHexValue((Integer.toHexString((int)l)),2);

                                    }
                                    int dec = 0;
                                    int num = 0;
                                    if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                        String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                        dec = Integer.parseInt(s1[1]);
                                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                    }
                                    else {
                                        num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                    }
                                    String unpackedData = unpackData(q, dec, num);
                                    sb.append("~" + unpackedData);
                                    if(detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg1.put(p.substring(0 , p.length() -2), unpackedData);
                                    } else {
                                        jObj.put(p, unpackedData);
                                    }
                                }
                            }catch(Exception e ) {

                            }
                        }
                    }
                    else if(p.contains(RR.recordType.substring(3)+"2") && (rec2 != 0)) {


                        if(detailConfigMap.get(p).isOccurs()) {
                            if(!occurValueSeg2.equals(p.substring(p.length() -2, p.length()))) {
                                if(jObjOccurSeg2 != null && !jObjOccurSeg2.toString().equals("{}")) {
                                    occurArraySeg2.add(jObjOccurSeg2);
                                }
                                jObjOccurSeg2 = new JSONObject();
                                occurValueSeg2 = p.substring(p.length() -2, p.length());
                            }
                        }

                        if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {
                            if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +detailConfigMap.get(p).getEndIdx() ) {
                                String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +detailConfigMap.get(p).getStartIdx()-1, 32 + rec0 +rec1 +detailConfigMap.get(p).getEndIdx()));
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg2.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                                } else {
                                    jObj.put(p, ss.replace("\u0000", " "));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                            if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +detailConfigMap.get(p).getEndIdx() ) {
                                String ss = ebcdic.substring( 32 + rec0 +rec1 +detailConfigMap.get(p).getStartIdx()-1, 32 + rec0 +rec1 +detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }

//                                sb.append("~"+Integer.parseInt(q,16));
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg2.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                                } else {
                                    jObj.put(p, Integer.parseInt(q,16));
                                }
                            }
                        }
                        else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                            try {

                                if ((32 + rec0 + rec1 + rec2) >= (32 + rec0 + rec1
                                        + detailConfigMap.get(p).getEndIdx())
                                        && convertedASCII.length() >= 32 + rec0 + rec1
                                        + detailConfigMap.get(p).getEndIdx()) {
                                    String ss = ebcdic.substring(
                                            32 + rec0 + rec1 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + detailConfigMap.get(p).getEndIdx());
                                    String q = "";
                                    for (char l : ss.toCharArray()) {
                                        q = q + getHexValue((Integer.toHexString((int) l)), 2);
                                    }
                                    int dec = 0;
                                    int num = 0;
                                    if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                        String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                        dec = Integer.parseInt(s1[1]);
                                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                    } else {
                                        num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1,detailConfigMap.get(p).getOutType().length()));
                                    }
                                    String unpackedData = unpackData(q, dec, num);
//                                    sb.append("~" + unpackedData);
                                    if(detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg2.put(p.substring(0 , p.length() -2), unpackedData);
                                    } else {
                                        jObj.put(p, unpackedData);
                                    }
                                }
                            }catch(Exception e ) {

                            }
                        }
                    }

                    else if (p.contains(RR.recordType.substring(3) + "3") && (rec3 != 0)) {

                        if (RR.recordType.startsWith("ETSPETR")) {

                            if(!detailConfigMap.get(p).isOccurs()) {

                                if (detailConfigMap.get(p).getInType().equals("X")
                                        || detailConfigMap.get(p).getInType().equals("N")) {

                                    if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                            && convertedASCII.length() >= 32 + rec0 + rec1 + rec2+ detailConfigMap.get(p).getEndIdx()) {
                                        String ss = normalize(convertedASCII.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()));
                                        jObj.put(p, ss.replace("\u0000", " "));
                                    }

                                }

                                else if (detailConfigMap.get(p).getInType().equals("COMP")) {

                                    if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                            && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                        String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                        String q = "";
                                        for (int l : ss.toCharArray()) {
                                            q += Integer.toHexString(l).length() == 1 ? "0" + Integer.toHexString(l) : Integer.toHexString(l);
                                        }
                                        jObj.put(p, Integer.parseInt(q, 16));
                                    }
                                }

                                else if (detailConfigMap.get(p).getInType().equals("COMP3")) {
                                    try {

                                        if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                                && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                            String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                            String q = "";
                                            for (char l : ss.toCharArray()) {
                                                q = q + getHexValue((Integer.toHexString((int) l)), 2);
                                            }
                                            int dec = 0;
                                            int num = 0;
                                            if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                                String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                                dec = Integer.parseInt(s1[1]);
                                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                            } else {
                                                num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1,
                                                        detailConfigMap.get(p).getOutType().length()));
                                            }
                                            String unpackedData = unpackData(q, dec, num);
                                            jObj.put(p, unpackedData);
                                        }
                                    } catch (Exception e) {

                                    }
                                }
                            } else { //detailConfigMap.get(p).isOccurs() = true case

                                if (null != detailConfigMap.get(p).getOccursArrayKey()
                                        && !detailConfigMap.get(p).getOccursArrayKey().isBlank()
                                        && detailConfigMap.get(p).getOccursArrayKey().endsWith("TABLE2")){
                                    occurKey5050_Table2 = detailConfigMap.get(p).getOccursArrayKey();
                                    isTable = false;
                                    isTable2 = true;
                                }

                                else if (null != detailConfigMap.get(p).getOccursArrayKey()
                                        && !detailConfigMap.get(p).getOccursArrayKey().isBlank()
                                        && detailConfigMap.get(p).getOccursArrayKey().endsWith("TABLE")){
                                    occurKey5050_Table = detailConfigMap.get(p).getOccursArrayKey();
                                    isTable = true;
                                    isTable2 = false;
                                } else {
                                    //System.out.println("           rec3 I should not be here!");
                                    isTable = false;
                                    isTable2 = false;
                                }


                                //System.out.println("           rec3 occurKey5050_Table [" + occurKey5050_Table + "]");
                                //System.out.println("           rec3 occurKey5050_Table2 [" + occurKey5050_Table2 + "]");

                                if(isTable) {

                                    //System.out.println("           rec3 case isTable [ " + isTable + "], occurKey5050_Table [" + occurKey5050_Table + "]");
                                    if (!occurValueSeg3_Table.equals(p.substring(p.length() - 2, p.length()))) {
                                        if (jObjOccurSeg3_Table != null && !jObjOccurSeg3_Table.toString().equals("{}")) {
                                            occurArraySeg3_Table.add(jObjOccurSeg3_Table);
                                        }
                                        jObjOccurSeg3_Table = new JSONObject();
                                        occurValueSeg3_Table = p.substring(p.length() - 2, p.length());
                                    }

                                    if (detailConfigMap.get(p).getInType().equals("X")
                                            || detailConfigMap.get(p).getInType().equals("N")) {

                                        if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                                && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                            String ss = normalize(convertedASCII.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()));
                                            jObjOccurSeg3_Table.put(p.substring(0, p.length() - 2), ss.replace("\u0000", " "));


                                        } else {
                                            // System.out.println(" rec3 else X or N , rec0 [" + rec0 + "], rec1 [" + rec1 +
                                            // "], rec2 [" + rec2 + "], rec3 [" + rec3 + "], p-Key [" + p + "], p-Value [" +
                                            // detailConfigMap.get(p) + "]");
                                        }

                                    }

                                    else if (detailConfigMap.get(p).getInType().equals("COMP")) {

                                        if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                            String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                            String q = "";
                                            for (int l : ss.toCharArray()) {
                                                q += Integer.toHexString(l).length() == 1 ? "0" + Integer.toHexString(l) : Integer.toHexString(l);
                                            }

                                            jObjOccurSeg3_Table.put(p.substring(0, p.length() - 2), Integer.parseInt(q, 16));
                                        }
                                    }

                                    else if (detailConfigMap.get(p).getInType().equals("COMP3")) {
                                        try {

                                            if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                                    && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                                String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                                String q = "";
                                                for (char l : ss.toCharArray()) {
                                                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                                                }
                                                int dec = 0;
                                                int num = 0;
                                                if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                                    String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                                    dec = Integer.parseInt(s1[1]);
                                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                                } else {
                                                    num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                                }
                                                String unpackedData = unpackData(q, dec, num);
                                                jObjOccurSeg3_Table.put(p.substring(0, p.length() - 2), unpackedData);
                                            }
                                        } catch (Exception e) {

                                        }
                                    }
                                } else if(isTable2) {

                                    //System.out.println("           rec3 case isTable2 [ " + isTable2 + "], occurKey5050_Table2 [" + occurKey5050_Table2 + "]");
                                    if (!occurValueSeg3_Table2.equals(p.substring(p.length() - 2, p.length()))) {
                                        if (jObjOccurSeg3_Table2 != null && !jObjOccurSeg3_Table2.toString().equals("{}")) {
                                            occurArraySeg3_Table2.add(jObjOccurSeg3_Table2);
                                        }
                                        jObjOccurSeg3_Table2 = new JSONObject();
                                        occurValueSeg3_Table2 = p.substring(p.length() - 2, p.length());
                                    }

                                    if (detailConfigMap.get(p).getInType().equals("X")
                                            || detailConfigMap.get(p).getInType().equals("N")) {

                                        if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                                && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                            String ss = normalize(convertedASCII.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()));
                                            jObjOccurSeg3_Table2.put(p.substring(0, p.length() - 2), ss.replace("\u0000", " "));


                                        } else {
                                            // System.out.println(" rec3 else X or N , rec0 [" + rec0 + "], rec1 [" + rec1 +
                                            // "], rec2 [" + rec2 + "], rec3 [" + rec3 + "], p-Key [" + p + "], p-Value [" +
                                            // detailConfigMap.get(p) + "]");
                                        }

                                    }

                                    else if (detailConfigMap.get(p).getInType().equals("COMP")) {

                                        if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                            String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                            String q = "";
                                            for (int l : ss.toCharArray()) {
                                                q += Integer.toHexString(l).length() == 1 ? "0" + Integer.toHexString(l) : Integer.toHexString(l);
                                            }

                                            jObjOccurSeg3_Table2.put(p.substring(0, p.length() - 2), Integer.parseInt(q, 16));
                                        }
                                    }

                                    else if (detailConfigMap.get(p).getInType().equals("COMP3")) {
                                        try {

                                            if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                                    && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                                String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                                String q = "";
                                                for (char l : ss.toCharArray()) {
                                                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                                                }
                                                int dec = 0;
                                                int num = 0;
                                                if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                                    String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                                    dec = Integer.parseInt(s1[1]);
                                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                                } else {
                                                    num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                                }
                                                String unpackedData = unpackData(q, dec, num);
                                                jObjOccurSeg3_Table2.put(p.substring(0, p.length() - 2), unpackedData);
                                            }
                                        } catch (Exception e) {

                                        }
                                    }
                                } else {
                                    //System.out.println("           rec3 I should not be here!!!");
                                }

                            }
                        } else { //RR.recordType.startsWith("ETSPETR") = false

                            if (detailConfigMap.get(p).isOccurs()) {

                                if (!occurValueSeg3.equals(p.substring(p.length() - 2, p.length()))) {
                                    if (jObjOccurSeg3 != null && !jObjOccurSeg3.toString().equals("{}")) {
                                        occurArraySeg3.add(jObjOccurSeg3);
                                    }
                                    jObjOccurSeg3 = new JSONObject();
                                    occurValueSeg3 = p.substring(p.length() - 2, p.length());
                                }
                            }

                            if (detailConfigMap.get(p).getInType().equals("X")
                                    || detailConfigMap.get(p).getInType().equals("N")) {

                                if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                        && convertedASCII.length() >= 32 + rec0 + rec1 + rec2+ detailConfigMap.get(p).getEndIdx()) {
                                    String ss = normalize(convertedASCII.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()));
                                    if (detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg3.put(p.substring(0, p.length() - 2), ss.replace("\u0000", " "));
                                    } else {
                                        jObj.put(p, ss.replace("\u0000", " "));
                                    }

                                }

                            }

                            else if (detailConfigMap.get(p).getInType().equals("COMP")) {

                                if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                        && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                    String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                    String q = "";
                                    for (int l : ss.toCharArray()) {
                                        q += Integer.toHexString(l).length() == 1 ? "0" + Integer.toHexString(l) : Integer.toHexString(l);
                                    }

                                    if (detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg3.put(p.substring(0, p.length() - 2), Integer.parseInt(q, 16));
                                    } else {
                                        jObj.put(p, Integer.parseInt(q, 16));
                                    }
                                }
                            }

                            else if (detailConfigMap.get(p).getInType().equals("COMP3")) {
                                try {

                                    if ((32 + rec0 + rec1 + rec2 + rec3) >= (32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx())
                                            && convertedASCII.length() >= 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx()) {
                                        String ss = ebcdic.substring(32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getStartIdx() - 1, 32 + rec0 + rec1 + rec2 + detailConfigMap.get(p).getEndIdx());
                                        String q = "";
                                        for (char l : ss.toCharArray()) {
                                            q = q + getHexValue((Integer.toHexString((int) l)), 2);
                                        }
                                        int dec = 0;
                                        int num = 0;
                                        if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                            String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                            dec = Integer.parseInt(s1[1]);
                                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                        } else {
                                            num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1,
                                                    detailConfigMap.get(p).getOutType().length()));
                                        }
                                        String unpackedData = unpackData(q, dec, num);
                                        if (detailConfigMap.get(p).isOccurs()) {
                                            jObjOccurSeg3.put(p.substring(0, p.length() - 2), unpackedData);
                                        } else {
                                            jObj.put(p, unpackedData);
                                        }
                                    }
                                } catch (Exception e) {

                                }
                            }
                        }

                    }
                    else if(p.contains(recordType+"4") && (rec4 != 0)) {

                        if(detailConfigMap.get(p).isOccurs()) {
                            if(!occurValueSeg4.equals(p.substring(p.length() -2, p.length()))) {
                                if(jObjOccurSeg4 != null && !jObjOccurSeg4.toString().equals("{}")) {
                                    occurArraySeg4.add(jObjOccurSeg4);
                                }
                                jObjOccurSeg4 = new JSONObject();
                                occurValueSeg4 = p.substring(p.length() -2, p.length());
                            }
                        }

                        if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx()) {
                                String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx()));
//                                sb.append("~"+ss);
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg4.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                                } else {
                                    jObj.put(p, ss.replace("\u0000", " "));
                                }
                            }
                        }
                        else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }

//                                sb.append("~"+Integer.parseInt(q,16));
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg4.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                                } else {
                                    jObj.put(p, Integer.parseInt(q,16));
                                }
                            }
                        }
                        else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                            try {

                                if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx()) {
                                    String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +detailConfigMap.get(p).getEndIdx());
                                    String q = "";
                                    for(char l :ss.toCharArray()) {
                                        q =q+getHexValue((Integer.toHexString((int)l)),2);

                                    }
                                    int dec = 0;
                                    int num = 0;
                                    if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                        String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                        dec = Integer.parseInt(s1[1]);
                                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                    }
                                    else {
                                        num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                    }
                                    String unpackedData = unpackData(q, dec, num);
//                                    sb.append("~" + unpackedData);
                                    if(detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg4.put(p.substring(0 , p.length() -2), unpackedData);
                                    } else {
                                        jObj.put(p, unpackedData);
                                    }
                                }
                            }catch(Exception e ) {

                            }
                        }
                    }

                    else if(p.contains(recordType+"5") && (rec5 != 0)) {

/*                    	if(p.matches(recordType+"5-.*SEGLEN")) {
                            sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                    (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx())))
                                    +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                        }
*/
                        if(detailConfigMap.get(p).isOccurs()) {
                            if(!occurValueSeg5.equals(p.substring(p.length() -2, p.length()))) {
                                if(jObjOccurSeg5 != null && !jObjOccurSeg5.toString().equals("{}")) {
                                    occurArraySeg5.add(jObjOccurSeg5);
                                }
                                jObjOccurSeg5 = new JSONObject();
                                occurValueSeg5 = p.substring(p.length() -2, p.length());
                            }
                        }

                        if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx()) {
                                String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx()));
//                                sb.append("~"+ss);

                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg5.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                                } else {
                                    jObj.put(p, jObj.put(p, ss.replace("\u0000", " ")));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }

//                                sb.append("~"+Integer.parseInt(q,16));

                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg5.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                                } else {
                                    jObj.put(p, Integer.parseInt(q,16));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                            try {

                                if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx()) {
                                    String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+detailConfigMap.get(p).getEndIdx());
                                    String q = "";
                                    for(char l :ss.toCharArray()) {
                                        q =q+getHexValue((Integer.toHexString((int)l)),2);

                                    }
                                    int dec = 0;
                                    int num = 0;
                                    if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                        String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                        dec = Integer.parseInt(s1[1]);
                                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                    }
                                    else {
                                        num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                    }
                                    String unpackedData = unpackData(q, dec, num);
//                                    sb.append("~" + unpackedData);
                                    if(detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg5.put(p.substring(0 , p.length() -2), unpackedData);
                                    } else {
                                        jObj.put(p, unpackedData);
                                    }
                                }
                            }catch(Exception e ) {

                            }
                        }
                    }
                    else if(p.contains(recordType+"6") && (rec6 != 0)) {
/*                        if(p.matches(recordType+"6-.*SEGLEN")) {
                            sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                    (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx())))
                                    +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                        }
 */
                        if(detailConfigMap.get(p).isOccurs()) {
                            if(!occurValueSeg6.equals(p.substring(p.length() -2, p.length()))) {
                                if(jObjOccurSeg6 != null && !jObjOccurSeg6.toString().equals("{}")) {
                                    occurArraySeg6.add(jObjOccurSeg6);
                                }
                                jObjOccurSeg6 = new JSONObject();
                                occurValueSeg6 = p.substring(p.length() -2, p.length());
                            }
                        }

                        if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx()) {
                                String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx()));
//                                sb.append("~"+ss);
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg6.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                                } else {
                                    jObj.put(p, ss.replace("\u0000", " "));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }

//                                sb.append("~"+Integer.parseInt(q,16));
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg6.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                                } else {
                                    jObj.put(p, Integer.parseInt(q,16));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                            try {

                                if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx()) {
                                    String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+detailConfigMap.get(p).getEndIdx());
                                    String q = "";
                                    for (char l : ss.toCharArray()) {
                                        q = q + getHexValue((Integer.toHexString((int) l)), 2);

                                    }
                                    int dec = 0;
                                    int num = 0;
                                    if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                        String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                        dec = Integer.parseInt(s1[1]);
                                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                    }
                                    else {
                                        num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                    }
                                    String unpackedData = unpackData(q, dec, num);
//                                    sb.append("~" + unpackedData);
                                    if(detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg6.put(p.substring(0 , p.length() -2), unpackedData);
                                    } else {
                                        jObj.put(p, unpackedData);
                                    }
                                }
                            }catch(Exception e ) {

                            }
                        }
                    }
                    else if(p.contains(recordType+"7") && (rec7 != 0)) {
/*                        if(p.matches(recordType+"7-.*SEGLEN")) {
                            sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                    (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getEndIdx())))
                                    +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                        }
*/
                        if(detailConfigMap.get(p).isOccurs()) {
                            if(!occurValueSeg7.equals(p.substring(p.length() -2, p.length()))) {
                                if(jObjOccurSeg7 != null && !jObjOccurSeg7.toString().equals("{}")) {
                                    occurArraySeg7.add(jObjOccurSeg7);
                                }
                                jObjOccurSeg7 = new JSONObject();
                                occurValueSeg7 = p.substring(p.length() -2, p.length());
                            }
                        }

                        if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+detailConfigMap.get(p).getEndIdx()) {
                                String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getEndIdx()));
//                                sb.append("~"+ss);
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg7.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                                } else {
                                    jObj.put(p, ss.replace("\u0000", " "));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+detailConfigMap.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getEndIdx());
                                String q = "";
                                for(int l :ss.toCharArray()) {
                                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                                }

                                sb.append("~"+Integer.parseInt(q,16));
                                if(detailConfigMap.get(p).isOccurs()) {
                                    jObjOccurSeg7.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                                } else {
                                    jObj.put(p, Integer.parseInt(q,16));
                                }
                            }
                        }

                        else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                            try {

                                if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+detailConfigMap.get(p).getEndIdx()) {
                                    String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+detailConfigMap.get(p).getEndIdx());
                                    String q = "";
                                    for (char l : ss.toCharArray()) {
                                        q = q + getHexValue((Integer.toHexString((int) l)), 2);

                                    }
                                    int dec = 0;
                                    int num = 0;
                                    if (detailConfigMap.get(p).getOutType().length() >= 4) {
                                        String[] s1 = detailConfigMap.get(p).getOutType().split("V");
                                        dec = Integer.parseInt(s1[1]);
                                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                    }
                                    else {
                                        num = Integer.parseInt(detailConfigMap.get(p).getOutType().substring(1, detailConfigMap.get(p).getOutType().length()));
                                    }
                                    String unpackedData = unpackData(q, dec, num);
//                                    sb.append("~" + unpackedData);
                                    if(detailConfigMap.get(p).isOccurs()) {
                                        jObjOccurSeg7.put(p.substring(0 , p.length() -2), unpackedData);
                                    } else {
                                        jObj.put(p, unpackedData);
                                    }
                                }
                            }catch(Exception e ) {

                            }
                        }
                    }
                }
                else{
                    continue;
                }
            }
            if(!flag) {
                sb = new StringBuilder();
            }

/////////////////////////////////////////////////////////////////

            if(RR.recordType.equals("ETSTTSS") || RR.recordType.equals("ETS9010") || RR.recordType.equals("ETS9050") ||
                    RR.recordType.equals("ETS9080") || RR.recordType.equals("ETS9090")) {
                RR.isTrailer = true;
                switch(RR.recordType) {
                    case "ETS9080":
                        FileConverter_MT.B228TrailerRecords = (long) jObj.get("90800-TLR-TOTAL");
                        break;
                    case "ETSTTSS":
                        if(!occurValueSeg3.equals("")) {
                            //jObj.put("04300-TCL-IMPR-MSG-TABLE", occurArraySeg3);
                            if(!jObjOccurSeg3.toString().equals("{}")) {
                                occurArraySeg3.add(jObjOccurSeg3);
                            }
                            jObj.put(occurKey4300, occurArraySeg3);
                        }
                        break;
                }
            }
            else if (RR.recordType.startsWith("ETS04") || RR.recordType.startsWith("ETS05") || RR.recordType.startsWith("ETS06")) {
                RR.isHeader = true;
                switch(RR.recordType) {
                    case "ETS0430":
                        if(!occurValueSeg0.equals("")) {
                            if(!jObjOccurSeg0.toString().equals("{}")) {
                                occurArraySeg0.add(jObjOccurSeg0);
                            }
                            jObj.put(occurKey4300, occurArraySeg0);
                        }
                        break;
                    case "ETS0440":
                        if(!occurValueSeg0.equals("")) {
                            if(!jObjOccurSeg0.toString().equals("{}")) {
                                occurArraySeg0.add(jObjOccurSeg0);
                            }
                            jObj.put(occurKey4400, occurArraySeg0);
                        }
                        break;
                    case "ETS0445":
                        if(!occurValueSeg0.equals("")) {

                            if(!jObjOccurSeg0.toString().equals("{}")) {
                                occurArraySeg0.add(jObjOccurSeg0);
                            }
                            jObj.put(occurKey4450, occurArraySeg0);
                        }
                        break;
                    case "ETS0450":
                        if(!occurValueSeg0.equals("")) {

                            if(!jObjOccurSeg0.toString().equals("{}")) {
                                occurArraySeg0.add(jObjOccurSeg0);
                            }
                            jObj.put(occurKey4500, occurArraySeg0);
                        }
                        break;
                    case "ETS0510":
                        if(!occurValueSeg0.equals("")) {

                            //if(!jObjOccurSeg0.toString().equals("{}") && !occurArraySeg0.contains(jObjOccurSeg0)) {
                            if(!jObjOccurSeg0.toString().equals("{}")) {
                                occurArraySeg0.add(jObjOccurSeg0);
                            }
                            jObj.put(occurKey5100, occurArraySeg0);
                        }
                        break;
                    //NEW
                    case "ETSCHEK":
                        if(!occurValueSeg0.equals("")) {

                            occurArraySeg0.add(jObjOccurSeg0);
                            jObj.put(occurKey50145, occurArraySeg0);
                        }
                        break;
                    case "ETS0610":
                        if(!occurValueSeg0.equals("")) {

                            if(!jObjOccurSeg0.toString().equals("{}")) {
                                occurArraySeg0.add(jObjOccurSeg0);
                            }
                            jObj.put(occurKey6100, occurArraySeg0);
                        }
                        break;
                }
            } else if (RR.recordType.startsWith("ETSPETR") || RR.recordType.startsWith("ETSOPEN")) {

                if(!occurValueSeg3_Table.equals("")) {
                    if(!jObjOccurSeg3_Table.toString().equals("{}")) {
                        occurArraySeg3_Table.add(jObjOccurSeg3_Table);
                    }
                    jObj.put(occurKey5050_Table, occurArraySeg3_Table);
                }

                if(!occurValueSeg3_Table2.equals("")) {
                    if(!jObjOccurSeg3_Table2.toString().equals("{}")) {
                        occurArraySeg3_Table2.add(jObjOccurSeg3_Table2);
                    }
                    jObj.put(occurKey5050_Table2, occurArraySeg3_Table2);
                }
            } else if (RR.recordType.contains("ETSANNT") ) {
                    if(!occurValueSeg3.equals("")) {
                        //jObj.put("04300-TCL-IMPR-MSG-TABLE", occurArraySeg3);
                        if(!jObjOccurSeg3.toString().equals("{}")) {
                            occurArraySeg3.add(jObjOccurSeg3);
                        }
                        jObj.put(occuranceKey, occurArraySeg3);
                    }
            }

            RR.jObj = jObj;
        }
        return sb.toString();

    }





    //////////////////////////////////////////////////////////////////////
/*
    private void loadDetailRecords(String convertedASCII, String ebcdic, int record2, int index2, StringBuilder sb,
			long lengthOfChar2, String recordType, int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6,
			int rec7, JSONObject jObj) {
		// TODO Auto-generated method stub

	}
*/


    private void load5085Record(String convertedASCII, String ebcdic, int record, int index, StringBuilder sb, long lengthOfChar, String recordType,

                                int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, int rec8, JSONObject jObj5085) {
        // TODO Auto-generated method stub
        JSONObject jObjOccur50853 = null;
        JSONArray occurArray50853 = new JSONArray();
        String occurValue50853 = "";
        String occurKey50853 = "";
        JSONObject jObjOccur50854 = null;
        JSONArray occurArray50854 = new JSONArray();
        String occurValue50854 = "";
        String occurKey50854 = "";
        JSONObject jObjOccur50855 = null;
        JSONArray occurArray50855 = new JSONArray();
        String occurValue50855 = "";
        String occurKey50855 = "";
        JSONObject jObjOccur50856 = null;
        JSONArray occurArray50856 = new JSONArray();
        String occurValue50856 = "";
        String occurKey50856 = "";
        JSONObject jObjOccur50857 = null;
        JSONArray occurArray50857 = new JSONArray();
        String occurValue50857 = "";
        String occurKey50857 = "";
        JSONObject jObjOccur50858 = null;
        JSONArray occurArray50858 = new JSONArray();
        String occurValue50858 = "";
        String occurKey50858 = "";
        for(String p : ConfigSplit.detailConfigMap5085.keySet()) {
            if(p.contains(recordType+"0") && (rec0 != 0)) {

/*                if(p.matches(recordType+"0-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
                        sb.append("~"+ss);
                        jObj5085.put(p, ss.replace("\u0000", " "));
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5085.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5085.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(char l :ss.toCharArray()) {
                            q =q+getHexValue((Integer.toHexString((int)l)),2);

                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        else {
                            num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
//                        sb.append("~" + unpackedData);
                        jObj5085.put(p, unpackedData);
                    }
                }
            }

            else if(p.contains(recordType+"1") && (rec1 != 0)) {
/*                if(p.matches(recordType+"1-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj5085.put(p, ss.replace("\u0000", " "));
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5085.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj5085.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"2") && (rec2 != 0)) {

/*                if(p.matches(recordType+"2-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj5085.put(p, ss.replace("\u0000", " "));
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx() ) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5085.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj5085.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"3") && (rec3 != 0)) {
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs() && occurKey50853.equals("")) {
                    occurKey50853 = ConfigSplit.detailConfigMap5085.get(p).getOccursArrayKey();
                }
/*            	if(p.matches(recordType+"3-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                    if(!occurValue50853.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur50853 != null && !jObjOccur50853.toString().equals("{}")) {
                            occurArray50853.add(jObjOccur50853);
                        }
                        jObjOccur50853 = new JSONObject();
                        occurValue50853 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50853.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj5085.put(p, ss.replace("\u0000", " "));
                        }

                    }

                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50853.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj5085.put(p, Integer.parseInt(q,16));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);


                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                                jObjOccur50853.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj5085.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"4") && (rec4 != 0)) {

                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs() && occurKey50854.equals("")) {
                    occurKey50854 = ConfigSplit.detailConfigMap5085.get(p).getOccursArrayKey();
                }

/*            	if(p.matches(recordType+"4-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                    if(!occurValue50854.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur50854 != null && !jObjOccur50854.toString().equals("{}")) {
                            occurArray50854.add(jObjOccur50854);
                        }
                        jObjOccur50854 = new JSONObject();
                        occurValue50854 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50854.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj5085.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50854.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj5085.put(p, Integer.parseInt(q,16));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                                jObjOccur50854.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj5085.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"5") && (rec5 != 0)) {
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs() && occurKey50855.equals("")) {
                    occurKey50855 = ConfigSplit.detailConfigMap5085.get(p).getOccursArrayKey();
                }

/*                if(p.matches(recordType+"5-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                    if(!occurValue50855.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur50855 != null && !jObjOccur50855.toString().equals("{}")) {
                            occurArray50855.add(jObjOccur50855);
                        }
                        jObjOccur50855 = new JSONObject();
                        occurValue50855 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50855.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj5085.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50855.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj5085.put(p, Integer.parseInt(q,16));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                                jObjOccur50855.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj5085.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"6") && (rec6 != 0)) {
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs() && occurKey50856.equals("")) {
                    occurKey50856 = ConfigSplit.detailConfigMap5085.get(p).getOccursArrayKey();
                }
/*                if(p.matches(recordType+"6-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                    if(!occurValue50856.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur50856 != null && !jObjOccur50856.toString().equals("{}")) {
                            occurArray50856.add(jObjOccur50856);
                        }
                        jObjOccur50856 = new JSONObject();
                        occurValue50856 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50856.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj5085.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50856.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj5085.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                                jObjOccur50856.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                            } else {
                                jObj5085.put(p, Integer.parseInt(q,16));
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"7") && (rec7 != 0)) {
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs() && occurKey50857.equals("")) {
                    occurKey50857 = ConfigSplit.detailConfigMap5085.get(p).getOccursArrayKey();
                }

/*                if(p.matches(recordType+"7-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                    if(!occurValue50857.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur50857 != null && !jObjOccur50857.toString().equals("{}")) {
                            occurArray50857.add(jObjOccur50857);
                        }
                        jObjOccur50857 = new JSONObject();
                        occurValue50857 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50857.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj5085.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50857.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj5085.put(p, Integer.parseInt(q,16));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                                jObjOccur50857.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj5085.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"8") && (rec8 != 0)) {
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs() && occurKey50858.equals("")) {
                    occurKey50858 = ConfigSplit.detailConfigMap5085.get(p).getOccursArrayKey();
                }

/*                if(p.matches(recordType+"8-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"8")+"~"+"ETS"+recordType+"8");

                }
*/
                if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                    if(!occurValue50858.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur50858 != null && !jObjOccur50858.toString().equals("{}")) {
                            occurArray50858.add(jObjOccur50853);
                        }
                        jObjOccur50858 = new JSONObject();
                        occurValue50858 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5085.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+rec8) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50858.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj5085.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+rec8) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                            jObjOccur50858.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj5085.put(p, Integer.parseInt(q,16));
                        }


                    }
                }
                else if(ConfigSplit.detailConfigMap5085.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+rec8) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7+ConfigSplit.detailConfigMap5085.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5085.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5085.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5085.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5085.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5085.get(p).isOccurs()) {
                                jObjOccur50858.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj5085.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }
        }
        if(!occurValue50853.equals("")) {

            if(!jObjOccur50853.toString().equals("{}")) {
                occurArray50853.add(jObjOccur50853);
            }
            jObj5085.put(occurKey50853, occurArray50853);
        }
        if(!occurValue50854.equals("")) {


            if(!jObjOccur50854.toString().equals("{}")) {
                occurArray50854.add(jObjOccur50854);
            }
            jObj5085.put(occurKey50854, occurArray50854);
        }
        if(!occurValue50855.equals("")) {

            if(!jObjOccur50855.toString().equals("{}")) {
                occurArray50855.add(jObjOccur50855);
            }
            jObj5085.put(occurKey50855, occurArray50855);
        }
        if(!occurValue50856.equals("")) {

            if(!jObjOccur50856.toString().equals("{}")) {
                occurArray50856.add(jObjOccur50856);
            }
            jObj5085.put(occurKey50856, occurArray50856);
        }
        if(!occurValue50857.equals("")) {

            if(!jObjOccur50857.toString().equals("{}")) {
                occurArray50857.add(jObjOccur50857);
            }
            jObj5085.put(occurKey50857, occurArray50857);
        }
        if(!occurValue50858.equals("")) {

            if(!jObjOccur50858.toString().equals("{}")) {
                occurArray50858.add(jObjOccur50858);
            }
            jObj5085.put(occurKey50858, occurArray50858);
        }
        RR.jObj5085 = jObj5085;
        return;
    }





    //////////////////////////////////////////////////////////////////////

    private boolean isSegmentLengthPresent(List<Integer> segmentedRecordLength,int segment){
        return segmentedRecordLength.get(segment)!=0;
    }
    private class OccuranceKeyJsonObjectMap{
        String currentOccuranceKeyName;
        JSONArray currentOccuranceJsonArrayValues;
        String currentOccuranceValue;
        JSONObject tempJsonObject;
        Map<String,JSONArray> occuranceKeyJsonArrayMap;

        public OccuranceKeyJsonObjectMap(String currentOccuranceKeyName, JSONArray orruranceValues, String currentOccuranceValue,
                                         JSONObject tempJsonObject) {
            this.currentOccuranceKeyName = currentOccuranceKeyName;
            this.currentOccuranceJsonArrayValues = orruranceValues;
            this.currentOccuranceValue = currentOccuranceValue;
            this.tempJsonObject = tempJsonObject;
            occuranceKeyJsonArrayMap = new HashMap<>();
        }
        public void updateCurrentOccuranceJsonArrayValues(){
            if(currentOccuranceJsonArrayValues!=null&&!currentOccuranceJsonArrayValues.isEmpty()) {
                occuranceKeyJsonArrayMap.put(currentOccuranceKeyName, currentOccuranceJsonArrayValues);
                currentOccuranceJsonArrayValues=new JSONArray();
            }
        }
    }
    private void load0422Records(String convertedASCII, String ebcdic, int record, int index,

                                 StringBuilder sb, long lengthOfChar, String recordType, JSONObject jObj0422,List<Integer> segmentedRecordLength) {

        List<Integer> cumulativeUpSegmentedLength = new ArrayList<>(segmentedRecordLength.size());
        int cumulatedLength =0;
        for(int i=0;i<segmentedRecordLength.size();i++){
            cumulatedLength = cumulatedLength+segmentedRecordLength.get(i);
            cumulativeUpSegmentedLength.add(cumulatedLength);
        }
        Map<String,InputCFG> detailConfigMap = ConfigSplit.detailConfigMap0422;
        OccuranceKeyJsonObjectMap[] occuranceKeyJsonObjectMapList = new OccuranceKeyJsonObjectMap[segmentedRecordLength.size()];
        Arrays.fill(occuranceKeyJsonObjectMapList,null);
        for(String p : ConfigSplit.detailConfigMap0422.keySet()) {
            InputCFG config = ConfigSplit.detailConfigMap0422.get(p);
            int segment = Integer.parseInt(config.getRecordType().substring(config.getRecordType().length()-1));
            if(isSegmentLengthPresent(segmentedRecordLength,segment)){
                if(config.isOccurs()) {
                   String occuranceKey = config.getOccursArrayKey();
                   if(occuranceKeyJsonObjectMapList[segment]==null) {
                       occuranceKeyJsonObjectMapList[segment] = new OccuranceKeyJsonObjectMap(occuranceKey,
                               new JSONArray(), "", null);
                   }else if(!occuranceKey.equals(occuranceKeyJsonObjectMapList[segment].currentOccuranceKeyName)){
                       occuranceKeyJsonObjectMapList[segment].updateCurrentOccuranceJsonArrayValues();
                       occuranceKeyJsonObjectMapList[segment].tempJsonObject = new JSONObject();
                       occuranceKeyJsonObjectMapList[segment].currentOccuranceValue = p.substring(p.length() -2,
                               p.length());
                       occuranceKeyJsonObjectMapList[segment].currentOccuranceKeyName=occuranceKey;
                   }
                }
                if(config.isOccurs()&&!occuranceKeyJsonObjectMapList[segment].currentOccuranceValue.
                        equals(p.substring(p.length() -2, p.length()))) {
                    if(occuranceKeyJsonObjectMapList[segment].tempJsonObject != null &&
                            !occuranceKeyJsonObjectMapList[segment].tempJsonObject.toString().equals("{}")) {
                        occuranceKeyJsonObjectMapList[segment].currentOccuranceJsonArrayValues.
                                add(occuranceKeyJsonObjectMapList[segment].tempJsonObject);
                    }
                    occuranceKeyJsonObjectMapList[segment].tempJsonObject = new JSONObject();
                    occuranceKeyJsonObjectMapList[segment].currentOccuranceValue = p.substring(p.length() -2, p.length());
                }
                int prevCumulativeLength = (segment>0) ? cumulativeUpSegmentedLength.get(segment-1) : 0;
                if ((32 + cumulativeUpSegmentedLength.get(segment)) >= (32 +prevCumulativeLength
                        + detailConfigMap.get(p).getEndIdx())
                        && convertedASCII.length() >= 32 + prevCumulativeLength
                        + detailConfigMap.get(p).getEndIdx()){
                    if(detailConfigMap.get(p).getInType().equals("X") || detailConfigMap.get(p).getInType().equals("N")) {
                        String ss = normalize(convertedASCII.substring(32 + prevCumulativeLength+
                                detailConfigMap.get(p).getStartIdx()-1,32 + prevCumulativeLength+
                                detailConfigMap.get(p).getEndIdx()));
                        if(config.isOccurs()){
                            occuranceKeyJsonObjectMapList[segment].tempJsonObject.put(p.substring(0 , p.length() -2),
                                    ss.replace("\u0000", " "));
                        }
                        else {
                            jObj0422.put(p, ss.replace("\u0000", " "));
                        }
                    }
                    else if(detailConfigMap.get(p).getInType().equals("COMP")) {
                        String ss = ebcdic.substring(32 +prevCumulativeLength
                                +detailConfigMap.get(p).getStartIdx()-1,32 + prevCumulativeLength+
                                detailConfigMap.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }
//                        sb.append("~"+Integer.parseInt(q,16));
                        if(config.isOccurs()){
                            occuranceKeyJsonObjectMapList[segment].tempJsonObject.put(p.substring(0 , p.length() -2),
                                    Integer.parseInt(q,16));
                        }else {
                            jObj0422.put(p, Integer.parseInt(q, 16));
                        }
                    }
                    else if(detailConfigMap.get(p).getInType().equals("COMP3")) {
                        String ss = ebcdic.substring(32 + prevCumulativeLength+detailConfigMap.get(p).getStartIdx()-1,32
                                + prevCumulativeLength+detailConfigMap.get(p).getEndIdx());
                        String q = "";
                        for(char l :ss.toCharArray()) {
                            q =q+getHexValue((Integer.toHexString((int)l)),2);
                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMap0422.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMap0422.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        else {
                            num = Integer.parseInt(ConfigSplit.detailConfigMap0422.get(p).getOutType().
                                    substring(1, ConfigSplit.detailConfigMap0422.get(p).getOutType().length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
//                        sb.append("~" + unpackedData);
                        if(config.isOccurs()){
                            occuranceKeyJsonObjectMapList[segment].tempJsonObject.put(p.substring(0 , p.length() -2),
                                    unpackedData);
                        }else {
                            jObj0422.put(p, unpackedData);
                        }
                    }
                }
            }
        }

        RR.jObj0422 = jObj0422;
        for(int i=0;i< occuranceKeyJsonObjectMapList.length;i++){
            if(occuranceKeyJsonObjectMapList[i]!=null) {
                occuranceKeyJsonObjectMapList[i].updateCurrentOccuranceJsonArrayValues();
                for (Map.Entry<String, JSONArray> entry : occuranceKeyJsonObjectMapList[i].occuranceKeyJsonArrayMap.entrySet()) {
                    String occuranceKey = entry.getKey();
                    JSONArray occuranceValue = entry.getValue();
                    RR.jObj0422.put(occuranceKey, occuranceValue);
                }
            }
        }
        RR.isHeader = true;

        return;

    }





    //////////////////////////////////////////////////////////////////////

    private void loadXBASE1010Record(String convertedASCII, String ebcdic, int record, int index, StringBuilder sb,

                                     long lengthOfChar, String recordType, JSONObject jObjXbase1010) {

        // TODO Auto-generated method stub

        boolean flag =false;

        for(String p : ConfigSplit.detailConfigMap.keySet()) {

            if (p.contains("ETSXBASE") && !flag) {

                flag = true;

            }

            if(!p.contains("ETSXBASE") && flag) {

                break;

            }

            if(p.contains("XBASE") ) {

                if(p.equals(FileConverter_MT.XBASERECLEN)) {

                    if (recordType.equals("1010")) {

                        RR.accNumCheck = true;

/////                        RR.recAccNum = convertedASCII.substring(ConfigSplit.detailConfigMap.get("XBASE-HDR-BR").getStartIdx() + 1,
                        RR.recAccNum = convertedASCII.substring(ConfigSplit.detailConfigMap.get("XBASE-HDR-BR").getStartIdx() - 1,

                                ConfigSplit.detailConfigMap.get("XBASE-HDR-ACT").getEndIdx());

//                        System.out.println("RR.recAccNum ->"+RR.recAccNum);
//                        System.out.println("ConfigSplit.detailConfigMap.get(\"XBASE-HDR-BR\").getStartIdx() ->"+ConfigSplit.detailConfigMap.get("XBASE-HDR-BR").getStartIdx());
//                        System.out.println("ConfigSplit.detailConfigMap.get(\"XBASE-HDR-ACT\").getEndIdx()) ->"+ConfigSplit.detailConfigMap.get("XBASE-HDR-ACT").getEndIdx());

                        if (indexerFlag) {

                            RR.isWriteIndexerFile = true;

                        }

                        indexerFlag = true;

                        RR.updatePrevOffset = true;

/*                        sb.append("REC" + (index) + "~" + ((record+lengthOfChar) - (record) + (ebcdic.indexOf(ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1,

                                ConfigSplit.detailConfigMap.get(p).getEndIdx())))+ (1)) + "~" + ConfigSplit.recordCount.get("ETSXBASE") + "~" + "ETSXBASENEW");
*/
                    }

                    //else {

                    //    System.out.println("Found not 1010 !!");

                    //    sb.append("REC" + (index) + "~" + ((record+lengthOfChar) - (record) + (ebcdic.indexOf(ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1,

                    //            ConfigSplit.detailConfigMap.get(p).getEndIdx())))+ (1)) + "~" + ConfigSplit.recordCount.get("ETSXBASE") + "~" + "ETSXBASE");

                    //}



                }

                if(ConfigSplit.detailConfigMap.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap.get(p).getInType().equals("N")) {
                    String ss = normalize(convertedASCII.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx()));
//                    sb.append("~"+ss);
                    jObjXbase1010.put(p, ss.replace("\u0000", " "));
                }

                else if(ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP")) {
                    String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                    String q = "";
                    for(int l :ss.toCharArray()) {
                        q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                    }
//                    sb.append("~"+Integer.parseInt(q,16));
                    jObjXbase1010.put(p, Integer.parseInt(q,16));
                }

                else if (ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP3")) {

                    String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                    String q = "";
                    for (char l : ss.toCharArray()) {
                        q = q + getHexValue((Integer.toHexString((int) l)), 2);
                    }
                    int dec = 0;
                    int num = 0;
                    if (ConfigSplit.detailConfigMap.get(p).getOutType().length() >= 4) {
                        String[] s1 = ConfigSplit.detailConfigMap.get(p).getOutType().split("V");
                        dec = Integer.parseInt(s1[1]);
                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));



                    }else{
                        num = Integer.parseInt(ConfigSplit.detailConfigMap.get(p).getOutType().substring(1));
                    }

                    String unpackedData = unpackData(q, dec, num);
//                    sb.append("~" + unpackedData);
                    jObjXbase1010.put(p, unpackedData);

                    if(p.equals("XBASE-HDR-CL")) {
                        RR.clientId = unpackedData;
                    }

                }

            }

        }

        return;

    }



    //////////////////////////////////////////////////////////////////////

    private void load2410Records(String convertedASCII, String ebcdic, int record, int index, StringBuilder sb, long lengthOfChar, String recordType,

                                 int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, JSONObject jObj2410) {

        // TODO Auto-generated method stub

        JSONObject jObjOccur24103 = null;

        JSONArray occurArray24103 = new JSONArray();

        String occurValue24103 = "";
        String occurKey24103 = "";

        for(String p : ConfigSplit.detailConfigMap2410.keySet()) {
            if(ConfigSplit.detailConfigMap2410.get(p).isOccurs() && occurKey24103.equals("")) {
                occurKey24103 = ConfigSplit.detailConfigMap2410.get(p).getOccursArrayKey();
            }

            if(p.contains(recordType+"0") && (rec0 != 0)) {

/*                if(p.matches(recordType+"0-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                }
*/
                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx() ) {

                        String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

//                        sb.append("~"+ss);

                        jObj2410.put(p, ss.replace("\u0000", " "));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap2410.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));

                        jObj2410.put(p, Integer.parseInt(q,16));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(char l :ss.toCharArray()) {

                            q =q+getHexValue((Integer.toHexString((int)l)),2);

                        }

                        int dec = 0;

                        int num = 0;

                        if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                            String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                            dec = Integer.parseInt(s1[1]);

                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                        }

                        else {

                            num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                        }

                        String unpackedData = unpackData(q, dec, num);

//                        sb.append("~" + unpackedData);

                        jObj2410.put(p, unpackedData);

                    }

                }

            }


            if(p.contains(recordType+"1") && (rec1 != 0)) {

/*                if(p.matches(recordType+"1-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                }
*/
                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

//                        sb.append("~"+ss);

                        jObj2410.put(p , ss.replace("\u0000", " "));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));

                        jObj2410.put(p, Integer.parseInt(q,16));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    try {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                            String q = "";

                            for(char l :ss.toCharArray()) {

                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }

                            int dec = 0;

                            int num = 0;

                            if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                                String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                                dec = Integer.parseInt(s1[1]);

                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                            }

                            else {

                                num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                            }

                            String unpackedData = unpackData(q, dec, num);

//                            sb.append("~" + unpackedData);

                            jObj2410.put(p, unpackedData);

                        }

                    }catch(Exception e ) {

                        e.printStackTrace();

                    }

                }



            }

            if(p.contains(recordType+"2") && (rec2 != 0)) {



/*                if(p.matches(recordType+"2-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                }
*/


                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx() ) {

                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

//                        sb.append("~"+ss);

                        jObj2410.put(p , ss.replace("\u0000", " "));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx() ) {

                        String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));

                        jObj2410.put(p , Integer.parseInt(q,16));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    try {

                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx() ) {

                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                            String q = "";

                            for(char l :ss.toCharArray()) {

                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }

                            int dec = 0;

                            int num = 0;

                            if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                                String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                                dec = Integer.parseInt(s1[1]);

                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                            }

                            else {

                                num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                            }

                            String unpackedData = unpackData(q, dec, num);

//                            sb.append("~" + unpackedData);

                            jObj2410.put(p, unpackedData);

                        }

                    }catch(Exception e ) {

                        e.printStackTrace();

                    }

                }

            }



            if(p.contains(recordType+"3") && (rec3 != 0)) {

/*                if(p.matches(recordType+"3-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }
*/
                if(ConfigSplit.detailConfigMap2410.get(p).isOccurs()) {
                    if(!occurValue24103.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur24103 != null && !jObjOccur24103.toString().equals("{}")) {
                            occurArray24103.add(jObjOccur24103);
                        }
                        jObjOccur24103 = new JSONObject();
                        occurValue24103 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx() ) {

                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

//                        sb.append("~"+ss);

                        if(ConfigSplit.detailConfigMap2410.get(p).isOccurs()) {
                            jObjOccur24103.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj2410.put(p, ss.replace("\u0000", " "));
                        }

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));

                        if(ConfigSplit.detailConfigMap2410.get(p).isOccurs()) {
                            jObjOccur24103.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj2410.put(p, Integer.parseInt(q,16));
                        }

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                            String q = "";

                            for(char l :ss.toCharArray()) {

                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }

                            int dec = 0;

                            int num = 0;

                            if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                                String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                                dec = Integer.parseInt(s1[1]);

                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                            }

                            else {

                                num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                            }

                            String unpackedData = unpackData(q, dec, num);

//                            sb.append("~" + unpackedData);

                            if(ConfigSplit.detailConfigMap2410.get(p).isOccurs()) {
                                jObjOccur24103.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj2410.put(p, unpackedData);
                            }

                        }

                    }catch(Exception e ) {

                        e.printStackTrace();

                    }

                }

            }

            if(p.contains(recordType+"4") && (rec4 != 0)) {

/*                if(p.matches(recordType+"4-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }
*/
                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

//                        sb.append("~"+ss);

                        jObj2410.put(p , ss.replace("\u0000", " "));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));

                        jObj2410.put(p , Integer.parseInt(q,16));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                            String q = "";

                            for(char l :ss.toCharArray()) {

                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }

                            int dec = 0;

                            int num = 0;

                            if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                                String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                                dec = Integer.parseInt(s1[1]);

                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                            }

                            else {

                                num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                            }

                            String unpackedData = unpackData(q, dec, num);

//                            sb.append("~" + unpackedData);

                            jObj2410.put(p, unpackedData);

                        }

                    }catch(Exception e ) {



                    }

                }

            }

            if(p.contains(recordType+"5") && (rec5 != 0)) {

/*                if(p.matches(recordType+"5-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }
*/
                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss =normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

//                        sb.append("~"+ss);

                        jObj2410.put(p , ss.replace("\u0000", " "));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));

                        jObj2410.put(p , Integer.parseInt(q,16));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                            String q = "";

                            for(char l :ss.toCharArray()) {

                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }

                            int dec = 0;

                            int num = 0;

                            if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                                String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                                dec = Integer.parseInt(s1[1]);

                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                            }

                            else {

                                num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                            }

                            String unpackedData = unpackData(q, dec, num);

//                            sb.append("~" + unpackedData);

                            jObj2410.put(p, unpackedData);

                        }

                    }catch(Exception e ) {



                    }

                }

            }

            if(p.contains(recordType+"6") && (rec6 != 0)) {

/*                if(p.matches(recordType+"6-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }
*/
                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

                        //                       sb.append("~"+ss);

                        jObj2410.put(p , ss.replace("\u0000", " "));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));

                        jObj2410.put(p , Integer.parseInt(q,16));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                            String q = "";

                            for (char l : ss.toCharArray()) {

                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }

                            int dec = 0;

                            int num = 0;

                            if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                                String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                                dec = Integer.parseInt(s1[1]);

                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                            }

                            else {

                                num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                            }

                            String unpackedData = unpackData(q, dec, num);

//                            sb.append("~" + unpackedData);

                            jObj2410.put(p, unpackedData);

                        }

                    }catch(Exception e ) {



                    }

                }

            }

            if(p.contains(recordType+"7") && (rec7 != 0)) {

/*                if(p.matches(recordType+"7-.*SEGLEN")) {

                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }
*/
                if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2410.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()));

//                        sb.append("~"+ss);

                        jObj2410.put(p , ss.replace("\u0000", " "));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                        String q = "";

                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }
//                        sb.append("~"+Integer.parseInt(q,16));

                        jObj2410.put(p , Integer.parseInt(q,16));

                    }

                }

                else if(ConfigSplit.detailConfigMap2410.get(p).getInType().equals("COMP3")) {

                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx()) {

                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2410.get(p).getEndIdx());

                            String q = "";

                            for (char l : ss.toCharArray()) {

                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }

                            int dec = 0;

                            int num = 0;

                            if (ConfigSplit.detailConfigMap2410.get(p).getOutType().length() >= 4) {

                                String[] s1 = ConfigSplit.detailConfigMap2410.get(p).getOutType().split("V");

                                dec = Integer.parseInt(s1[1]);

                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                            }

                            else {

                                num = Integer.parseInt(ConfigSplit.detailConfigMap2410.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2410.get(p).getOutType().length()));

                            }

                            String unpackedData = unpackData(q, dec, num);

//                            sb.append("~" + unpackedData);

                            jObj2410.put(p, unpackedData);

                        }

                    }catch(Exception e ) {



                    }

                }

            }



        }

        if(!occurValue24103.equals("")) {

            if(!jObjOccur24103.toString().equals("{}")) {
                occurArray24103.add(jObjOccur24103);
            }
            jObj2410.put(occurKey24103, occurArray24103);
        }

        RR.jObj2410 = jObj2410;

        return;

    }





    //////////////////////////////////////////////////////////////////////
/*
    private void load5050Records(String convertedASCII, String ebcdic, int record, int index, StringBuilder sb, long lengthOfChar, String recordType,

                                        int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, JSONObject jObj5050) {

        // TODO Auto-generated method stub

    	JSONObject jObjOccur50503 = null;

    	JSONArray occurArray50503 = new JSONArray();

    	String occurValue50503 = "";

    	String occurKey50503 = "";

        for(String p : ConfigSplit.detailConfigMap5050.keySet()) {
        	if(ConfigSplit.detailConfigMap5085.get(p).isOccurs() && !occurKey50503.equals("")) {
        		occurKey50503 = ConfigSplit.detailConfigMap5050.get(p).getOccursArrayKey();
        	}

            if(p.contains(recordType+"0") && (rec0 != 0)) {

            	if(ConfigSplit.detailConfigMap5050.get(p).getRedefine().equals("")) {

            	}

                if(p.matches(recordType+"0-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                }

                if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx() ) {
                        String ss = convertedASCII.substring(32+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5050.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5050.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5050.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(char l :ss.toCharArray()) {
                            q =q+getHexValue((Integer.toHexString((int)l)),2);

                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        else {
//                            num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(0, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
                        sb.append("~" + unpackedData);
                        jObj5050.put(p, unpackedData);
                    }
                }
            }

            else if(p.contains(recordType+"1") && (rec1 != 0)) {

            	if(p.matches(recordType+"1-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                }

            	if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5050.put(p, ss.replace("\u0000", " "));
                    }
                }

            	else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }
                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5050.put(p, Integer.parseInt(q,16));
                    }
                }

            	else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5050.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"2") && (rec2 != 0)) {

                if(p.matches(recordType+"2-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                }

                if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx() ) {
                        String ss = convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5050.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx() ) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5050.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5050.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"3") && (rec3 != 0)) {

                if(p.matches(recordType+"3-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }

                if(ConfigSplit.detailConfigMap5050.get(p).isOccurs()) {
                	if(!occurValue50503.equals(p.substring(p.length() -2, p.length()))) {
                		if(jObjOccur50503 != null && !jObjOccur50503.toString().equals("{}")) {
                			occurArray50503.add(jObjOccur50503);
                		}
                		jObjOccur50503 = new JSONObject();
                		occurValue50503 = p.substring(p.length() -2, p.length());
                	}
                }

                if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx() ) {
                        String ss = convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5050.get(p).isOccurs()) {
                        	jObjOccur50503.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                        	jObj5050.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5050.get(p).isOccurs()) {
                        	jObjOccur50503.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                        	jObj5050.put(p, Integer.parseInt(q,16));
                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);


                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5050.get(p).isOccurs()) {
                            	jObjOccur50503.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                            	jObj5050.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"4") && (rec4 != 0)) {

                if(p.matches(recordType+"4-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }

                if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5050.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5050.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5050.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"5") && (rec5 != 0)) {
                if(p.matches(recordType+"5-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }

                if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5050.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5050.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5050.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"6") && (rec6 != 0)) {

            	if(p.matches(recordType+"6-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }

            	if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5050.put(p, ss.replace("\u0000", " "));
                    }
                }

            	else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5050.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5050.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"7") && (rec7 != 0)) {
                if(p.matches(recordType+"7-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }

                if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5050.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5050.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5050.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5050.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5050.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5050.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5050.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5050.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5050.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5050.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

        }

        if(!occurValue50503.equals("")) {


			if(!jObjOccur50503.toString().equals("{}")) {
				occurArray50503.add(jObjOccur50503);
        	}
        	jObj5050.put(recordType+occurKey50503, occurArray50503);
        }

        return;

    }

*/



    //////////////////////////////////////////////////////////////////////
/*
    private void load5040Records(String convertedASCII, String ebcdic, int record, int index, StringBuilder sb, long lengthOfChar, String recordType,

                                        int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, JSONObject jObj5040) {

        // TODO Auto-generated method stub
    	JSONObject jObjOccur50403 = null;
    	JSONArray occurArray50403 = new JSONArray();
    	String occurValue50403 = "";
    	String occurKey50403 = "";
        for(String p : ConfigSplit.detailConfigMap5040.keySet()) {
        	if(ConfigSplit.detailConfigMap5040.get(p).isOccurs() && !occurKey50403.equals("")) {
        		occurKey50403 = ConfigSplit.detailConfigMap5040.get(p).getOccursArrayKey();
        	}

            if(p.contains(recordType+"0") && (rec0 != 0)) {

            	if(ConfigSplit.detailConfigMap5040.get(p).getRedefine().equals("RDF_PRCD-PEND-INFO")) {
                    continue;
                }

            	if(p.matches(recordType+"0-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                }

                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx() ) {
                        String ss = convertedASCII.substring(32+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5040.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5040.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5040.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(char l :ss.toCharArray()) {
                            q =q+getHexValue((Integer.toHexString((int)l)),2);

                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        else {
                            num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
                        sb.append("~" + unpackedData);
                        jObj5040.put(p, unpackedData);
                    }
                }
            }

            else if(p.contains(recordType+"1") && (rec1 != 0)) {
                if(p.matches(recordType+"1-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                }
                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5040.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }
                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5040.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5040.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"2") && (rec2 != 0)) {

                if(p.matches(recordType+"2-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                }

                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx() ) {
                        String ss = convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5040.put(p, ss.replace("\u0000", " "));
                    }
                }
                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx() ) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5040.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5040.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"3") && (rec3 != 0)) {

                if(p.matches(recordType+"3-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }

                if(ConfigSplit.detailConfigMap5040.get(p).isOccurs()) {
                	if(!occurValue50403.equals(p.substring(p.length() -2, p.length()))) {
                		if(jObjOccur50403 != null && !jObjOccur50403.toString().equals("{}")) {
                			occurArray50403.add(jObjOccur50403);
                		}
                		jObjOccur50403 = new JSONObject();
                		occurValue50403 = p.substring(p.length() -2, p.length());
                	}
                }

                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx() ) {
                        String ss = convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap5040.get(p).isOccurs()) {
                        	jObjOccur50403.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                        	jObj5040.put(p, ss.replace("\u0000", " "));
                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap5040.get(p).isOccurs()) {
                        	jObjOccur50403.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                        	jObj5040.put(p, Integer.parseInt(q,16));
                        }
                    }
                }
                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);


                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap5040.get(p).isOccurs()) {
                            	jObjOccur50403.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                            	jObj5040.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"4") && (rec4 != 0)) {

                if(p.matches(recordType+"4-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }

                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5040.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5040.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5040.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"5") && (rec5 != 0)) {
                if(p.matches(recordType+"5-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }

                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5040.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5040.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5040.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"6") && (rec6 != 0)) {
                if(p.matches(recordType+"6-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }

                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5040.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5040.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5040.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"7") && (rec7 != 0)) {
                if(p.matches(recordType+"7-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }

                if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap5040.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        sb.append("~"+ss);
                        jObj5040.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5040.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap5040.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap5040.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap5040.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap5040.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap5040.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap5040.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
                            sb.append("~" + unpackedData);
                            jObj5040.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

        }

        if(!occurValue50403.equals("")) {

			if(!jObjOccur50403.toString().equals("{}")) {
				occurArray50403.add(jObjOccur50403);
        	}
        	jObj5040.put(recordType+occurKey50403, occurArray50403);
        }

        return;

    }

*/



    //////////////////////////////////////////////////////////////////////

    private void load22NNRecord(String convertedASCII, String ebcdic,int record, int index, StringBuilder sb, long lengthOfChar, String recordType,

                                int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, JSONObject jObj22NN) {

        // TODO Auto-generated method stub
        JSONObject jObjOccur22NN3 = null;
        JSONArray occurArray22NN3 = new JSONArray();
        String occurValue22NN3 = "";
        String occurKey22NN3 = "";

        for(String p : ConfigSplit.detailConfigMap22NN.keySet()) {

            if(ConfigSplit.detailConfigMap22NN.get(p).isOccurs() && !occurKey22NN3.equals("")) {
                occurKey22NN3 = ConfigSplit.detailConfigMap22NN.get(p).getOccursArrayKey();
            }

            if(p.contains(recordType+"0") && (rec0 != 0)) {

/*                if(p.matches(recordType+"0-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj22NN.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj22NN.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(char l :ss.toCharArray()) {
                            q =q+getHexValue((Integer.toHexString((int)l)),2);

                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        else {
                            num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
//                        sb.append("~" + unpackedData);
                        jObj22NN.put(p, unpackedData);
                    }
                }
            }

            if(p.contains(recordType+"1") && (rec1 != 0)) {
/*                if(p.matches(recordType+"1-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj22NN.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj22NN.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj22NN.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            if(p.contains(recordType+"2") && (rec2 != 0)) {

/*                if(p.matches(recordType+"2-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj22NN.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx() ) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj22NN.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj22NN.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }


            if(p.contains(recordType+"3") && (rec3 != 0)) {

/*                if(p.matches(recordType+"3-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).isOccurs()) {
                    if(!occurValue22NN3.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur22NN3 != null && !jObjOccur22NN3.toString().equals("{}")) {
                            occurArray22NN3.add(jObjOccur22NN3);
                        }
                        jObjOccur22NN3 = new JSONObject();
                        occurValue22NN3 = p.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap3010.get(p).isOccurs()) {
                            jObjOccur22NN3.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                        } else {
                            jObj22NN.put(p, ss.replace("\u0000", " "));
                        }
                    }

                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap3010.get(p).isOccurs()) {
                            jObjOccur22NN3.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj22NN.put(p, Integer.parseInt(q,16));
                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);


                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap3010.get(p).isOccurs()) {
                                jObjOccur22NN3.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj22NN.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            if(p.contains(recordType+"4") && (rec4 != 0)) {

/*                if(p.matches(recordType+"4-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj22NN.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj22NN.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj22NN.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            if(p.contains(recordType+"5") && (rec5 != 0)) {
/*                if(p.matches(recordType+"5-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj22NN.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj22NN.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj22NN.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            if(p.contains(recordType+"6") && (rec6 != 0)) {
/*                if(p.matches(recordType+"6-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj22NN.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj22NN.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj22NN.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            if(p.contains(recordType+"7") && (rec7 != 0)) {
/*                if(p.matches(recordType+"7-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2201.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2201.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }
*/
                if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj22NN.put(p, ss.replace("\u0000", " "));
                    }
                }
                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj22NN.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap22NN.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap22NN.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap22NN.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap22NN.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap22NN.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap22NN.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj22NN.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
        }
        if(!occurValue22NN3.equals("")) {

            if(!jObjOccur22NN3.toString().equals("{}")) {
                occurArray22NN3.add(jObjOccur22NN3);
            }
            jObj22NN.put(occurKey22NN3, occurArray22NN3);
        }
        RR.jObj22NN = jObj22NN;

        return;

    }





    //////////////////////////////////////////////////////////////////////

    private void load1010Records(String convertedASCII, String ebcdic,int record, int index, StringBuilder sb1, long lengthOfChar, String recordType,

                                 int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, JSONObject jObj1010) {
        // TODO Auto-generated method stub
        try {


            StringBuilder sb = new StringBuilder();
            JSONObject jObjOccur10100 = null;
            JSONArray occurArray10100 = new JSONArray();
            String occurValue10100 = "";
            String occurKey10100 = "";
            JSONObject jObjOccur10101 = null;
            JSONArray occurArray10101 = new JSONArray();
            String occurValue10101 = "";
            String occurKey10101 = "";
            JSONObject jObjOccur10103 = null;
            JSONArray occurArray10103 = new JSONArray();
            String occurValue10103 = "";
            String occurKey10103 = "";
            for(String p : ConfigSplit.detailConfigMap1010.keySet()) {
                if(p.contains(recordType+"0") && (rec0 != 0)) {
                    if(ConfigSplit.detailConfigMap1010.get(p).isOccurs() && occurKey10100.equals("")) {
                        occurKey10100 = ConfigSplit.detailConfigMap1010.get(p).getOccursArrayKey();
                    }

/*                    if(p.matches(recordType+"0-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                    }
 */
                    if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                        if(!occurValue10100.equals(p.substring(p.length() -2, p.length()))) {
                            if(jObjOccur10100 != null && !jObjOccur10100.toString().equals("{}")) {
                                occurArray10100.add(jObjOccur10100);
                            }
                            jObjOccur10100 = new JSONObject();
                            occurValue10100 = p.substring(p.length() -2, p.length());
                        }
                    }

                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                jObjOccur10100.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                            } else {
                                jObj1010.put(p, ss.replace("\u0000", " "));
                            }
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap1010.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj1010.put(p, Integer.parseInt(q,16));
                            if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                jObjOccur10100.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                            } else {
                                jObj1010.put(p, Integer.parseInt(q,16));
                            }
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                jObjOccur10100.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj1010.put(p, unpackedData);
                            }
                        }
                    }
                }
                if(p.contains(recordType+"1") && (rec1 != 0)) {
                    if(ConfigSplit.detailConfigMap1010.get(p).isOccurs() && !occurKey10101.equals("")) {
                        occurKey10101 = ConfigSplit.detailConfigMap1010.get(p).getOccursArrayKey();
                    }
/*                    if(p.matches(recordType+"1-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                    }
*/
                    if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                        if(!occurValue10101.equals(p.substring(p.length() -2, p.length()))) {
                            if(jObjOccur10101 != null && !jObjOccur10101.toString().equals("{}")) {
                                occurArray10101.add(jObjOccur10101);
                            }
                            jObjOccur10101 = new JSONObject();
                            occurValue10101 = p.substring(p.length() -2, p.length());
                        }
                    }

                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                jObjOccur10101.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                            } else {
                                jObj1010.put(p, ss.replace("\u0000", " "));
                            }
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                jObjOccur10101.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                            } else {
                                jObj1010.put(p, Integer.parseInt(q,16));
                            }
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        try {

                            if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                    jObjOccur10101.put(p.substring(0 , p.length() -2), unpackedData);
                                } else {
                                    jObj1010.put(p, unpackedData);
                                }
                            }
                        }catch(Exception e ) {

                        }
                    }
                }

                if(p.contains(recordType+"2") && (rec2 != 0)) {
/*                    if(p.matches(recordType+"2-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                    }
*/
                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {
                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj1010.put(p, ss.replace("\u0000", " "));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj1010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        try {

                            if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx() ) {
                                String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                jObj1010.put(p, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                }


                if(p.contains(recordType+"3") && (rec3 != 0)) {
                    if(ConfigSplit.detailConfigMap1010.get(p).isOccurs() && occurKey10103.equals("")) {
                        occurKey10103 = ConfigSplit.detailConfigMap1010.get(p).getOccursArrayKey();
                    }
/*                    if(p.matches(recordType+"3-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                    }
*/
                    if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                        if(!occurValue10103.equals(p.substring(p.length() -2, p.length()))) {
                            if(jObjOccur10103 != null && !jObjOccur10103.toString().equals("{}")) {
                                occurArray10103.add(jObjOccur10103);
                            }
                            jObjOccur10103 = new JSONObject();
                            occurValue10103 = p.substring(p.length() -2, p.length());
                        }
                    }

                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                jObjOccur10103.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));
                            } else {
                                jObj1010.put(p, ss.replace("\u0000", " "));
                            }

                        }

                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                jObjOccur10103.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                            } else {
                                jObj1010.put(p, Integer.parseInt(q,16));
                            }
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        try {

                            if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                                String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);


                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                if(ConfigSplit.detailConfigMap1010.get(p).isOccurs()) {
                                    jObjOccur10103.put(p.substring(0 , p.length() -2), unpackedData);
                                } else {
                                    jObj1010.put(p, unpackedData);
                                }
                            }
                        }catch(Exception e ) {

                        }
                    }

                }
                if(p.contains(recordType+"4") && (rec4 != 0)) {
/*                    if(p.matches(recordType+"4-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                    }
*/
                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj1010.put(p, ss.replace("\u0000", " "));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj1010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        try {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                jObj1010.put(p, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                }

                if(p.contains(recordType+"5") && (rec5 != 0)) {
/*                    if(p.matches(recordType+"5-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                    }
*/
                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj1010.put(p, ss.replace("\u0000", " "));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj1010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        try {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                jObj1010.put(p, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                }
                if(p.contains(recordType+"6") && (rec6 != 0)) {
/*                	if(p.matches(recordType+"6-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                    }
*/
                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj1010.put(p, ss.replace("\u0000", " "));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj1010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        try {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                                String q = "";
                                for (char l : ss.toCharArray()) {
                                    q = q + getHexValue((Integer.toHexString((int) l)), 2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                jObj1010.put(p, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                }
                if(p.contains(recordType+"7") && (rec7 != 0)) {

/*                	if(p.matches(recordType+"7-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                    }
*/
                    if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap1010.get(p).getInType().equals("N")) {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj1010.put(p, ss.replace("\u0000", " "));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj1010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap1010.get(p).getInType().equals("COMP3")) {
                        try {

                            if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap1010.get(p).getEndIdx());
                                String q = "";
                                for (char l : ss.toCharArray()) {
                                    q = q + getHexValue((Integer.toHexString((int) l)), 2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap1010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap1010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap1010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap1010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
                                //                               sb.append("~" + unpackedData);
                                jObj1010.put(p, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                }
            }

            /*String [] segments1010 = sb.toString().split("\n");

            for(int i =0 ; i < segments1010.length ; i++) {

                if(segments1010[i].contains("ETS10100")) {

                    String [] segments1010Details = segments1010[i].split("~");

                    //RR.millionInd = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10100-MILL-MKT-VAL-IND").getfId()) + 3];
                    //RR.printInd = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10100-ACCT-PRINT-IND").getfId()) + 3];
                }


                if(segments1010[i].contains("ETS10102")) {

                    String [] segments1010Details = segments1010[i].split("~");
                    String tempBranch = " ";
                    String tempAccount = " ";

                    //RR.xferToDate = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10102-XFER-TO-DATE").getfId()) + 3];
                    //RR.xferFromDate = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10102-XFER-FROM-DATE").getfId()) + 3];

                    //tempBranch = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10102-XFER-TO-BRANCH").getfId()) + 3];
                    //tempAccount = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10102-XFER-TO-ACCOUNT").getfId()) + 3];
                    //RR.xferToAccount = tempBranch+tempAccount;

                    tempBranch = " ";
                    tempAccount = " ";
                    //tempBranch = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10102-XFER-FROM-BRANCH").getfId()) + 3];
                    //tempAccount = segments1010Details[Integer.parseInt(ConfigSplit.detailConfigMap1010.get("10102-XFER-FROM-ACCOUNT").getfId()) + 3];
                    //RR.xferFromAccount = tempBranch+tempAccount;

                }


            }*/

            if(!occurValue10100.equals("")) {

                if(!jObjOccur10100.toString().equals("{}")) {
                    occurArray10100.add(jObjOccur10100);
                }
                jObj1010.put(occurKey10100, occurArray10100);
            }

            if(!occurValue10101.equals("")) {

                if(!jObjOccur10101.toString().equals("{}")) {
                    occurArray10101.add(jObjOccur10101);
                }
                jObj1010.put(occurKey10101, occurArray10101);
            }

            if(!occurValue10103.equals("")) {

                if(!jObjOccur10103.toString().equals("{}")) {
                    occurArray10103.add(jObjOccur10103);
                }
                jObj1010.put(occurKey10103, occurArray10103);
            }

            // Register currency suppression based on 10100-ACH-PRINT-IND
     try {
         if (jObj1010.containsKey("10100-ACH-PRINT-IND") && currencySuppressionService.isEnabled()) {
             String achPrintInd = String.valueOf(jObj1010.get("10100-ACH-PRINT-IND")).trim();
             String accountNumber = convertedASCII.substring(4, 12).trim();
             String currency = "";
             if (convertedASCII.length() >= 15) {
                 currency = convertedASCII.substring(12, 15).trim();
             }
     
             // Client ID needed for account_filter_file gating
             String clientId = "";
             if (jObj1010.containsKey("XBASE-HDR-CL")) {
                 clientId = String.valueOf(jObj1010.get("XBASE-HDR-CL")).replace("\u0000", " ").trim();
             } else if (RR.clientId != null) {
                 clientId = RR.clientId.trim();
             }
     
             boolean passesAccountFilter = !accountFilterService.isEnabled()
                     || (!accountNumber.isEmpty() && !clientId.isEmpty()
                     && accountFilterService.shouldEmit(accountNumber, clientId));
     
             if (passesAccountFilter && !accountNumber.isEmpty() && !currency.isEmpty()) {
                 currencySuppressionService.registerSuppression(accountNumber, currency, achPrintInd);
                 logger.debug("Registered suppression for account={}, currency={}, ACH-PRINT-IND={}",
                         accountNumber, currency, achPrintInd);
             }
         }
     } catch (Exception e) {
         logger.warn("Error registering currency suppression: {}", e.getMessage());
     }
	 
            //           sb1.append(sb.toString());

            return;



        } catch(Exception e) {

            e.printStackTrace();

        }
    }



    //////////////////////////////////////////////////////////////////////

    private void load2010Records(String convertedASCII, String ebcdic, int record, int index, StringBuilder sb1, long lengthOfChar, String recordType,

                                 int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, JSONObject jObj2010) {

        // TODO Auto-generated method stub

        StringBuilder sb = new StringBuilder();

        JSONObject jObjOccur20103 = null;

        JSONArray occurArray20103 = new JSONArray();

        String occurValue20103 = "";

        String occurKey20103 = "";

        JSONObject jObjOccur20104 = null;

        JSONArray occurArray20104 = new JSONArray();

        String occurValue20104 = "";

        String occurKey20104 = "";

        for(String p : ConfigSplit.detailConfigMap2010.keySet()) {
            if(p.contains(recordType+"0") && (rec0 != 0)) {

 /*               if(p.matches(recordType+"0-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                }
 */
                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj2010.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap2010.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj2010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(char l :ss.toCharArray()) {
                            q =q+getHexValue((Integer.toHexString((int)l)),2);

                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        else {
                            num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
//                        sb.append("~" + unpackedData);
                        jObj2010.put(p, unpackedData);
                    }
                }
            }

            else if(p.contains(recordType+"1") && (rec1 != 0)) {
/*                if(p.matches(recordType+"1-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                }
*/
                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj2010.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj2010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj2010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"2") && (rec2 != 0)) {

/*                if(p.matches(recordType+"2-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                }
*/
                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);

                        jObj2010.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx() ) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj2010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj2010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"3") && (rec3 != 0)) {

                if(ConfigSplit.detailConfigMap2010.get(p).isOccurs() && occurKey20103.equals("")) {
                    occurKey20103 = ConfigSplit.detailConfigMap2010.get(p).getOccursArrayKey();
                }

/*            	if(p.matches(recordType+"3-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }
*/
                if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                    if(!occurValue20103.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur20103 != null && !jObjOccur20103.toString().equals("{}")) {
                            occurArray20103.add(jObjOccur20103);
                        }
                        jObjOccur20103 = new JSONObject();
                        occurValue20103 = p.substring(p.length() -2, p.length());
                    }

                }

                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                            jObjOccur20103.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));

                        } else {

                            jObj2010.put(p, ss.replace("\u0000", " "));

                        }
                    }

                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                            jObjOccur20103.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));

                        } else {

                            jObj2010.put(p, Integer.parseInt(q,16));

                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);


                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);

                            if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                                jObjOccur20103.put(p.substring(0 , p.length() -2), unpackedData);

                            } else {

                                jObj2010.put(p, unpackedData);

                            }
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"4") && (rec4 != 0)) {

                if(ConfigSplit.detailConfigMap2010.get(p).isOccurs() && !occurKey20104.equals("")) {
                    occurKey20104 = ConfigSplit.detailConfigMap2010.get(p).getOccursArrayKey();
                }

/*            	if(p.matches(recordType+"4-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }
*/
                if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                    if(!occurValue20104.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur20104 != null && !jObjOccur20104.toString().equals("{}")) {
                            occurArray20104.add(jObjOccur20104);
                        }
                        jObjOccur20104 = new JSONObject();
                        occurValue20104 = p.substring(p.length() -2, p.length());
                    }

                }

                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                            jObjOccur20104.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " "));

                        } else {

                            jObj2010.put(p, ss.replace("\u0000", " "));

                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                            jObjOccur20104.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));

                        } else {

                            jObj2010.put(p, Integer.parseInt(q,16));

                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap2010.get(p).isOccurs()) {

                                jObjOccur20104.put(p.substring(0 , p.length() -2), unpackedData);

                            } else {

                                jObj2010.put(p, unpackedData);

                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"5") && (rec5 != 0)) {
/*                if(p.matches(recordType+"5-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }
*/
                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj2010.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj2010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj2010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"6") && (rec6 != 0)) {
/*                if(p.matches(recordType+"6-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }
*/
                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj2010.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj2010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj2010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"7") && (rec7 != 0)) {
/*                if(p.matches(recordType+"7-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }
*/
                if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj2010.put(p, ss.replace("\u0000", " "));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj2010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap2010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap2010.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap2010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap2010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap2010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap2010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj2010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
        }

        /*
         * String [] segments2010 = sb.toString().split("\n");
         *
         * for(int i =0 ; i < segments2010.length ; i++) {
         *
         * if(segments2010[i].contains("ETS20106")) {
         *
         * String [] segments2010Details = segments2010[i].split("~");
         *
         * RR.topAcct =
         * segments2010Details[Integer.parseInt(ConfigSplit.detailConfigMap2010.
         * get("20106-SLEEVE TOP ACCOUNT").getfId()) + 3].trim();
         *
         * RR.uanNum =
         * segments2010Details[Integer.parseInt(ConfigSplit.detailConfigMap2010.get(
         * "20106-UNIV-ACCT-NBR").getfId()) + 3].trim(); // ACCT_SWING
         *
         * }
         *
         * }
         */

        if(!occurValue20103.equals("")) {

            if(!jObjOccur20103.toString().equals("{}")) {
                occurArray20103.add(jObjOccur20103);
            }
            jObj2010.put(occurKey20103, occurArray20103);

        }

        if(!occurValue20104.equals("")) {

            if(!jObjOccur20104.toString().equals("{}")) {
                occurArray20104.add(jObjOccur20104);
            }
            jObj2010.put(occurKey20104, occurArray20104);
        }

//        sb1.append(sb.toString());
        return;

    }





    //////////////////////////////////////////////////////////////////////

    private void load3010Records(String convertedASCII, String ebcdic, int record, int index, StringBuilder sb, long lengthOfChar, String recordType,

                                 int rec0, int rec1, int rec2, int rec3, int rec4, int rec5, int rec6, int rec7, JSONObject jObj3010) {

        // TODO Auto-generated method stub

        boolean bondFlag = false;

        JSONObject jObjOccur30103 = null;

        JSONArray occurArray30103 = new JSONArray();

        String occurValue30103 = "";

        String occurKey30103 = "";

        String tridType = convertedASCII.substring(32+ConfigSplit.detailConfigMap3010.get("30100-DTL-OLD-TRID").getStartIdx()-1,
                32+ConfigSplit.detailConfigMap3010.get("30100-DTL-OLD-TRID").getEndIdx());

        if(tridType.equals("G") || tridType.equals("V") || tridType.equals("W") || tridType.equals("Z") || tridType.equals("A")) {

            bondFlag = true;

        }

        for(String p : ConfigSplit.detailConfigMap3010.keySet()) {
            if(p.contains(recordType+"0") && (rec0 != 0)) {
                if(bondFlag) {
                    if(ConfigSplit.detailConfigMap3010.get(p).getRedefine().equals("RDF_DTL-P-S-DATA1")) {
                        continue;
                    }
 /*                   if(p.matches(recordType+"0-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                    }
 */
                    if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
                            //                           sb.append("~"+ss);
                            if (
                            	    ConfigSplit.detailConfigMap3010.get(p).getFieldName().equals("30100-DTL-ENTRY-CODE") ||
                            	    ConfigSplit.detailConfigMap3010.get(p).getFieldName().equals("30100-DTL-BATCH")
                            	) {
                            	    jObj3010.put(p, ss.replaceAll("\\\\u000d\\\\u000a\\s*", " "));
                            	} else {
                            	    jObj3010.put(p, ss.replaceAll("\\\\u000d\\\\u000a\\s*", " ").trim());
                            	}                            
                        }
                    }

                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }
//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj3010.put(p, Integer.parseInt(q,16));
                        }
                    }
                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);
                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj3010.put(p, unpackedData);
                        }
                    }
                } else {

                    if(ConfigSplit.detailConfigMap3010.get(p).getRedefine().equals("RDF_DTL-BK-DATA1")) {
                        continue;
                    }
/*                    if(p.matches(recordType+"0-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");
                    }
*/
                    if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            if(ConfigSplit.detailConfigMap3010.get(p).getFieldName().equals("30100-DTL-ENTRY-CODE")) {
                            	jObj3010.put(p, ss.replace("\u0000", " "));
                            } else {
                            	jObj3010.put(p, ss.replace("\u0000", " ").trim());
                            }
                        }
                    }

                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj3010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                        if((32 + rec0) >= (32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj3010.put(p, unpackedData);
                        }
                    }
                }
            }
            else if(p.contains(recordType+"1") && (rec1 != 0)) {
/*                if(p.matches(recordType+"1-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                }
*/
                if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj3010.put(p, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj3010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj3010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"2") && (rec2 != 0)) {
                if(bondFlag) {
                    if(ConfigSplit.detailConfigMap3010.get(p).getRedefine().equals("REDEFINE_BKP")) {
                        continue;
                    }

/*                    if(p.matches(recordType+"2-BOND-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                    }
*/
                    if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {
                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj3010.put(p, ss.replace("\u0000", " ").trim());
                        }
                    }

                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj3010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                        try {

                            if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                                String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                jObj3010.put(p, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                } else {
                    if(ConfigSplit.detailConfigMap3010.get(p).getRedefine().equals("REDEFINE_PS")) {
                        continue;
                    }

/*            		if(p.matches(recordType+"2-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                    }
*/
                    if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {
                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                            String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj3010.put(p, ss.replace("\u0000", " ").trim());
                        }
                    }

                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj3010.put(p, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                        try {

                            if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                                String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                                }

                                String unpackedData = unpackData(q, dec, num);
                                if(unpackedData.equals("404040404040404.04")) {
//                            		sb.append("~" + "000000000000000.00");
                                    jObj3010.put(p, "000000000000000.00");
                                } else {
                                    sb.append("~" + unpackedData);
                                    jObj3010.put(p, unpackedData);
                                }

                            }
                        }catch(Exception e ) {

                        }
                    }
                }
            }

            else if(p.contains(recordType+"3") && (rec3 != 0)) {

                if(ConfigSplit.detailConfigMap3010.get(p).isOccurs() && occurKey30103.equals("")) {
                    occurKey30103 = ConfigSplit.detailConfigMap3010.get(p).getOccursArrayKey();
                }

/*                if(p.matches(recordType+"3-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }
*/
                if(ConfigSplit.detailConfigMap3010.get(p).isOccurs()) {
                    if(!occurValue30103.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur30103 != null && !jObjOccur30103.toString().equals("{}")) {
                            occurArray30103.add(jObjOccur30103);
                        }
                        jObjOccur30103 = new JSONObject();
                        occurValue30103 = p.substring(p.length() -2, p.length());
                    }
                }
                if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMap3010.get(p).isOccurs()) {
                            jObjOccur30103.put(p.substring(0 , p.length() -2), ss.replace("\u0000", " ").trim());
                        } else {
                            jObj3010.put(p, ss.replace("\u0000", " ").trim());
                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMap3010.get(p).isOccurs()) {
                            jObjOccur30103.put(p.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj3010.put(p, Integer.parseInt(q,16));
                        }
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);


                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMap3010.get(p).isOccurs()) {
                                jObjOccur30103.put(p.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj3010.put(p, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }

            }
            else if(p.contains(recordType+"4") && (rec4 != 0)) {

/*                if(p.matches(recordType+"4-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }
*/
                if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj3010.put(p, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj3010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj3010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"5") && (rec5 != 0)) {
/*                if(p.matches(recordType+"5-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }
*/
                if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj3010.put(p, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj3010.put(p, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj3010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"6") && (rec6 != 0)) {
/*                if(p.matches(recordType+"6-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }
*/
                if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj3010.put(p, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj3010.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj3010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"7") && (rec7 != 0)) {
/*                if(p.matches(recordType+"7-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }
*/
                if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3010.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj3010.put(p, ss.replace("\u0000", " ").trim());
                    }
                }
                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj3010.put(p, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMap3010.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMap3010.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMap3010.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMap3010.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMap3010.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap3010.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj3010.put(p, unpackedData);
                        }
                    }catch(Exception e ) {
                        e.printStackTrace();
                    }
                }
            }
        }

        if(!occurValue30103.equals("")) {

            if(!jObjOccur30103.toString().equals("{}")) {
                occurArray30103.add(jObjOccur30103);
            }
            jObj3010.put(occurKey30103, occurArray30103);
        }
        return;

    }


    //////////////////////////////////////////////////////////////////////

    private void load5030Records(String convertedASCII,String ebcdic, int record, int index, StringBuilder sb, long lengthOfChar,String recordType,

                                 int rec0 , int rec1, int rec2 , int rec3 , int rec4, int rec5 , int rec6 , int rec7 , JSONObject jObj5030) {

        // TODO Auto-generated method stub

        String posType = "";

        JSONObject jObjOccur50303 = null;

        JSONArray occurArray50303 = new JSONArray();

        String occurValue50303 = "";
        String occurKey50303 = "";
        String recordTypeFronInput = recordType;
        RR.recordType = "ETS" + recordType;


        if(recordType.startsWith("3P3")) {
            ConfigSplit.detailConfigMapPositions = ConfigSplit.detailConfigMap3P30;
            recordType = "3P30";
        } else {
            ConfigSplit.detailConfigMapPositions = ConfigSplit.detailConfigMap5030;
            recordType = "5030";
        }

        //For Wells Files we will use 3P30 and 5030
        if (clientID.equals("381") || clientID.equals("117") || clientID.equals("127")) {
            if(recordType.startsWith("3P3")) {
                RR.recordType = "ETS" + recordType;
            } else {
                ConfigSplit.detailConfigMapPositions = ConfigSplit.detailConfigMap5030;
                RR.recordType = "ETS" + recordType;
            }
            recordTypeFronInput = recordType;
        }



        for(String p : ConfigSplit.detailConfigMapPositions.keySet()) {
            String replacedP = p.replace(recordType,recordTypeFronInput);

            if(p.contains(recordType+"0") && (rec0 != 0)) {

                if(ConfigSplit.detailConfigMapPositions.get(p).getRedefine().equals("RDF_POS-INFO")) {
                    continue;
                }

/*          	if(p.matches(recordType+"0-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"0")+"~"+"ETS"+recordType+"0");

                }
*/
                if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(p.equals("50300-POS-CLASS-CODE")) {
                            posType = ss.substring(0,1);
                        }
                        jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                    }
                }
                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())  && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5030.put(replacedP, Integer.parseInt(q,16));
                    }
                }
                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                    if((32 + rec0) >= (32+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(char l :ss.toCharArray()) {
                            q =q+getHexValue((Integer.toHexString((int)l)),2);

                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        else {
                            num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
//                        sb.append("~" + unpackedData);
                        jObj5030.put(replacedP, unpackedData);
                    }
                }
            }

            else if(p.contains(recordType+"1") && (rec1 != 0)) {
                if(posType.equals("2") || posType.equals("3") || posType.equals("4") || posType.equals("7") || posType.equals("8")) {

                    if(ConfigSplit.detailConfigMapPositions.get(p).getRedefine().equals("RDF_XPO-BOND-DATA")) {
                        continue;
                    }

/*                	if(p.matches(recordType+"1-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                    }
*/
                    if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                        }
                    }

                    else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }

//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj5030.put(replacedP, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                        try {

                            if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                jObj5030.put(replacedP, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                } else {

                    if(ConfigSplit.detailConfigMapPositions.get(p).getRedefine().equals("RDF_XPO-OPTIONS-DATA")) {
                        continue;
                    }

/*                	if(p.matches(recordType+"1-.*SEGLEN")) {
                        sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                                (ebcdic.indexOf(ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                                +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"1")+"~"+"ETS"+recordType+"1");

                    }
*/
                    if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {

                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = normalize(convertedASCII.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                            sb.append("~"+ss);
                            jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                        }
                    }

                    else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                        if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for(int l :ss.toCharArray()) {
                                q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                            }
//                            sb.append("~"+Integer.parseInt(q,16));
                            jObj5030.put(replacedP, Integer.parseInt(q,16));
                        }
                    }

                    else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                        try {

                            if((32 + rec0 +rec1) >= (32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                                String ss = ebcdic.substring(32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                                String q = "";
                                for(char l :ss.toCharArray()) {
                                    q =q+getHexValue((Integer.toHexString((int)l)),2);

                                }
                                int dec = 0;
                                int num = 0;
                                if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                    String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                    dec = Integer.parseInt(s1[1]);
                                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                }
                                else {
                                    num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                                }
                                String unpackedData = unpackData(q, dec, num);
//                                sb.append("~" + unpackedData);
                                jObj5030.put(replacedP, unpackedData);
                            }
                        }catch(Exception e ) {

                        }
                    }
                }
            }
            else if(p.contains(recordType+"2") && (rec2 != 0)) {

/*                if(p.matches(recordType+"2-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"2")+"~"+"ETS"+recordType+"2");

                }
*/
                if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx() ) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5030.put(replacedP, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2) >= ( 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx() ) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj5030.put(replacedP, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"3") && (rec3 != 0)) {

//            	if(ConfigSplit.detailConfigMapPositions.get(p).isOccurs() && !occurKey50303.equals("")) {
                if(ConfigSplit.detailConfigMapPositions.get(p).isOccurs() && occurKey50303.equals("")) {
                    occurKey50303 = ConfigSplit.detailConfigMapPositions.get(p).getOccursArrayKey();

                }

/*                if(p.matches(recordType+"3-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"3")+"~"+"ETS"+recordType+"3");

                }
*/
                if(ConfigSplit.detailConfigMapPositions.get(p).isOccurs()) {
                    if(!occurValue50303.equals(p.substring(p.length() -2, p.length()))) {
                        if(jObjOccur50303 != null && !jObjOccur50303.toString().equals("{}")) {
                            occurArray50303.add(jObjOccur50303);
                        }
                        jObjOccur50303 = new JSONObject();
                        occurValue50303 = replacedP.substring(p.length() -2, p.length());
                    }
                }

                if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {

                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx() ) {
                        String ss = normalize(convertedASCII.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        if(ConfigSplit.detailConfigMapPositions.get(p).isOccurs()) {
                            if(ss.contains("ACCESS FUND")){
                                System.out.println(ss);
                            }
                            jObjOccur50303.put(replacedP.substring(0 , p.length() -2), ss.replace("\u0000", " ").trim());
                        } else {
                            jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                        }
                    }

                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                    if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        if(ConfigSplit.detailConfigMapPositions.get(p).isOccurs()) {
                            jObjOccur50303.put(replacedP.substring(0 , p.length() -2), Integer.parseInt(q,16));
                        } else {
                            jObj5030.put(replacedP, Integer.parseInt(q,16));
                        }
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                    try {

                        if(( 32 + rec0 +rec1 +rec2 +rec3) >= ( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 + rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = ebcdic.substring( 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1, 32 + rec0 +rec1 +rec2+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);


                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            if(ConfigSplit.detailConfigMapPositions.get(p).isOccurs()) {
                                jObjOccur50303.put(replacedP.substring(0 , p.length() -2), unpackedData);
                            } else {
                                jObj5030.put(replacedP, unpackedData);
                            }
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"4") && (rec4 != 0)) {

/*                if(p.matches(recordType+"4-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"4")+"~"+"ETS"+recordType+"4");

                }
*/
                if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5030.put(replacedP, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4) >= (32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj5030.put(replacedP, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

            else if(p.contains(recordType+"5") && (rec5 != 0)) {
/*                if(p.matches(recordType+"5-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"5")+"~"+"ETS"+recordType+"5");

                }
*/
                if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5030.put(replacedP, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for(char l :ss.toCharArray()) {
                                q =q+getHexValue((Integer.toHexString((int)l)),2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj5030.put(replacedP, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"6") && (rec6 != 0)) {
/*                if(p.matches(recordType+"6-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"6")+"~"+"ETS"+recordType+"6");

                }
*/
                if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5030.put(replacedP, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj5030.put(replacedP, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }
            else if(p.contains(recordType+"7") && (rec7 != 0)) {


/*            	if(p.matches(recordType+"7-.*SEGLEN")) {
                    sb.append("\n"+"REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                            (ebcdic.indexOf(ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx())))
                            +(1))+"~"+ConfigSplit.recordCount.get("ETS"+recordType+"7")+"~"+"ETS"+recordType+"7");

                }
*/
                if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("N")) {

                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = normalize(convertedASCII.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()));
//                        sb.append("~"+ss);
                        jObj5030.put(replacedP, ss.replace("\u0000", " ").trim());
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP")) {
                    if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                        String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                        String q = "";
                        for(int l :ss.toCharArray()) {
                            q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                        }

//                        sb.append("~"+Integer.parseInt(q,16));
                        jObj5030.put(replacedP, Integer.parseInt(q,16));
                    }
                }

                else if(ConfigSplit.detailConfigMapPositions.get(p).getInType().equals("COMP3")) {
                    try {

                        if((32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+rec7) >= (32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) && convertedASCII.length() >= 32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+ rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx()) {
                            String ss = ebcdic.substring(32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getStartIdx()-1,32 + rec0 +rec1 +rec2 +rec3 +rec4+rec5+rec6+ConfigSplit.detailConfigMapPositions.get(p).getEndIdx());
                            String q = "";
                            for (char l : ss.toCharArray()) {
                                q = q + getHexValue((Integer.toHexString((int) l)), 2);

                            }
                            int dec = 0;
                            int num = 0;
                            if (ConfigSplit.detailConfigMapPositions.get(p).getOutType().length() >= 4) {
                                String[] s1 = ConfigSplit.detailConfigMapPositions.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                            }
                            else {
                                num = Integer.parseInt(ConfigSplit.detailConfigMapPositions.get(p).getOutType().substring(1, ConfigSplit.detailConfigMapPositions.get(p).getOutType().length()));
                            }
                            String unpackedData = unpackData(q, dec, num);
//                            sb.append("~" + unpackedData);
                            jObj5030.put(replacedP, unpackedData);
                        }
                    }catch(Exception e ) {

                    }
                }
            }

        }
        if(!occurValue50303.equals("")) {

            if(!jObjOccur50303.toString().equals("{}")) {
                occurArray50303.add(jObjOccur50303);
            }
//        	jObj5030.put(recordType+occurKey50303, occurArray50303);
            jObj5030.put(occurKey50303.replace(recordType,recordTypeFronInput), occurArray50303);
        }

        return;

    }



    //////////////////////////////////////////////////////////////////////

    public String getHexValue(String value, int length) {

        StringBuffer sb = new StringBuffer();

        if(value.length() < length){

            int v = length - value.length();

            for(int i= 0 ; i< v ; i++){

                sb.append("0");

            }

            sb.append(value);

            value = sb.toString();

        }else if(value.length() > length){

            value.substring(0, length);

        }

        return value;

    }



    //////////////////////////////////////////////////////////////////////

    public String unpackData(String packedData, int decimalPointLocation, int num) {

        String unpackedData = "-";

        String pack = packedData;

        if (!packedData.substring(packedData.length() - 1).equals("d")) {

            if(packedData.substring(0, packedData.length() - 1).length() == (num+decimalPointLocation)) {

                pack = packedData.substring(0, packedData.length() - 1);

            }

            else if(packedData.substring(0, packedData.length() - 1).length() > (num+decimalPointLocation)) {

                pack = packedData.substring(1, packedData.length() - 1);

            }



        }

        if (packedData.substring(packedData.length() - 1).equals("d")) {

            pack = unpackedData + packedData.substring(0, packedData.length() - 1);

        }



        if (decimalPointLocation > 2) {

            return pack.substring(0, pack.length() - decimalPointLocation) + "."

                    + pack.substring(pack.length() - decimalPointLocation, pack.length());

        } else if (decimalPointLocation == 2) {

            return pack.substring(0, pack.length() - 2) + "." + pack.substring(pack.length() - 2);

        } else {

            return pack;

        }

    }



    //////////////////////////////////////////////////////////////////////

    private void loadXBASERecord(String convertedASCII, String ebcdic, int record, int index,

                                 StringBuilder sb, long lengthOfChar , String recordType, JSONObject jObjXbase) {

        // TODO Auto-generated method stub

        boolean flag = false;

        for(String p : ConfigSplit.detailConfigMap.keySet()) {

            if (p.contains("XBASE") && !flag) {

                flag = true;

            }

            if(!p.contains("XBASE") && flag) {

                break;

            }

            if(p.contains("XBASE") ) {

/*                if(p.equals(FileConverter_MT.XBASERECLEN)) {

                    sb.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +

                            (ebcdic.indexOf(ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx())))

                            +(1))+"~"+ConfigSplit.recordCount.get("ETSXBASE")+"~"+"ETSXBASE");

                }
*/
                if(ConfigSplit.detailConfigMap.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap.get(p).getInType().equals("N")) {
                    String ss = normalize(convertedASCII.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx()));
//                    sb.append("~"+ss);
                    jObjXbase.put(p, ss.replace("\u0000", " "));
                }

                else if(ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP")) {
                    String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                    String q = "";
                    for(int l :ss.toCharArray()) {
                        q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                    }
//                    sb.append("~"+Integer.parseInt(q,16));
                    jObjXbase.put(p, Integer.parseInt(q,16));
                }

                else if (ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP3")) {

                    String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                    String q = "";
                    for (char l : ss.toCharArray()) {
                        q = q + getHexValue((Integer.toHexString((int) l)), 2);
                    }
                    int dec = 0;
                    int num = 0;
                    if (ConfigSplit.detailConfigMap.get(p).getOutType().length() >= 4) {
                        String[] s1 = ConfigSplit.detailConfigMap.get(p).getOutType().split("V");
                        dec = Integer.parseInt(s1[1]);
                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                    }else{
                        num = Integer.parseInt(ConfigSplit.detailConfigMap.get(p).getOutType().substring(1));
                    }
                    String unpackedData = unpackData(q, dec, num);
//                    sb.append("~" + unpackedData);
                    jObjXbase.put(p, unpackedData);
                    if (p.equals("XBASE-HDR-CL")) {
                        clientID = unpackedData;
                    }

                }

            }

        }

        //return printOutput;

        return;

    }

    private void loadTridXBASERecoed(String convertedASCII, String ebcdic , String recordType, JSONObject jObjXbase){

        // TODO Auto-generated method stub

        boolean flag = false;

        for(String p : ConfigSplit.detailConfigMap.keySet()) {
            try {
                if ((p.contains("XBASE") || (p.contains(recordType)) && !flag)) {

                    flag = true;

                }

                if ((!p.contains("XBASE") && (!p.contains(recordType)) && !flag)) {

//    		if(!p.contains("XBASE") && flag) {

                    break;

                }

//    		if(p.contains("XBASE") ) {
                if (p.contains("XBASE")) {

                    if (ConfigSplit.detailConfigMap.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap.get(p).getInType().equals("N")) {
                        String ss = normalize(convertedASCII.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1, ConfigSplit.detailConfigMap.get(p).getEndIdx()));
                        jObjXbase.put(p, ss.replace("\u0000", " "));
                    } else if (ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP")) {
                        String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1, ConfigSplit.detailConfigMap.get(p).getEndIdx());
                        String q = "";
                        for (int l : ss.toCharArray()) {
                            q += Integer.toHexString(l).length() == 1 ? "0" + Integer.toHexString(l) : Integer.toHexString(l);
                        }
                        jObjXbase.put(p, Integer.parseInt(q, 16));
                    } else if (ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP3")) {

                        String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1, ConfigSplit.detailConfigMap.get(p).getEndIdx());
                        String q = "";
                        for (char l : ss.toCharArray()) {
                            q = q + getHexValue((Integer.toHexString((int) l)), 2);
                        }
                        int dec = 0;
                        int num = 0;
                        if (ConfigSplit.detailConfigMap.get(p).getOutType().length() >= 4) {
                            String[] s1 = ConfigSplit.detailConfigMap.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        } else {
                            num = Integer.parseInt(ConfigSplit.detailConfigMap.get(p).getOutType().substring(1));
                        }
                        String unpackedData = unpackData(q, dec, num);
                        jObjXbase.put(p, unpackedData);
                    }
                }
            }catch (Exception e){
                e.printStackTrace();
            }

        }

//return printOutput;
        return;

    }
    private void load2720XBASERecord(String convertedASCII, String ebcdic, int record, int index,
                                     StringBuilder sb, long lengthOfChar , String recordType, JSONObject jObjXbase) {

        // TODO Auto-generated method stub

        boolean flag = false;

        for(String p : ConfigSplit.detailConfigMap.keySet()) {

            if ((p.contains("XBASE") || (p.contains("27200")) && !flag)) {

                flag = true;

            }

            if ((!p.contains("XBASE") && (!p.contains("27200")) && !flag)) {

//    		if(!p.contains("XBASE") && flag) {

                break;

            }

//    		if(p.contains("XBASE") ) {
            if(p.contains("XBASE") || p.contains("27200")) {

                if(ConfigSplit.detailConfigMap.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap.get(p).getInType().equals("N")) {
                    String ss = normalize(convertedASCII.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx()));
                    jObjXbase.put(p, ss.replace("\u0000", " "));
                }

                else if(ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP")) {
                    String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx()-1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                    String q = "";
                    for(int l :ss.toCharArray()) {
                        q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                    }
                    jObjXbase.put(p, Integer.parseInt(q,16));
                }

                else if (ConfigSplit.detailConfigMap.get(p).getInType().equals("COMP3")) {

                    String ss = ebcdic.substring(ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1,ConfigSplit.detailConfigMap.get(p).getEndIdx());
                    String q = "";
                    for (char l : ss.toCharArray()) {
                        q = q + getHexValue((Integer.toHexString((int) l)), 2);
                    }
                    int dec = 0;
                    int num = 0;
                    if (ConfigSplit.detailConfigMap.get(p).getOutType().length() >= 4) {
                        String[] s1 = ConfigSplit.detailConfigMap.get(p).getOutType().split("V");
                        dec = Integer.parseInt(s1[1]);
                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                    }else{
                        num = Integer.parseInt(ConfigSplit.detailConfigMap.get(p).getOutType().substring(1));
                    }
                    String unpackedData = unpackData(q, dec, num);
                    jObjXbase.put(p, unpackedData);
                }

            }

        }

//return printOutput;
        return;

    }



    //////////////////////////////////////////////////////////////////////

    private void load0530Record(String convertedASCII, String ebcdic, int record, int index,

                                StringBuilder sb, long lengthOfChar, String recordType, JSONObject jObj0530) {

        // TODO Auto-generated method stub

        for(String p : ConfigSplit.detailConfigMap0530.keySet()) {

/*            if(p.matches("MFRSADV-.*SEGLEN")) {
                sb.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                        (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap0530.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap0530.get(p).getEndIdx())))
                        +(1))+"~"+ConfigSplit.recordCount.get("ETS05300")+"~"+"ETS0530");
            }
*/
            if(ConfigSplit.detailConfigMap0530.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap0530.get(p).getInType().equals("N")) {
                String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap0530.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap0530.get(p).getEndIdx()));
//                sb.append("~"+ss);
                jObj0530.put(p, ss.replace("\u0000", " "));
            }

            else if(ConfigSplit.detailConfigMap0530.get(p).getInType().equals("COMP")) {
                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap0530.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap0530.get(p).getEndIdx());
                String q = "";
                for(int l :ss.toCharArray()) {
                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                }
//                sb.append("~"+Integer.parseInt(q,16));
                jObj0530.put(p, Integer.parseInt(q,16));
            }

            else if (ConfigSplit.detailConfigMap0530.get(p).getInType().equals("COMP3")) {
                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap0530.get(p).getStartIdx() - 1,32+ConfigSplit.detailConfigMap0530.get(p).getEndIdx());
                String q = "";
                for (char l : ss.toCharArray()) {
                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                }
                int dec = 0;
                int num = 0;
                if (ConfigSplit.detailConfigMap0530.get(p).getOutType().length() >= 4) {
                    String[] s1 = ConfigSplit.detailConfigMap0530.get(p).getOutType().split("V");
                    dec = Integer.parseInt(s1[1]);
                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                }
                String unpackedData = unpackData(q, dec, num);
//                sb.append("~" + unpackedData);
                jObj0530.put(p, unpackedData);
            }
        }

        // global.clientArray.add(jObj0530);

        return;

    }



    //////////////////////////////////////////////////////////////////////

    private void load2582Record(String convertedASCII, String ebcdic, int record, int index,

                                StringBuilder sb, long lengthOfChar, String recordType, JSONObject jObj2582) {

        // TODO Auto-generated method stub

        for(String p : ConfigSplit.detailConfigMap2582.keySet()) {

/*        	if(p.matches("MF529-.*SEGLEN")) {
                sb.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                        (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap2582.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2582.get(p).getEndIdx())))
                        +(1))+"~"+ConfigSplit.recordCount.get("ETS25820")+"~"+"ETS2582");
            }
*/
            if(ConfigSplit.detailConfigMap2582.get(p).getInType().equals("X")) {
                String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap2582.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2582.get(p).getEndIdx()));
//                sb.append("~"+ss);
                jObj2582.put(p, ss.replace("\u0000", " "));
            }

            else if(ConfigSplit.detailConfigMap2582.get(p).getInType().equals("COMP")) {
                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2582.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2582.get(p).getEndIdx());
                String q = "";
                for(int l :ss.toCharArray()) {
                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                }
//                sb.append("~"+Integer.parseInt(q,16));
                jObj2582.put(p, Integer.parseInt(q,16));
            }

            else if (ConfigSplit.detailConfigMap2582.get(p).getInType().equals("COMP3")) {

                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2582.get(p).getStartIdx() - 1,32+ConfigSplit.detailConfigMap2582.get(p).getEndIdx());
                String q = "";
                for (char l : ss.toCharArray()) {
                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                }
                int dec = 0;
                int num = 0;
                if (ConfigSplit.detailConfigMap2582.get(p).getOutType().length() >= 4) {
                    String[] s1 = ConfigSplit.detailConfigMap2582.get(p).getOutType().split("V");
                    dec = Integer.parseInt(s1[1]);
                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                }
                String unpackedData = unpackData(q, dec, num);
//                sb.append("~" + unpackedData);
                jObj2582.put(p, unpackedData);
            }
        }

        return;

    }



    //////////////////////////////////////////////////////////////////////

    private void load3011Record(String convertedASCII, String ebcdic, int record, int index,

                                StringBuilder sb, long lengthOfChar, String recordType, JSONObject jObj3011) {

        // TODO Auto-generated method stub

        for(String p : ConfigSplit.detailConfigMap3011.keySet()) {

/*        	if(p.matches("30110-.*SEGLEN")) {
                sb.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                        (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap3011.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3011.get(p).getEndIdx())))
                        +(1))+"~"+ConfigSplit.recordCount.get("ETS30110")+"~"+"ETS3011");
            }
*/
            if(ConfigSplit.detailConfigMap3011.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3011.get(p).getInType().equals("N")) {

                String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap3011.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3011.get(p).getEndIdx()));
//                sb.append("~"+ss);
                jObj3011.put(p, ss.replace("\u0000" , " "));
            }

            else if(ConfigSplit.detailConfigMap3011.get(p).getInType().equals("COMP")) {

                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3011.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3011.get(p).getEndIdx());
                String q = "";
                for(int l :ss.toCharArray()) {
                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                }
//                sb.append("~"+Integer.parseInt(q,16));
                jObj3011.put(p, Integer.parseInt(q,16));
            }

            else if (ConfigSplit.detailConfigMap3011.get(p).getInType().equals("COMP3")) {

                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3011.get(p).getStartIdx() - 1,32+ConfigSplit.detailConfigMap3011.get(p).getEndIdx());
                String q = "";
                for (char l : ss.toCharArray()) {
                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                }
                int dec = 0;
                int num = 0;
                if (ConfigSplit.detailConfigMap3011.get(p).getOutType().length() >= 4) {
                    String[] s1 = ConfigSplit.detailConfigMap3011.get(p).getOutType().split("V");
                    dec = Integer.parseInt(s1[1]);
                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                }
                String unpackedData = unpackData(q, dec, num);
//                sb.append("~" + unpackedData);
                jObj3011.put(p, unpackedData);
            }
        }

        return;

    }



    //////////////////////////////////////////////////////////////////////

    private void load3020Record(String convertedASCII, String ebcdic, int record, int index,

                                StringBuilder sb, long lengthOfChar, String recordType, JSONObject jObj3020) {

        // TODO Auto-generated method stub

        for(String p : ConfigSplit.detailConfigMap3020.keySet()) {

/*        	if(p.matches("30200-.*SEGLEN")) {
                sb.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                        (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap3020.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3020.get(p).getEndIdx())))
                        +(1))+"~"+ConfigSplit.recordCount.get("ETS30200")+"~"+"ETS3020");
            }
*/
            if(ConfigSplit.detailConfigMap3011.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3011.get(p).getInType().equals("N")) {
                String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap3020.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3020.get(p).getEndIdx()));
//                sb.append("~"+ss);
                jObj3020.put(p, ss.replace("\u0000", " "));
            }

            else if(ConfigSplit.detailConfigMap3020.get(p).getInType().equals("COMP")) {
                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3020.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3020.get(p).getEndIdx());
                String q = "";
                for(int l :ss.toCharArray()) {
                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                }
//                sb.append("~"+Integer.parseInt(q,16));
                jObj3020.put(p, Integer.parseInt(q,16));
            }

            else if (ConfigSplit.detailConfigMap3020.get(p).getInType().equals("COMP3")) {

                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3020.get(p).getStartIdx() - 1,32+ConfigSplit.detailConfigMap3020.get(p).getEndIdx());
                String q = "";
                for (char l : ss.toCharArray()) {
                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                }
                int dec = 0;
                int num = 0;
                if (ConfigSplit.detailConfigMap3020.get(p).getOutType().length() >= 4) {
                    String[] s1 = ConfigSplit.detailConfigMap3020.get(p).getOutType().split("V");
                    dec = Integer.parseInt(s1[1]);
                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                }
                String unpackedData = unpackData(q, dec, num);
//                sb.append("~" + unpackedData);
                jObj3020.put(p, unpackedData);
            }
        }

        return;

    }

    //////////////////////////////////////////////////////////////////////

    private void loadCLDTRecord(String convertedASCII, String strEBCDIC2, int record2, int index2,

                                StringBuilder sbOutput2, long lengthOfChar2, String recordType, JSONObject jObjCLDT) {
        // TODO Auto-generated method stub

        for(String p : ConfigSplit.detailConfigMapcldt.keySet()) {
            if (p.contains("FILLER")) {
                continue;
            }
            try {
                if (p.equals("CLDT0-CDL-SEGLEN")) {

                    sbOutput2.append("REC" + (index) + "~" + ((record + lengthOfChar) - (record) +

                            (strEBCDIC.indexOf(strEBCDIC.substring(32 + ConfigSplit.detailConfigMapcldt.get(p).getStartIdx() - 1, 32 + ConfigSplit.detailConfigMapcldt.get(p).getEndIdx())))

                            + (1)) + "~" + ConfigSplit.recordCount.get("ETSCLDT") + "~" + "ETSCLDT");

                }

                if (ConfigSplit.detailConfigMapcldt.get(p).getInType().equals("X") || ConfigSplit.detailConfigMapcldt.get(p).getInType().equals("N")) {

                    String ss = normalize(convertedASCII.substring(32 + ConfigSplit.detailConfigMapcldt.get(p).getStartIdx() - 1, 32 + ConfigSplit.detailConfigMapcldt.get(p).getEndIdx()));

//                sbOutput2.append("~"+ss);

                    jObjCLDT.put(p, ss.replace("\u0000", " "));

                } else if (ConfigSplit.detailConfigMapcldt.get(p).getInType().equals("COMP")) {

                    String ss = strEBCDIC.substring(32 + ConfigSplit.detailConfigMapcldt.get(p).getStartIdx() - 1, 32 + ConfigSplit.detailConfigMapcldt.get(p).getEndIdx());

                    String q = "";

                    for (int l : ss.toCharArray()) {
                        q += Integer.toHexString(l).length() == 1 ? "0" + Integer.toHexString(l) : Integer.toHexString(l);
                    }

//                sbOutput2.append("~"+Integer.parseInt(q,16));

                    jObjCLDT.put(p, Integer.parseInt(q, 16));

                }
            } catch (Exception e) {
                // Log the exception with details for debugging
                logger.error("Error processing key: " + p + ", error: " + e.getMessage());
                //     System.exit(400);
            }

        }

//        RR.strASCII = sbOutput2.toString();

        RR.jObjCLDT = jObjCLDT;
    }

    // To hold the occrs data
    class OccurrenceDTO {
        JSONArray jsonArray;

        public OccurrenceDTO(JSONArray jsonArray) {
            this.jsonArray = jsonArray;
        }

        public JSONArray getJsonArray() {
            return jsonArray;
        }

        public void setJsonArray(JSONArray jsonArray) {
            this.jsonArray = jsonArray;
        }
    }

    private void loadTrid(String convertedASCII, String ebcdic,String recordType, JSONObject jObj,Map<String,InputCFG> config){
        Map<String, OccurrenceDTO> occuranceHolders = new HashMap();
        int lengthToBeSubtracted = 0;
        for(String p : config.keySet()) {
            InputCFG fieldConfig = ConfigSplit.detailConfigMap.get(p);
            if (fieldConfig.isOccurs()) {
                try {
                    if(p.contains("FILLER")){
                        continue;
                    }
                    String rootObjectKey = p.substring(0, p.lastIndexOf("-"));
                    int currentOccurs = Integer.parseInt(p.substring(fieldConfig.fieldName.length()));
                    OccurrenceDTO occuranceDetail = occuranceHolders.getOrDefault(rootObjectKey, null);
                    if (occuranceDetail != null && !(jObj.get(rootObjectKey + "-NO") == null ||
                            Integer.parseInt((String) jObj.get(rootObjectKey + "-NO")) >= currentOccurs)) {
                        lengthToBeSubtracted = lengthToBeSubtracted + fieldConfig.getFieldLength();
                        continue;
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
            }
            Object result = null;
            if(p.contains(recordType))
                try {
                    if (config.get(p).getInType().equals("X") || config.get(p).getInType().equals("N")) {
                        String ss = normalize(convertedASCII.substring(32 + config.get(p).getStartIdx() - 1, 32 + config.get(p).getEndIdx()));
                        int dec = 0;
                        int num = 0;
                        if (config.get(p).getOutType().contains("V")) {
                            if (config.get(p).getOutType().length() >= 4) {
                                String[] s1 = config.get(p).getOutType().split("V");
                                dec = Integer.parseInt(s1[1]);
                                num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                                String ss_temp_1 = ss.substring(0, num);
                                String ss_temp_2 = ss.substring(num);
                                String ss_temp_3 = ss_temp_1 + "." + ss_temp_2;
                                result = ss_temp_3.replace("\u0000", " ");

                            }
                        } else {
                            result = ss.replace("\u0000", " ");
                        }
                    } else if (config.get(p).getInType().equals("COMP")) {
                        String ss = ebcdic.substring(32 + config.get(p).getStartIdx() - 1, 32 + config.get(p).getEndIdx());
                        String q = "";
                        for (int l : ss.toCharArray()) {
                            q += Integer.toHexString(l).length() == 1 ? "0" + Integer.toHexString(l) : Integer.toHexString(l);
                        }
                        result =  Integer.parseInt(q, 16);

                    } else if (config.get(p).getInType().equals("COMP3")) {

                        String ss = ebcdic.substring(32 + config.get(p).getStartIdx() - 1, 32 + config.get(p).getEndIdx());
                        String q = "";
                        for (char l : ss.toCharArray()) {
                            q = q + getHexValue((Integer.toHexString((int) l)), 2);
                        }
                        int dec = 0;
                        int num = 0;
                        if (config.get(p).getOutType().length() >= 4) {
                            String[] s1 = config.get(p).getOutType().split("V");
                            dec = Integer.parseInt(s1[1]);
                            num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                        }
                        String unpackedData = unpackData(q, dec, num);
                        result = unpackedData;
                    }
                    if (fieldConfig.isOccurs()) {
                        String rootObjectKey = p.substring(0, p.lastIndexOf("-"));
                        OccurrenceDTO occuranceHolder = occuranceHolders.getOrDefault(rootObjectKey, null);
                        if (occuranceHolder == null) {
                            int inputOccurs = -1;
                            if (jObj.get(rootObjectKey + "-NO") != null) {
                                Object inputOccursObject = jObj.get(rootObjectKey + "-NO");
                                if (inputOccursObject instanceof String)
                                    inputOccurs = Integer.parseInt(inputOccursObject.toString());
                                else inputOccurs = (Integer) inputOccursObject;
                            }
                            JSONObject jsonObject = new JSONObject();
                            jsonObject.put(fieldConfig.getFieldName(), result);
                            JSONArray jsonArray = new JSONArray();
                            jsonArray.add(jsonObject);
                            occuranceHolder = new OccurrenceDTO(jsonArray);
                            occuranceHolders.put(rootObjectKey, occuranceHolder);
                        } else {
                            int currentOccurs = Integer.parseInt(p.substring(fieldConfig.fieldName.length()));
                            JSONObject jsonObject = new JSONObject();
                            if (currentOccurs <= occuranceHolder.getJsonArray().size()) {
                                jsonObject = (JSONObject) occuranceHolder.getJsonArray().get(currentOccurs - 1);
                                jsonObject.put(fieldConfig.getFieldName().replace("-"+recordType,""), result);
                            } else {
                                jsonObject.put(fieldConfig.getFieldName().replace("-"+recordType,""), result);
                                occuranceHolder.getJsonArray().add(jsonObject);
                            }
                        }
                    } else {
                        jObj.put(p, result);
                    }
                    for (Map.Entry<String, OccurrenceDTO> entry : occuranceHolders.entrySet()) {
                        jObj.put(entry.getKey(), entry.getValue().getJsonArray());
                    }
                }catch (Exception e){
                    e.printStackTrace();
                }
        }

    }


    //////////////////////////////////////////////////////////////////////
    private void load2720Record(String convertedASCII, String ebcdic, int record, int index,

                                StringBuilder sb, long lengthOfChar, String recordType, JSONObject jObj2720) {

// TODO Auto-generated method stub

        for(String p : ConfigSplit.detailConfigMap2720.keySet()) {

//        	System.out.println("**********  INSIDE THE 2720  *********");


            if(ConfigSplit.detailConfigMap2720.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap2720.get(p).getInType().equals("N")) {
                String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap2720.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2720.get(p).getEndIdx()));

                int dec = 0;
                int num = 0;
                if (ConfigSplit.detailConfigMap2720.get(p).getOutType().contains("V")) {
                    if (ConfigSplit.detailConfigMap2720.get(p).getOutType().length() >= 4) {
                        String[] s1 = ConfigSplit.detailConfigMap2720.get(p).getOutType().split("V");
                        dec = Integer.parseInt(s1[1]);
                        num = Integer.parseInt(s1[0].substring(1, s1[0].length()));

                        String ss_temp_1 = ss.substring(0, num);
                        String ss_temp_2 = ss.substring(num);
                        String ss_temp_3 = ss_temp_1 + "." + ss_temp_2;

//        				System.out.println("2720 - ss ->" + ss);
//        				System.out.println("2720 - ss_temp_1 ->" + ss_temp_1);
//        				System.out.println("2720 - ss_temp_2 ->" + ss_temp_2);
//        				System.out.println("2720 - ss_temp_3 ->" + ss_temp_3);


                        jObj2720.put(p, ss_temp_3.replace("\u0000", " "));

                    }
                } else {
                    jObj2720.put(p, ss.replace("\u0000", " "));
                }
            }

            else if(ConfigSplit.detailConfigMap2720.get(p).getInType().equals("COMP")) {
                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2720.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap2720.get(p).getEndIdx());
                String q = "";
                for(int l :ss.toCharArray()) {
                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                }
                jObj2720.put(p, Integer.parseInt(q,16));

                Integer temp = Integer.parseInt(q,16);

/*    			if( p.equals("27200-CLT-PLG-SEGLEN") && (temp.toString().equals("136"))) {
    				System.out.println("p ->" + p);
    				System.out.println("temp ->" + temp.toString());
    			} else if ( p.equals("27200-CLT-PLG-SEGLEN")) {
    				System.out.println("other - p ->" + p);
    				System.out.println("temp ->" + temp.toString());
    			}
*/

//    			System.out.println("p ->" + p);
//    			System.out.println("q ->" + Integer.parseInt(q,16));

            }

            else if (ConfigSplit.detailConfigMap2720.get(p).getInType().equals("COMP3")) {

                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap2720.get(p).getStartIdx() - 1,32+ConfigSplit.detailConfigMap2720.get(p).getEndIdx());
                String q = "";
                for (char l : ss.toCharArray()) {
                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                }
                int dec = 0;
                int num = 0;
                if (ConfigSplit.detailConfigMap2720.get(p).getOutType().length() >= 4) {
                    String[] s1 = ConfigSplit.detailConfigMap2720.get(p).getOutType().split("V");
                    dec = Integer.parseInt(s1[1]);
                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                }
                String unpackedData = unpackData(q, dec, num);
                jObj2720.put(p, unpackedData);

//    			System.out.println("p ->" + p);
//    			System.out.println("unpackedData ->" + unpackedData);

            }
        }

        return;

    }


    private void load3050Record(String convertedASCII, String ebcdic, int record, int index,

                                StringBuilder sb, long lengthOfChar, String recordType, JSONObject jObj3050) {

        // TODO Auto-generated method stub

        for(String p : ConfigSplit.detailConfigMap3050.keySet()) {

/*        	if(p.matches("30500-.*SEGLEN")) {
                sb.append("REC"+(index)+"~"+((record+lengthOfChar) - (record) +
                        (ebcdic.indexOf(ebcdic.substring(32+ConfigSplit.detailConfigMap3050.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3050.get(p).getEndIdx())))
                        +(1))+"~"+ConfigSplit.recordCount.get("ETS30500")+"~"+"ETS3050");
            }
*/
            if(ConfigSplit.detailConfigMap3050.get(p).getInType().equals("X") || ConfigSplit.detailConfigMap3050.get(p).getInType().equals("N")) {
                String ss = normalize(convertedASCII.substring(32+ConfigSplit.detailConfigMap3050.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3050.get(p).getEndIdx()));
//                sb.append("~"+ss);
                jObj3050.put(p, ss.replace("\u0000", " "));
            }

            else if(ConfigSplit.detailConfigMap3050.get(p).getInType().equals("COMP")) {
                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3050.get(p).getStartIdx()-1,32+ConfigSplit.detailConfigMap3050.get(p).getEndIdx());
                String q = "";
                for(int l :ss.toCharArray()) {
                    q +=Integer.toHexString(l).length() == 1 ? "0"+Integer.toHexString(l) : Integer.toHexString(l);
                }
//                sb.append("~"+Integer.parseInt(q,16));
                jObj3050.put(p, Integer.parseInt(q,16));
            }

            else if (ConfigSplit.detailConfigMap3050.get(p).getInType().equals("COMP3")) {

                String ss = ebcdic.substring(32+ConfigSplit.detailConfigMap3050.get(p).getStartIdx() - 1,32+ConfigSplit.detailConfigMap3050.get(p).getEndIdx());
                String q = "";
                for (char l : ss.toCharArray()) {
                    q = q + getHexValue((Integer.toHexString((int) l)), 2);
                }
                int dec = 0;
                int num = 0;
                if (ConfigSplit.detailConfigMap3050.get(p).getOutType().length() >= 4) {
                    String[] s1 = ConfigSplit.detailConfigMap3050.get(p).getOutType().split("V");
                    dec = Integer.parseInt(s1[1]);
                    num = Integer.parseInt(s1[0].substring(1, s1[0].length()));
                }
                String unpackedData = unpackData(q, dec, num);
//                sb.append("~" + unpackedData);
                jObj3050.put(p, unpackedData);
            }
        }

        return;

    }

    // Initialize writers once
    private static synchronized void initializeWriters() {
        if (initialized) return;

        try {
            new File(tempDir).mkdirs();

            // Create writers for each record type
            recordWriters.put("30100", new BufferedWriter(new FileWriter(tempDir + "30100.txt")));
            recordWriters.put("50300", new BufferedWriter(new FileWriter(tempDir + "50300.txt")));
            recordWriters.put("27200", new BufferedWriter(new FileWriter(tempDir + "27200.txt")));
            System.out.printf("Writers initialized in directory: %s%n", tempDir);

            // Initialize counters
            recordCounters.put("30100", new AtomicInteger(0));
            recordCounters.put("50300", new AtomicInteger(0));
            recordCounters.put("27200", new AtomicInteger(0));

            initialized = true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to initialize record writers", e);
        }
    }

    /*public static void addRecord30100(JSONObject jObj3010) {
        initializeWriters();
        try {
            String line = formatDelimitedLine(jObj3010, "30100");
            if (line != null && !line.isEmpty()) {
                // Ensure no existing line breaks in the formatted string
                line = line.replaceAll("\r\n|\n|\r", "");

                recordWriters.get("30100").write(line);
                recordWriters.get("30100").newLine();
                recordWriters.get("30100").flush();
                recordCounters.get("30100").incrementAndGet();
             //   System.out.println("line data: " + line);
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write 30100 record", e);
        }
    }*/

    /*public static void addRecord30100(JSONObject jObj3010) {
        initializeWriters();
        try {
            String line = formatDelimitedLine(jObj3010, "30100");
            if (line != null && !line.isEmpty()) {
                // Ensure no existing line breaks in the formatted string
                line = line.replaceAll("\r\n|\n|\r", "");

                BufferedWriter writer = recordWriters.get("30100");

                // Ensure we're at the beginning of a new line
             //   writer.write(System.lineSeparator());
                writer.write(line);
               // writer.flush();
                writer.newLine();
                writer.flush();
                recordCounters.get("30100").incrementAndGet();
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to write 30100 record", e);
        }
    }*/

    public static void addRecord30100(JSONObject jObj3010) {
        initializeWriters();
        BufferedWriter writer = recordWriters.get("30100");
        if (writer == null) return;
        synchronized (writer) {
            try {
                String line = formatDelimitedLine(jObj3010, "30100");
                if (line == null) return;
                line = line.replaceAll("\r\n|\n|\r", "").trim();
                if (line.isEmpty()) return;
                writer.write(line);
                writer.newLine();
                writer.flush();
                recordCounters.get("30100").incrementAndGet();
            } catch (IOException e) {
                throw new RuntimeException("Failed to write 30100 record", e);
            }
        }
    }

    public static void addRecord50300(JSONObject jObj5030) {
        initializeWriters();
        BufferedWriter writer = recordWriters.get("50300");
        if (writer == null) return;
        synchronized (writer) {
            try {
                String line = formatDelimitedLine(jObj5030, "50300");
                if (line == null) return;
                line = line.replaceAll("\r\n|\n|\r", "").trim();
                if (line.isEmpty()) return;
                writer.write(line);
                writer.newLine();
                writer.flush();
                recordCounters.get("50300").incrementAndGet();
            } catch (IOException e) {
                throw new RuntimeException("Failed to write 50300 record", e);
            }
        }
    }

    public static void addRecord27200(JSONObject jObj2720) {
        initializeWriters();
        BufferedWriter writer = recordWriters.get("27200");
        if (writer == null) return;
        synchronized (writer) {
            try {
                String line = formatDelimitedLine(jObj2720, "27200");
                if (line == null) return;
                line = line.replaceAll("\r\n|\n|\r", "").trim();
                if (line.isEmpty()) return;
                writer.write(line);
                writer.newLine();
                writer.flush();
                recordCounters.get("27200").incrementAndGet();
            } catch (IOException e) {
                throw new RuntimeException("Failed to write 27200 record", e);
            }
        }
    }


    // Method to close all writers and get record counts
    /*public static void closeWriters() {
        for (BufferedWriter writer : recordWriters.values()) {
            try {
                if(writer!= null) {
                    writer.flush();
                    writer.close();
                }
            } catch (IOException e) {
                // Log error
                System.err.println("Error closing writer: " + e.getMessage());
            }
        }
    }*/

    public static synchronized void closeWriters() {
        if (recordWriters.isEmpty()) return; // already closed
        for (Map.Entry<String, BufferedWriter> entry : recordWriters.entrySet()) {
            BufferedWriter writer = entry.getValue();
            if (writer == null) continue;
            try {
                writer.flush();
            } catch (IOException ignored) {
            }
            try {
                writer.close();
            } catch (IOException e) {
                if (!"Stream closed".equals(e.getMessage())) {
                    System.err.println("Error closing writer: " + e.getMessage());
                }
            }
        }
        recordWriters.clear(); // prevent future close attempts
    }

    // Method to get record count
    public static int getRecordCount(String recordType) {
        AtomicInteger counter = recordCounters.get(recordType);
        return counter != null ? counter.get() : 0;
    }

    private static  String formatDelimitedLine(JSONObject obj, String recordType) {
        if (obj == null || obj.isEmpty()) return null;

        // Format according to record type (this logic should match your existing formatting)
        if ("50300".equals(recordType)) {
            String keyValue = Main.KeyFields50300.stream()
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("|"));

            String delimitedFields = Main.fields50300.stream()
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("||"));

            //   String result=keyValue + "||" + delimitedFields;
            //  System.out.println("Formatted 50300 record: " + result);
            return keyValue + "||" + delimitedFields;
        }

       /* if ("30100".equals(recordType)) {
            // Create a unique key from the key fields
            String keyValue = Main.KeyFields30100.stream()
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("|"));

            // Check if this record was already processed
            Set<String> keysForType = processedKeys.computeIfAbsent("30100", k -> new HashSet<>());
            if (keysForType.contains(keyValue)) {
                // Skip duplicate record
                return null;
            }

            // Add to processed keys set
            keysForType.add(keyValue);

            // Continue with normal processing for non-duplicates
            String delimitedFields = Main.fields30100.stream()
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("||"));

            return keyValue + "||" + delimitedFields;
        }*/

        if ("30100".equals(recordType)) {
            // Creates a unique key from the key fields
            String keyValue = Main.KeyFields30100.stream()
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("|"));

            // Skip empty records
            if (keyValue.trim().isEmpty()) {
                return null;
            }

            // Normalize the key for consistent comparison
            String normalizedKey = keyValue.trim().toLowerCase();
            synchronized (processedKeys) {
                // Check if this record was already processed
                Set<String> keysForType = processedKeys.computeIfAbsent("30100", k -> new HashSet<>());
                if (keysForType.contains(normalizedKey)) {
                    //   System.out.println("Skipping duplicate 30100 record with key: " + keyValue);
                    // Skip duplicate record
                    return null;
                }

                // Add to processed keys set
                keysForType.add(normalizedKey);
                //   System.out.println("Added new 30100 record key: " + keyValue);
            }

            // Continue with normal processing for non-duplicates
            String delimitedFields = Main.fields30100.stream()
                    .filter(key -> obj.get(key) != null) // Filter out null values
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("||"));

            //   String result=keyValue + "||" + delimitedFields;
            //    System.out.println("Formatted 30100 record: " + result);

            //  return delimitedFields.isEmpty() ? null : keyValue + "||" + delimitedFields;
            return keyValue + "||" + delimitedFields;
        }

        if ("27200".equals(recordType)) {
            String keyValue = Main.KeyFields27200.stream()
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("|"));

            String delimitedFields = Main.fields27200.stream()
                    .map(key -> cleanValue(obj.get(key)))
                    .collect(Collectors.joining("||"));

            return keyValue + "||" + delimitedFields;
        }


        return null;
    }

    private static String cleanValue(Object value) {
        if (value == null) return " ";
        String val = value.toString().trim();
        return val.isEmpty() ? " " : val;
    }
//code block to filter the lookup the table of 27200/30100/50300
    private boolean shouldEmitAccountRecord(JSONObject payload) {
        if (!accountFilterService.isEnabled() || payload == null) {
            return true;
        }
        String clientId = extractField(payload, "XBASE-HDR-CL");
        String accountNumber = extractAccountNumber(payload);
        if (accountNumber.isEmpty() || clientId.isEmpty()) {
            return true;
        }
        return accountFilterService.shouldEmit(accountNumber, clientId);
    }

    private String extractAccountNumber(JSONObject payload) {
        String branch = extractField(payload, "XBASE-HDR-BR");
        String acct = extractField(payload, "XBASE-HDR-ACT");
        return (branch + acct).replace(" ", "");
    }

    private String extractField(JSONObject obj, String key) {
        Object value = obj.get(key);
        return value == null ? "" : value.toString().replace("\u0000", " ").trim();
    }



    public static Queue<JSONObject> getRecord50300() {
        return records50300;
    }

    public static Queue<JSONObject> getRecord27200() {
        return records27200;
    }
    public static Queue<JSONObject> getRecords30100() {
        return records30100;
    }
    public static String normalize(String text) {
        if (text == null) {
            return null;
        }

        StringBuilder sb = new StringBuilder(text.length());

        for (int i = 0; i < text.length(); i++) {
            char ch = text.charAt(i);

            switch (ch) {
                case '\u0080': sb.append('\u20AC'); break; // €
                case '\u0082': sb.append('\u201A'); break; // ‚
                case '\u0083': sb.append('\u0192'); break; // ƒ
                case '\u0084': sb.append('\u201E'); break; // „
                case '\u0085': sb.append('\u2026'); break; // …
                case '\u0086': sb.append('\u2020'); break; // †
                case '\u0087': sb.append('\u2021'); break; // ‡
                case '\u0088': sb.append('\u02C6'); break; // ˆ
                case '\u0089': sb.append('\u2030'); break; // ‰
                case '\u008A': sb.append('\u0160'); break; // Š
                case '\u008B': sb.append('\u2039'); break; // ‹
                case '\u008C': sb.append('\u0152'); break; // Œ
                case '\u008E': sb.append('\u017D'); break; // Ž

                case '\u0091': sb.append('\u2018'); break; // ‘
                case '\u0092': sb.append('\u2019'); break; // ’
                case '\u0093': sb.append('\u201C'); break; // “
                case '\u0094': sb.append('\u201D'); break; // ”
                case '\u0095': sb.append('\u2022'); break; // •
                case '\u0096': sb.append('\u2013'); break; // –
                case '\u0097': sb.append('\u2014'); break; // —
                case '\u0098': sb.append('\u02DC'); break; // ˜
                case '\u0099': sb.append('\u2122'); break; // ™
                case '\u009A': sb.append('\u0161'); break; // š
                case '\u009B': sb.append('\u203A'); break; // ›
                case '\u009C': sb.append('\u0153'); break; // œ
                case '\u009E': sb.append('\u017E'); break; // ž
                case '\u009F': sb.append('\u0178'); break; // Ÿ

                default:
                    sb.append(ch);
            }
        }

        return sb.toString();
    }
    /*public static Queue<JSONObject> getRecords11200() {
        return records11200;
    }*/
}

