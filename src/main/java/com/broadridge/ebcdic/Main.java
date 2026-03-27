package com.broadridge.ebcdic;

import java.io.BufferedReader;

import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class Main {



    private static final Logger logger = LogManager.getLogger(Main.class);
    /**
     * Command Line Parameters:
     * -if <input EBCDIC file>                    (mandatory)
     * -cfg <input config file>                   (mandatory)
     * -b1cfg <input b1 setting config file>      (mandatory)
     * -STrid <seperate TRID Data input file>     (mandatory)
     * -of <output JSON/ASCII file>               (mandatory)
     * -tof <output trailer file>                 (mandatory)
     * -supp <supperssion output AccountFile >                                                (optional)
     * -off <output file format; supported values # JSON and ASCII >         (default=JSON)   (optional)
     * -numthreads <number of threads> :                                     (default=8)      (optional)
     * -batchsize <batch size for records> :                                 (default=100000) (optional)
     * -showreccount <write msg to console every showreccount records processed> :
     *                        (if 0 [default], no output written)                             (optional)
     * -bucketName <AWS Bucket Name> :       (default=br-icsdev-dpm-dataingress-us-east-1-s3) (optional)
     * -outputQueueFolder <AWS output queue folder name> :             (default=wells/output) (optional)
     * -headerQueueFolder <AWS header queue folder name> : (default=wells/wip/to_brcc_header) (optional)
     * -headerQueueFolder <AWS queue folder name> :               (default=wells/wip/to_brcc) (optional)
     */

    protected static ArrayList<String> separateTridDataList = new ArrayList<String>();

    protected static ArrayList<String> holdTridDataList = new ArrayList<String>();
    protected static ArrayList<String> inputFolderTridDataList = new ArrayList<String>();
    protected static ArrayList<String> combinedTridDataList = new ArrayList<String>();
    protected static ArrayList<String> fields50300 = new ArrayList<String>();
    protected static ArrayList<String> fields27200 = new ArrayList<String>();
    protected static ArrayList<String> fields30100 = new ArrayList<String>();
    protected static ArrayList<String> KeyFields27200 = new ArrayList<String>();
    protected static ArrayList<String> KeyFields50300 = new ArrayList<String>();
    protected static ArrayList<String> KeyFields30100 = new ArrayList<String>();



    public static ArrayList<String> getSeparateTridDataList() {
        return separateTridDataList;
    }
    public static String awsAccountDetail =Globals.awsAccountDetail;
    // Specify the region
    public static Region region = Globals.region;

    public static void main(String[] args) {
        // Check current heap size





        Date startDate = new Date();
        // For Testing Command-Line Inputs
/*		String inputdir = "src/main/resources/input/";
		String outputdir = "src/main/resources/output/";

		String[] argsv = {
				"-if",                "C:\\Users\\rajnishk\\Downloads\\BIOS.C025.OUT.B228.ORG.12312025022701",
				"-cfg",               inputdir + "b228_config.txt",
				"-b1cfg",             inputdir + "b1_settings.txt",
				"-STrid",             inputdir + "trid_list_qa.txt",
				"-of",                outputdir + "BIOS.C025.OUT.B228.QA.20250407000004.json",
				"-tof",               outputdir + "trailer.json",
				"-supp",              outputdir + "suppress.txt",
				"-off",               "JSON",
				"-numthreads",        "16",
				"-batchsize",         "50000",
				//	"-showreccount",      "0",
				"-bucketName",        "br-icsdev-dpmrajnishk-dataingress-us-east-1-s3",
				"-outputQueueFolder", "src/main/resources/output",
				"-headerQueueFolder", "wells/wip/to_brcc_header/",
				"-holdQueueFolder",   "wells/wip/to_hold/",
				"-queueFolder",       "wells/accountdrivertest/",
				"-fileValidation", "False",
				"-awsAccountDetail","187777304606",
				"-accountFilter", "src/main/resources/input/account_filter_file.txt",
			//	"-s3ThreadCount","50",
				"-s3BatchSize","50",
           //     "-localEnv","true",
                "-printSuppInd","true",
                "-rbcfilterenable","true",
                "-rbcacffile", inputdir + "RBC_PROD_mend_aux_ACF_20251103.dat",
                "-rbcbranchfile", inputdir + "branchfile.txt"

		};

		args = argsv;*/

        try {

//            logger.debug("This is a DEBUG message.");
//            logger.info("This is an INFO message.");
//            logger.warn("This is a WARN message.");
//            logger.error("This is an ERROR message.");
//            logger.fatal("This is a FATAL message.");


            // Specify the region
//        	Region region = Region.US_EAST_1; // For example, "us-east-1"
            // Create the S3 client
            S3Client s3 = S3Client.builder().region(region).build();
            //	System.setProperty("Xmx", "10g");
            //	long heapSize = Runtime.getRuntime().maxMemory();
            //	System.out.println("Current Heap Size: " + (heapSize / (1024 * 1024)) + " MB");
            String inputfile = "";
            String outputfile = "";
            String configfile = "";
            String outputIndexer = "";
            String suppAccountFile = "";
            String trailerOutputFile = "";
            String outputFormat = "JSON";
            String splitJSON = "Y";
            String b1Settings = "";
            String sTrid = "";
            boolean fileValidation = false;
            int numThreads = 1;
            int batchSize = 100000;
            int showRecCount = 0;
            String bucketName = "br-icsdev-dpm-dataingress-us-east-1-s3";
            String outputQueueFolder = "wells/output/";
            String headerQueueFolder = "wip/to_brcc_header";
            String holdQueueFolder = "wip/to_hold";
            String queueFolder = "wip/to_brcc_ETSPETR";
            String headerInputFolder = "src/main/resource/wells/input/";
            int s3BatchSize = 1000;
            String accountFilter="";
            boolean printSuppInd = false;
            //	int s3ThreadCount= 50;
            boolean localEnv=false;
                //		String headerInputFolder = "wells/input/";
                // ACF (B228) filter - flag driven, off by default
                boolean rbcFilterEnable = false;
                String rbcAcfFile = "";
                String rbcBranchFile = "";

            for (int i = 0; i < args.length; i++) {
                if ((args[i].equals("-if"))) {
                    inputfile = args[i + 1];
                    logger.info("Input File is: " + inputfile);
                } else if ((args[i].equals("-of"))) {
                    outputfile = args[i + 1];
                    logger.info("Output File is: " + outputfile);
                } else if ((args[i].equals("-tof"))) {
                    trailerOutputFile = args[i + 1];
                    logger.info("Trailer Output JSON File is: " + trailerOutputFile);
                } else if ((args[i].equals("-cfg"))) {
                    configfile = args[i + 1];
                    logger.info("CFG File is: " + configfile);
                } else if ((args[i].equals("-oix"))) {
                    outputIndexer = args[i + 1];
                    logger.info("Output Indexer File is: " + outputIndexer);
                } else if ((args[i].equals("-supp"))) {
                    suppAccountFile = args[i + 1];
                    logger.info("Suppressed Accounts File is: " + suppAccountFile);
                } else if ((args[i].equals("-numthreads"))) {
                    numThreads = Integer.parseInt(args[i + 1]);
                    logger.info("numThreads : " + numThreads);
                } else if ((args[i].equals("-off"))) {
                    outputFormat = args[i + 1];
                    logger.info("Output File Format is : " + outputFormat);
                } else if ((args[i].equals("-batchsize"))) {
                    batchSize = Integer.parseInt(args[i + 1]);
                    logger.info("batchSize : " + batchSize);
                } else if ((args[i].equals("-showreccount"))) {
                    showRecCount = Integer.parseInt(args[i + 1]);
                    logger.info("showRecCount : " + showRecCount);
                } else if ((args[i].equals("-splitJson"))) {
                    splitJSON = args[i + 1];
                    logger.info("splitJSON : " + splitJSON);
                } else if((args[i].equals("-b1cfg"))) {
                    b1Settings = args[i + 1];
                    logger.info("b1Settings : " + b1Settings);
                } else if((args[i].equals("-bucketName"))) {
                    bucketName = args[i + 1];
                    logger.info("bucketName : " + bucketName);
                } else if((args[i].equals("-outputQueueFolder"))) {
                    outputQueueFolder = args[i + 1];
                    logger.info("outputQueueFolder : " + outputQueueFolder);
                } else if((args[i].equals("-headerQueueFolder"))) {
                    headerQueueFolder = args[i + 1];
                    logger.info("headerQueueFolder : " + headerQueueFolder);
                } else if((args[i].equals("-holdQueueFolder"))) {
                    holdQueueFolder = args[i + 1];
                    logger.info("holdQueueFolder : " + holdQueueFolder);
                } else if((args[i].equals("-queueFolder"))) {
                    queueFolder = args[i + 1];
                    logger.info("folderQueue : " + queueFolder);
                } else if((args[i].equals("-headerInputFolder"))) {
                    headerInputFolder = args[i + 1];
                    logger.info("headerInputFolder : " + headerInputFolder);
                }
                else if((args[i].equals("-STrid"))){
                    sTrid = args[i + 1];
                    logger.info("sTrid : " + sTrid);
                    loadDataFromFile(sTrid);
                } else if ((args[i].equals("-fileValidation"))) {
                    fileValidation = Boolean.parseBoolean(args[i + 1].toLowerCase());

                }else if ((args[i].equals("-localEnv"))) {
                    localEnv = Boolean.parseBoolean(args[i + 1].toLowerCase());
                }
                else if((args[i].equals("-awsAccountDetail"))) {
                    awsAccountDetail = args[i+1];
                    //default is DeveInvironment
                }
                else if((args[i].equals("-accountFilter"))) {
                    accountFilter = args[i + 1];
                    logger.info("accountFile : " + accountFilter);
                }
				/*else if ((args[i].equals("-s3ThreadCount"))) {
					s3ThreadCount = Integer.parseInt(args[i + 1]);
					logger.info("s3ThreadCount : " + s3ThreadCount);
				}*/
                else if((args[i].equals("-s3BatchSize"))) {
                    s3BatchSize = Integer.parseInt(args[i + 1]);
                    logger.info("B228 Bundle Batch size : " + s3BatchSize);
                    //default is DeveInvironment
                }
                else if((args[i].equals("-printSuppInd"))) {
                    printSuppInd = Boolean.parseBoolean(args[i + 1].toLowerCase());
                    logger.info("Currency suppression enabled (printSuppInd) : " + printSuppInd);
                }
                    else if ((args[i].equals("-rbcfilterenable"))) {
                        rbcFilterEnable = Boolean.parseBoolean(args[i + 1].toLowerCase());
                        logger.info("RBC ACF filter enabled (rbcfilterenable) : " + rbcFilterEnable);
                    }
                    else if ((args[i].equals("-rbcacffile"))) {
                        rbcAcfFile = args[i + 1];
                        logger.info("RBC ACF filter file : " + rbcAcfFile);
                    }
                    else if ((args[i].equals("-rbcbranchfile"))) {
                        rbcBranchFile = args[i + 1];
                        logger.info("RBC branch file for ACF filter : " + rbcBranchFile);
                    }
            }
            FileConverter_MT.LOCAL_ENV=localEnv;
            if(outputFormat.trim().equals("JSON")) {
                if (inputfile.trim().length()==0 || outputfile.trim().length()==0
                        || configfile.trim().length()==0 ) {
                    Globals.logMapper.put("Message",  "Error: CommandLine Args - some input files missing");
                    Globals.logMapper.put("Status", "failed");
                    Globals.logMapper.put("Class", "Main.class");
                    Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                    Globals.logMapper.clear();
                    logger.fatal("Error: CommandLine Args - some input files missing");
                    System.exit(1);
                }
            } else {
                if (inputfile.trim().length()==0 || outputfile.trim().length()==0
                        || configfile.trim().length()==0 || outputIndexer.trim().length()==0 || suppAccountFile.trim().length() == 0) {
                    logger.fatal("Error: CommandLine Args - some input files missing");
                    Globals.logMapper.put("Message",  "Error: CommandLine Args - some input files missing");
                    Globals.logMapper.put("Status", "failed");
                    Globals.logMapper.put("Class", "Main.class");
                    Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                    Globals.logMapper.clear();
                    System.exit(1);
                }
            }


            logger.info("Running Multi-Threaded, NumThreads=" + numThreads + ", BatchSize=" + batchSize);
            logger.info("fileValidation -> " + fileValidation);

            Globals.logMapper.put("Message",  "Running Multi-Threaded, NumThreads=" + numThreads + ", BatchSize=" + batchSize);
            Globals.logMapper.put("Status", "success");
            Globals.logMapper.put("Class", "Main.class");
            Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
            Globals.logMapper.clear();


            if (fileValidation) {
                Validation validate = new Validation();
                validate.validate(inputfile, configfile, numThreads, batchSize, outputFormat,b1Settings);

//				validate.addTrailersToOutputFiles(inputfile, s3, bucketName, outputQueueFolder);
                validate.addTrailersToOutputFiles(inputfile, s3, bucketName, trailerOutputFile, outputQueueFolder);


                if ( Validation.tailerTotalFound && Validation.tailerRecordFound && (Validation.B228TrailerRecords.get() == Validation.totalRecords.get())) {
//				if ( validate.tailerTotalFound && validate.tailerRecordFound && (Validation.B228TrailerRecords == Validation.totalRecords-1)) {
/*	The below code is not needed as this will not be in the step function

    				FileConverter_MT fileConvert = new FileConverter_MT();
					fileConvert.convert(inputfile, outputfile, configfile, outputIndexer, numThreads, batchSize, showRecCount,
							suppAccountFile, trailerOutputFile, outputFormat, splitJSON, b1Settings, bucketName, outputQueueFolder,
							headerQueueFolder, holdQueueFolder, queueFolder, headerInputFolder, s3);
*/

                    logger.info("Validation Run Successful.");
                    logger.info("M-B228TrailerRecords : [" + Validation.B228TrailerRecords + "]");
                    logger.info("M-totalRecords : [" + Validation.totalRecords + "]");

                    Globals.logMapper.put("Message",  "Validation Run Successful.\n\"M-B228TrailerRecords : [" + Validation.B228TrailerRecords + "]\n" + "M-totalRecords : [" + Validation.totalRecords + "]");
                    Globals.logMapper.put("Status", "success");
                    Globals.logMapper.put("Class", "Main.class");
                    Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                    Globals.logMapper.clear();


                } else {
                    //need to trailer file===================
                    if(!Validation.tailerRecordFound) {
                        logger.info("Validation Record not found ");
                        Globals.logMapper.put("Message", "Validation Record not found");
                        Globals.logMapper.put("Status", "Failed");
                        Globals.logMapper.put("Class", "Main.class");
                        Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                        Globals.logMapper.clear();


                    }
                    else if (!Validation.tailerTotalFound) {
                        logger.info(
                                "Validation Failed due to TLR-TOTAL not found in record .");
                        Globals.logMapper.put("Message", "Validation Failed due to TLR-TOTAL not found in record .");
                        Globals.logMapper.put("Status", "Failed");
                        Globals.logMapper.put("Class", "Main.class");
                        Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                        Globals.logMapper.clear();

                    }
                    else if (Validation.tailerRecordFound && Validation.tailerTotalFound) {
                        logger.info(
                                "Validation failed (Error Code: 12345): Mismatch between tailler record count and total record count. where"+Validation.B228TrailerRecords+" not equal to"+Validation.totalRecords);
                        Globals.logMapper.put("Message", "Validation failed (Error Code: 12345): Mismatch between tailler record count and total record count. where"+Validation.B228TrailerRecords+" not equal to"+Validation.totalRecords);
                        Globals.logMapper.put("Status", "Failed");
                        Globals.logMapper.put("Class", "Main.class");
                        Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                        Globals.logMapper.clear();

                    }

                }
//				System.exit(0);
            }
            else{
                try {

                    FileConverter_MT fileConvert = new FileConverter_MT();
                    fileConvert.convert(inputfile, outputfile, configfile, outputIndexer, numThreads, batchSize, showRecCount,
                            suppAccountFile, trailerOutputFile, outputFormat, splitJSON, b1Settings, bucketName, outputQueueFolder,
                            headerQueueFolder, holdQueueFolder, queueFolder, headerInputFolder, s3, accountFilter, s3BatchSize, printSuppInd,
                            rbcFilterEnable, rbcAcfFile, rbcBranchFile);

                    logger.info("Conversion Successful.");
                    Globals.logMapper.put("Message",  "Conversion Successful.");
                    Globals.logMapper.put("Status", "success");
                    Globals.logMapper.put("Class", "EBCIDIC TO JSON CLASS");
                    Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                    Globals.logMapper.clear();


                } catch (Exception e) {

                    logger.error("An error occurred", e);
                    logger.error("An error occurred", e.getMessage());
                    Globals.logMapper.put("message",  "AN error occurred "+e.toString());
                    Globals.logMapper.put("Status", "failed");
                    Globals.logMapper.put("Class", "Main.class");
                    logger.error("An error occurred", e);
                    Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
                    Globals.logMapper.clear();
                    System.exit(1);

                }

            }
            Date endDate = new Date();
            logExecutionTime("Main", startDate, endDate);
            System.exit(0);


        } catch (IOException e) {
            logger.error("An error occurred", e);
            Globals.logMapper.put("message",  "AN error occurred "+e.toString());
            Globals.logMapper.put("Status", "failed");
            Globals.logMapper.put("Class", "Main.class");
            logger.error("An error occurred", e);
            Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
            Globals.logMapper.clear();

            System.exit(1);

        }
    }

    private static void loadDataFromFile(String filePath) {

        boolean isHoldHeader = false;
        boolean isInputHeader = false;
        boolean isCombinedTrid=false;
        boolean is50300FieldSection = false;
        boolean is27200FieldSection = false;
        boolean is30100FieldSection = false;
        boolean isKeyFields27200 = false;
        boolean isKeyFields50300 = false;
        boolean isKeyFields30100 = false;


        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {

                if(!line.isBlank()) {

                    if(line.startsWith("#") && line.contains("Hold Header Files")) {

                        isHoldHeader = true;
                        isInputHeader = false;
                        is50300FieldSection = false;
                        is27200FieldSection = false;
                        isCombinedTrid = false;
                        is30100FieldSection = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = false;
                        isKeyFields30100 = false;

                    } else if(line.startsWith("#") && line.contains("Header Files - Input Folder")) {

                        isInputHeader = true;
                        isHoldHeader = false;
                        is50300FieldSection = false;
                        is27200FieldSection = false;
                        isCombinedTrid = false;
                        is30100FieldSection = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = false;
                        isKeyFields30100 = false;

                    }  else if(line.startsWith("#") && line.contains("Combined Trids")) {

                        isCombinedTrid = true;
                        isInputHeader = false;
                        isHoldHeader = false;
                        is50300FieldSection = false;
                        is27200FieldSection = false;
                        is30100FieldSection = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = false;
                        isKeyFields30100 = false;

                    }
                    else if (line.startsWith("#") && line.contains("Lookup trids 50300 fields")) {
                        is50300FieldSection = true;
                        isCombinedTrid = false;
                        isInputHeader = false;
                        isHoldHeader = false;
                        is27200FieldSection = false;
                        is30100FieldSection = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = false;
                        isKeyFields30100 = false;

                    }
                    else if (line.startsWith("#") && line.contains("Lookup trids 27200 fields")) {
                        is27200FieldSection = true;
                        is50300FieldSection = false;
                        isCombinedTrid = false;
                        isInputHeader = false;
                        isHoldHeader = false;
                        is30100FieldSection = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = false;
                        isKeyFields30100 = false;

                    }
                    else if (line.startsWith("#") && line.contains("Lookup trids 30100 fields")) {
                        is30100FieldSection = true;
                        is27200FieldSection = false;
                        is50300FieldSection = false;
                        isCombinedTrid = false;
                        isInputHeader = false;
                        isHoldHeader = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = false;
                        isKeyFields30100 = false;

                    }
                    else if (line.startsWith("#") && line.contains("Lookup trids 27200 key fields")) {
                        is30100FieldSection = false;
                        is27200FieldSection = false;
                        is50300FieldSection = false;
                        isCombinedTrid = false;
                        isInputHeader = false;
                        isHoldHeader = false;
                        isKeyFields27200 = true;
                        isKeyFields50300 = false;
                        isKeyFields30100 = false;

                    }
                    else if (line.startsWith("#") && line.contains("Lookup trids 50300 key fields")) {
                        is30100FieldSection = false;
                        is27200FieldSection = false;
                        is50300FieldSection = false;
                        isCombinedTrid = false;
                        isInputHeader = false;
                        isHoldHeader = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = true;
                        isKeyFields30100 = false;

                    }
                    else if (line.startsWith("#") && line.contains("Lookup trids 30100 key fields")) {
                        is30100FieldSection = false;
                        is27200FieldSection = false;
                        is50300FieldSection = false;
                        isCombinedTrid = false;
                        isInputHeader = false;
                        isHoldHeader = false;
                        isKeyFields27200 = false;
                        isKeyFields50300 = false;
                        isKeyFields30100 = true;

                    }

                    else {
                        separateTridDataList.add(line.trim());
                        if(isHoldHeader) {
                            holdTridDataList.add(line.trim());
                            //System.out.println("holdTridDataList ->"+ line);

                        } else if (isInputHeader) {
                            inputFolderTridDataList.add(line.trim());
                            //System.out.println("inputFolderTridDataList ->"+ line);

                        } else if (isCombinedTrid) {
                            combinedTridDataList.add(line.trim());
                            //System.out.print("combinedTridDataList ->"+ line);

                        }
                        else if (is50300FieldSection) {
                            fields50300.add(line.trim());
                            //System.out.print("fields50300 ->"+ line);

                        }
                        else if (is27200FieldSection) {
                            fields27200.add(line.trim());
                            //System.out.print("fields27200 ->"+ line);

                        }
                        else if (is30100FieldSection) {
                            fields30100.add(line.trim());
                            //System.out.print("fields30100 ->"+ line);

                        }
                        else if (isKeyFields27200) {
                            KeyFields27200.add(line.trim());
                            //System.out.print("KeyFields27200 ->"+ line);

                        }
                        else if (isKeyFields50300) {
                            KeyFields50300.add(line.trim());
                            //System.out.print("KeyFields50300 ->"+ line);

                        }
                        else if (isKeyFields30100) {
                            KeyFields30100.add(line.trim());
                            //System.out.print("KeyFields30100 ->"+ line);

                        }


                    }

                }
            }

            logger.info("separateTridDataList.size() : " + separateTridDataList.size());
            logger.info("holdTridDataList.size() : " + holdTridDataList.size());
            logger.info("inputFolderTridDataList.size() : " + inputFolderTridDataList.size());
            logger.info("combinedTridDataList.size() : " + combinedTridDataList.size());
            logger.info("fields27200.size() : " + fields27200.size());
            logger.info("fields50300.size() : " + fields50300.size());
            logger.info("fields30100.size() : " + fields30100.size());
            logger.info("KeyFields50300.size() : " + KeyFields50300.size());
            logger.info("KeyFields27200.size() : " + KeyFields27200.size());
            logger.info("KeyFields30100.size() : " + KeyFields30100.size());

            Globals.logMapper.put("Message",  "separateTridDataList.size() : "+separateTridDataList.size()+ " holdTridDataList.size() :"+holdTridDataList.size() +" inputFolderTridDataList.size() : "+ inputFolderTridDataList.size() +" combinedTridDataList.size() : "+combinedTridDataList.size()+
                    " KeyFields50300.size() : "+KeyFields50300.size()+" KeyFields27200.size() : "+KeyFields27200.size() + "KeyFields30100.size() : " + KeyFields30100.size()+      "fields27200.size() : " + fields27200.size()+ "fields50300.size() : " + fields50300.size()+ "fields30100.size() : " + fields30100.size());
            Globals.logMapper.put("Status", "success");
            Globals.logMapper.put("Class", "Main.class");

            Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
            Globals.logMapper.clear();

        } catch (IOException e) {
            logger.error("An error occurred", e);
            Globals.logMapper.put("message",  "AN error occurred "+e.toString());
            Globals.logMapper.put("Status", "failed");
            Globals.logMapper.put("Class", "Main.class");
            logger.error("An error occurred", e);
            Splunk.logMessage(Globals.logMapper, awsAccountDetail+" "+region.toString());
            Globals.logMapper.clear();
        }
    }

    public static void logExecutionTime(String module, Date startDate , Date endDate){

        long diffMilliSeconds = endDate.getTime() - startDate.getTime();
        long durationMilliSeconds = diffMilliSeconds % 1000;
        long durationSeconds = diffMilliSeconds / 1000 % 60;
        long durationMinutes = diffMilliSeconds / (60 * 1000) % 60;
        long durationHours = diffMilliSeconds / (60 * 60 * 1000);
        logger.info(module + " : Execution Time [" + durationHours + "] hours, [" + durationMinutes + "] minutes, [" + durationSeconds + "] seconds and [" + durationMilliSeconds + "] MilliSeconds.");

    }
}
