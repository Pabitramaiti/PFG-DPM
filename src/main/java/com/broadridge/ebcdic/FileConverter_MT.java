package com.broadridge.ebcdic;
import java.io.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import com.broadridge.ebcdic.AccountFilterService;


@SuppressWarnings("unchecked")
public class FileConverter_MT {

    private static final Logger logger = LogManager.getLogger(FileConverter_MT.class);

    private static final Date date = new Date();

    static BufferedWriter writer3 = null;

    static BufferedWriter writer4 = null;
    static BufferedWriter writerRecon = null;
    static BufferedWriter writerReconAcctList = null;
    static BufferedWriter writerHistorical = null;
    static BufferedWriter writerSuppressed = null;

//	static BufferedWriter writerLookUpTable = null;

    static long Offset = 0L;

    static long prevOffset = 0L;

    static String accNum = "";

    static String fmt_clientID = "";

    static int length = 0;

    static long lineNumber = 0L;

    private static String millionInd = "";

    static String topAcct = "";

    static String uanNum = "";     //  ACCT_SWING

    static String printInd = "";

    static long acctCount = 0;

    static String hist_yy = "";
    static String hist_mm = "";

    private static boolean isNewAcct = false;

    static String isXferInd = " ";
    static String xferFromDate = " ";
    static String xferFromAccount = " ";
    static String xferToDate = " ";
    static String xferToAccount = " ";

    public static final String XBASERECLEN = "XBASE-HDR-RECLEN";

    public static boolean LOCAL_ENV;

    public static final String XBASETRID = "XBASE-HDR-TRID-00";

    public static final String XBASESUBTRID = "XBASE-HDR-SUBTRID-00";

    public static int XBASE_REC_LENGTH_ENDIDX;

    public static int XBASE_TRID12_STARTIDX;

    public static int XBASE_TRID12_ENDIDX;

    public Globals global = new Globals();

    public Account acct = null;

    public boolean loadedHeader = false;
    static String ebcdicFileName = "";
    private static String timestamp = Long.toString(System.currentTimeMillis());
    private static final String BATCH_PREFIX = "BATCH_";
    private Map<String, List<BatchItem>> uploadBatches = new ConcurrentHashMap<>();
    private int s3UploadBatchSize = 100;
    //   private Set<String> validAccountNumbers = new HashSet<>();
    private boolean filterAccountsEnabled = false;
    private final AccountFilterService accountFilterService = AccountFilterService.getInstance();
    private final CurrencySuppressionService currencySuppressionService = CurrencySuppressionService.getInstance();
    /** ACF (B228) filter service – enabled/disabled by the -rbcfilterenable flag. */
    private final AcfFilterService acfFilterService = AcfFilterService.getInstance();
    /** Tracks the 20100-NAP-ALT-BRANCH value of the current account (reset per account). */
    private String currentAcctAltBranch = "";
    /** Tracks the 20100-NAP-RR-NUMBER (REP) value of the current account (reset per account). */
    private String currentAcctRep = "";
    private String reconFolderPath;
    private static class AccountFilterRule {
        String clientId;
        String startAccount;
        String endAccount;
        boolean isSingleAccount;
        String printIndicator;
        boolean isPrefix; // New field to indicate if this is a prefix rule

        AccountFilterRule(String clientId, String account, String printIndicator) {
            this.clientId = clientId;
            this.startAccount = account;
            this.endAccount = account;
            this.isSingleAccount = true;
            this.printIndicator = printIndicator;
            this.isPrefix = account.length() < 4; // Assume prefix if less than full account length
        }

        AccountFilterRule(String clientId, String startAccount, String endAccount, String printIndicator) {
            this.clientId = clientId;
            this.startAccount = startAccount;
            this.endAccount = endAccount;
            this.isSingleAccount = false;
            this.printIndicator = printIndicator;
            this.isPrefix = startAccount.length() < 4 || endAccount.length() < 4; // Prefix if either is short
        }
    }

    private List<AccountFilterRule> accountFilterRules = new ArrayList<>();

    public boolean firstAcct = true;
    private static LinkedHashMap<String, JSONArray> sperateTridData = new LinkedHashMap<String, JSONArray>();
    private static LinkedHashMap<String, JSONArray> inputFolderTridData = new LinkedHashMap<String, JSONArray>();

    ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final Set<String> writtenKeys = new HashSet<>();
    private static final Set<String> writtenAccNums = new HashSet<>();

    public static long totalRecords = 0;

    public static long B228TrailerRecords = 0;

    BufferedWriter writer2 = null;
    BufferedWriter writer59 = null;


    public File outFile = null;
    //changes for batch upload
    private static final int S3_BATCH_SIZE = 1000;
    private final List<Runnable> s3UploadTasks = new ArrayList<>();
    private final ExecutorService s3UploadExecutor = Executors.newFixedThreadPool(160); // adjust thread count as needed

    private void logMemoryStatus(String context) {
        long totalMemory = Runtime.getRuntime().totalMemory();
        long freeMemory = Runtime.getRuntime().freeMemory();
        //long usedMemory = totalMemory - freeMemory;
        logger.info(context + " - Memory Status - Total: " + (totalMemory / 1024 / 1024) + " MB, Free: " + (freeMemory / 1024 / 1024) + " MB");
    }

    //This method is now moved to AccountFilterService module and hence commented
    // Add this method to read account numbers from account filter file
 /*   private void loadValidAccountNumbers(String accountFile) {
        if (accountFile == null || accountFile.isEmpty()) {
            logger.info("No account filter file provided - processing all accounts");
            return;
        }

        try (BufferedReader reader = new BufferedReader(new FileReader(accountFile))) {
            String line;
            int count = 0;
            boolean isFirstLine = true;

            while ((line = reader.readLine()) != null) {
                // Skip header line
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                line = line.trim();
                if (line.isEmpty()) continue;

                String[] parts = line.split(",");
                if (parts.length != 3) {
                    logger.warn("Invalid line format, skipping: " + line);
                    continue;
                }

                String clientId = parts[0].trim();
                String accountRange = parts[1].trim();
                String printIndicator = parts[2].trim();

                // Process account range
                if (accountRange.contains("-")) {
                    // Range format: 1AA00000-1ZZ99999
                    String[] range = accountRange.split("-");
                    if (range.length == 2) {
                        String startAccount = range[0].trim();
                        String endAccount = range[1].trim();
                        accountFilterRules.add(new AccountFilterRule(clientId, startAccount, endAccount, printIndicator));
                        logger.info("Client ID: " + clientId + ", Account Range: " + startAccount + " to " + endAccount + ", Action: " + printIndicator);
                        count++;
                    }
                } else {
                    // Single account: 9BB24444
                    accountFilterRules.add(new AccountFilterRule(clientId, accountRange, printIndicator));
                    logger.info("Client ID: " + clientId + ", Account: " + accountRange + ", Action: " + printIndicator);
                    if ("P".equalsIgnoreCase(printIndicator)) {
                        validAccountNumbers.add(accountRange);
                    }
                    count++;
                }
            }

            // Only enable filtering if we actually loaded account numbers
            if (count > 0) {
                filterAccountsEnabled = true;
                logger.info("Loaded " + count + " account filter rules");
            } else {
                filterAccountsEnabled = false;
                logger.info("Account file was empty - processing all accounts");
            }
        } catch (IOException e) {
            logger.error("Error reading account filter file: " + accountFile, e);
            // Continue without filtering if file can't be read
        }
    }*/

    // Method to check if an account should be printed or suppressed
    private String shouldPrintAccount(String accountNumber, String clientId) {
        logger.debug("Checking print status for account: " + accountNumber + ", clientId: " + clientId);

        if (accountFilterService.isEnabled()) {
            boolean emit = accountFilterService.shouldEmit(accountNumber, clientId);
            logger.debug("Delegated filter decision to AccountFilterService. Emit=" + emit);
            return emit ? "P" : "S";
        }

        if (!filterAccountsEnabled) {
            logger.debug("Account filtering is disabled. Returning 'P' (Print) for account: " + accountNumber);
            return "P"; // Print by default if no filtering
        }

        boolean suppress = false;

        for (AccountFilterRule rule : accountFilterRules) {
            // Check if client ID matches
            if (!rule.clientId.equals(clientId)) {
                continue;
            }

            // Check single account or prefix
            if (rule.isSingleAccount) {
                if (rule.isPrefix) {
                    if (accountNumber.startsWith(rule.startAccount)) {
                        suppress = "S".equalsIgnoreCase(rule.printIndicator);
                    }
                } else if (accountNumber.equals(rule.startAccount)) {
                    suppress = "S".equalsIgnoreCase(rule.printIndicator);
                }
            } else {
                if (rule.isPrefix) {
                    if (isAccountInPrefixRange(accountNumber, rule.startAccount, rule.endAccount)) {
                        suppress = "S".equalsIgnoreCase(rule.printIndicator);
                    }
                } else if (isAccountInRange(accountNumber, rule.startAccount, rule.endAccount)) {
                    suppress = "S".equalsIgnoreCase(rule.printIndicator);
                }
            }

            // If suppression is determined, no need to check further
            if (suppress) {
                return "S";
            }
        }

        // If no suppression rule matched, check for print rules
        for (AccountFilterRule rule : accountFilterRules) {
            if (!rule.clientId.equals(clientId)) {
                continue;
            }

            if (rule.isSingleAccount) {
                if (rule.isPrefix) {
                    if (accountNumber.startsWith(rule.startAccount)) {
                        return "P";
                    }
                } else if (accountNumber.equals(rule.startAccount)) {
                    return "P";
                }
            } else {
                if (rule.isPrefix) {
                    if (isAccountInPrefixRange(accountNumber, rule.startAccount, rule.endAccount)) {
                        return "P";
                    }
                } else if (isAccountInRange(accountNumber, rule.startAccount, rule.endAccount)) {
                    return "P";
                }
            }
        }

        logger.info("No matching rule found for account: " + accountNumber + ", clientId: " + clientId + ". Returning 'S' (Suppress)");
        return "S"; // Default to suppress if no matching rule found
    }

    // Method to check if an account falls within a range
    private boolean isAccountInRange(String account, String startRange, String endRange) {
        // Ensure all accounts are the same length for comparison
        if (account.length() != startRange.length() || account.length() != endRange.length()) {
            return false;
        }

        // Lexicographic comparison
        return account.compareTo(startRange) >= 0 && account.compareTo(endRange) <= 0;
    }

    // Method to check if an account falls within a prefix range (e.g., 2SL-2SP)
    private boolean isAccountInPrefixRange(String account, String startPrefix, String endPrefix) {
        // Extract the prefix from the account number (same length as the range prefixes)
        int prefixLength = Math.min(startPrefix.length(), endPrefix.length());
        if (account.length() < prefixLength) {
            return false;
        }

        String accountPrefix = account.substring(0, prefixLength);

        // Lexicographic comparison of prefixes
        return accountPrefix.compareTo(startPrefix) >= 0 && accountPrefix.compareTo(endPrefix) <= 0;
    }

    private static class BatchItem {
        final String key;
        final String content;

        BatchItem(String key, String content) {
            this.key = key;
            this.content = content;
        }
    }

    private void submitS3UploadTask(Runnable task) {
        s3UploadTasks.add(task);
        if (s3UploadTasks.size() >= S3_BATCH_SIZE) {
            int taskCount = s3UploadTasks.size();
            logger.info("Submitting batch of " + taskCount + " S3 upload tasks");
            for (Runnable t : s3UploadTasks) {
                s3UploadExecutor.submit(t);
            }
            s3UploadTasks.clear();

            // Add backpressure - check if executor queue is getting too large
            ThreadPoolExecutor executor = (ThreadPoolExecutor)s3UploadExecutor;
            int queueSize = executor.getQueue().size();
            if (queueSize > 2000) {
                // Calculate wait time - longer pause for larger queues
                long waitTime = Math.min(5000, 50 + (queueSize - 1000) * 2); // Cap at 30 seconds
                logger.info("S3 upload queue size: " + queueSize + ", pausing for " + waitTime + "ms to allow processing");
                try {
                    Thread.sleep(waitTime);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.warn("Thread was interrupted while waiting for S3 queue to process");
                }
            }
        }
    }

    private void flushS3UploadTasks() {
        int taskCount = s3UploadTasks.size();
        logger.info("Flushing " + taskCount + " pending S3 upload tasks");
        for (Runnable t : s3UploadTasks) s3UploadExecutor.submit(t);
        s3UploadTasks.clear();
    }

    private void flushS3UploadTasks(int batchSize) {
        int taskCount = s3UploadTasks.size();
        int tasksToFlush = Math.min(batchSize, taskCount);
        logger.info("Partially flushing " + tasksToFlush + " of " + taskCount + " pending S3 upload tasks");

        if (tasksToFlush > 0) {
            List<Runnable> tasksToProcess = new ArrayList<>(s3UploadTasks.subList(0, tasksToFlush));
            s3UploadTasks.subList(0, tasksToFlush).clear();

            for (Runnable t : tasksToProcess) {
                s3UploadExecutor.submit(t);
            }

            // Report queue status after submission
            ThreadPoolExecutor executor = (ThreadPoolExecutor)s3UploadExecutor;
            logger.info("S3 upload queue status: " + executor.getQueue().size() + " pending, "
                    + executor.getActiveCount() + " active tasks");
        }
    }


    public void convert(String inputFile, String outputFile, String configFile, String outputIndexer, int NUM_THREADS, int BATCH_SIZE, int showRecCount, String suppAccountFile, String trailerFile, String outputFormat,
                        String splitJSON, String b1Settings, String bucketName, String outputQueueFolder, String headerQueueFolder, String holdQueueFolder, String queueFolder, String headerInputFolder, S3Client s3, String accountFile, int s3BatchSize, boolean printSuppInd,
                        boolean rbcFilterEnable, String rbcAcfFile, String rbcBranchFile) throws IOException, InterruptedException {
        this.s3UploadBatchSize = s3BatchSize > 0 ? s3BatchSize : 100;
        ebcdicFileName = inputFile;
        logger.info("Starting convert()...");
        logMemoryStatus("Start of convert()");
        //   loadValidAccountNumbers(accountFile);
        accountFilterService.loadFromFile(accountFile);
        currencySuppressionService.clear();
        currencySuppressionService.setEnabled(printSuppInd);
        logger.info("Currency suppression enabled: {}", printSuppInd);

        // Initialise ACF (B228) filter - isolated behind the rbcFilterEnable flag
        acfFilterService.clear();
        if (rbcFilterEnable) {
            acfFilterService.loadBranchFile(rbcBranchFile);
            acfFilterService.loadAcfFile(rbcAcfFile);
            acfFilterService.setEnabled(true);
            logger.info("RBC ACF filter enabled with acfFile={} branchFile={}", rbcAcfFile, rbcBranchFile);
        } else {
            logger.info("RBC ACF filter is disabled (rbcfilterenable=false).");
        }

        ConfigSplit.readB1Settings(b1Settings);
        ConfigSplit.readConfig(configFile);

        InputStreamReader inputr1 = null;

        BufferedReader br1 = null;

        outFile = new File(outputFile);
        //System.out.println("outFile::" + outFile);
        this.reconFolderPath = outputQueueFolder;
        ensureReconFilesInitialized(this.reconFolderPath);

        if (!splitJSON.equals("Y")) {

            //Specify the Final Output file path

            OutputStreamWriter ostream2 = new OutputStreamWriter(new FileOutputStream(outputFile), StandardCharsets.ISO_8859_1);

            writer2 = new BufferedWriter(ostream2);
        }


        int ch;

        try {

            //Specify the Input file path

            inputr1 = new InputStreamReader(new FileInputStream(inputFile), "CP819");

            br1 = new BufferedReader(inputr1, 8192);


            if (outputFormat.equals("ASCII")) {
                writer3 = new BufferedWriter(new FileWriter(outputIndexer));

                writer4 = new BufferedWriter(new FileWriter(suppAccountFile));
            }

            long chara = 0;

            String lineLength = "";

            long totalLine = 0;

            int record = 0;

            String ebcdic = "";

            int index = 0;

            int futuresIndex = 0;

            boolean writeFlag = false;

            long lengthOfChar = 0;

            StringBuilder sbEbcdic = new StringBuilder();

            String printOutput = "";

            String strIndexer = "";

            ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

            List<Future<RecordResult>> futuresList = new ArrayList<>(BATCH_SIZE + 5);


            // Get Indexes (constants)

            XBASE_REC_LENGTH_ENDIDX = ConfigSplit.detailConfigMap.get(XBASERECLEN).getEndIdx();

            XBASE_TRID12_STARTIDX = ConfigSplit.detailConfigMap.get(XBASETRID).getStartIdx();

            XBASE_TRID12_ENDIDX = ConfigSplit.detailConfigMap.get(XBASESUBTRID).getEndIdx();


            while ((ch = br1.read()) != -1) {

                chara++;

                if (chara != totalLine + XBASE_REC_LENGTH_ENDIDX) {

                    lineLength = Integer.toHexString(ch);

                    sbEbcdic.append((char) ch);

                } else {

                    if (Integer.toHexString(ch).length() == 1) {

                        totalLine = totalLine + Integer.parseInt(lineLength + "0" + Integer.toHexString(ch), 16);

                        record = Integer.parseInt(lineLength + "0" + Integer.toHexString(ch), 16);

                        sbEbcdic.append((char) ch);

                    } else {

                        totalLine = totalLine + Integer.parseInt(lineLength + Integer.toHexString(ch), 16);

                        record = Integer.parseInt(lineLength + Integer.toHexString(ch), 16);

                        sbEbcdic.append((char) ch);

                    }

                }


                if (chara == record + lengthOfChar) {

                    writeFlag = true;

                    ebcdic = sbEbcdic.toString();

                    sbEbcdic.setLength(0);

                }


                if (writeFlag) { // Record is ready to process

                    index++;

                    // Create Thread Task

                    Callable<RecordResult> task = new EBCDICRecordToASCIItask(ebcdic, index, record, lengthOfChar, global,accountFilterService);

                    Future<RecordResult> future = executor.submit(task);

                    futuresList.add(future);


                    // Pause every BATCH_SIZE records, to process results from futures so far.

                    if (index % BATCH_SIZE == 0) {

                        if (outputFormat.equals("ASCII")) {
                            for (Future<RecordResult> aFuture : futuresList) {

                                try {

                                    futuresIndex++;

                                    RecordResult rr = aFuture.get();


                                    // (0) Is New Acct?

                                    if (accNum.equals("") || !accNum.equals(rr.recAccNum)) isNewAcct = true;

                                    // (2) handle returned Indexer data

                                    if (rr.isWriteIndexerFile && isNewAcct) {

                                        strIndexer = uanNum + "|" + topAcct + "|" + accNum + "|" + lineNumber + "|" + prevOffset + "|"

                                                + String.valueOf(length + 1) + "|" + millionInd + "|" + printInd + "|" + isXferInd + "|" + xferFromAccount + "|" + xferFromDate + "|" + xferToAccount + "|" + xferToDate + "\n";

                                        writer3.write(strIndexer);

                                        if (printInd.equals("0")) {
                                            writer4.write(accNum.substring(0, 3) + " " + accNum.substring(3) + " " + "\n");
                                        }

                                        topAcct = "";
                                        uanNum = "";
                                        isXferInd = " ";
                                        xferFromAccount = " ";
                                        xferFromDate = " ";
                                        xferToAccount = " ";
                                        xferToDate = " ";

                                    }


                                    // (3) handle returned ASCII data

                                    printOutput = rr.strASCII;

                                    if (rr.accNumCheck) {

                                        if (isNewAcct) { //should have ETSXBASENEW

                                            printOutput = rr.strASCII;

                                            accNum = rr.recAccNum; // update accNum
                                            fmt_clientID = rr.clientId;

                                            if (!rr.millionInd.equals("")) millionInd = rr.millionInd;

                                            if (!rr.printInd.equals("")) printInd = rr.printInd;

                                            prevOffset = Offset;

                                            length = 0;

                                        } else { // not new acc

                                            printOutput = rr.strASCII.replace("ETSXBASENEW", "ETSXBASE");

                                        }

                                    }

                                    if (printOutput.length() > 1) {

                                        writer2.write(printOutput + "\n");

                                    }


                                    // (4) update post-set globals

                                    if (!rr.topAcct.equals("")) topAcct = rr.topAcct;
                                    if (!rr.uanNum.equals("")) uanNum = rr.uanNum;

                                    if (!rr.xferFromDate.equals("")) xferFromDate = rr.xferFromDate;
                                    if (!rr.xferToDate.equals("")) xferToDate = rr.xferToDate;

                                    if (!rr.xferFromAccount.equals("")) xferFromAccount = rr.xferFromAccount;
                                    if (!rr.xferToAccount.equals("")) xferToAccount = rr.xferToAccount;

                                    if (!xferFromDate.trim().equals("") || !xferToDate.trim().equals("")) {
                                        isXferInd = "T";
                                    } else {
                                        isXferInd = "F";
                                    }


                                    if (printOutput.length() > 0) {

                                        String[] linenumbers = printOutput.split("\\r?\\n");

                                        lineNumber += linenumbers.length;

                                        Offset += printOutput.length() + 1;

                                        length += printOutput.length() + 1;

                                    }


                                    isNewAcct = false;


                                } catch (Exception e) {

                                    logger.error(e);

                                }


                            } // end for; futures loop

                        } else {
                            // IF OUTPUT FORMAT IS JSON
                            if (!splitJSON.equals("Y")) {
                                loadJSONObjectForAccount(futuresList);
                            } else {
                                loadSplitJSONObjectForAccount(futuresList, bucketName, queueFolder, s3, outputQueueFolder);
                            }

                            futuresList.clear();

                        }

                    } // end if; Done processing BATCH_SIZE futures.

                    lengthOfChar = chara;

                    ebcdic = "";

                    writeFlag = false;


                    if (showRecCount > 0) {

                        if (index % showRecCount == 0) {

                            logger.info("processed " + index + " recs");
                        }

                    }

                } // if writeflag

            }


            // process futures from last part of file
            if (outputFormat.equals("ASCII")) {
                for (Future<RecordResult> aFuture : futuresList) {

                    try {

                        futuresIndex++;

                        RecordResult rr = aFuture.get();


                        // (0) Is New Acct?

                        if (accNum.equals("") || !accNum.equals(rr.recAccNum)) isNewAcct = true;


                        if (rr.isWriteIndexerFile && isNewAcct) {

                            strIndexer = uanNum + "|" + topAcct + "|" + accNum + "|" + lineNumber + "|" + prevOffset + "|"

                                    + String.valueOf(length + 1) + "|" + millionInd + "|" + printInd + "|" + isXferInd + "|" + xferFromAccount + "|" + xferFromDate + "|" + xferToAccount + "|" + xferToDate + "\n";

                            writer3.write(strIndexer);

                            if (printInd.equals("0")) {
                                writer4.write(accNum.substring(0, 3) + " " + accNum.substring(3) + " " + "\n");
                            }

                            topAcct = "";
                            uanNum = "";
                            isXferInd = " ";
                            xferFromAccount = " ";
                            xferFromDate = " ";
                            xferToAccount = " ";
                            xferToDate = " ";

                        }


                        // (3) handle returned ASCII data

                        printOutput = rr.strASCII;

                        if (rr.accNumCheck) {

                            if (isNewAcct) {

                                printOutput = rr.strASCII; // should have ETSXBASENEW

                                accNum = rr.recAccNum; // update accNum
                                fmt_clientID = rr.clientId;

                                if (!rr.millionInd.equals("")) millionInd = rr.millionInd;

                                if (!rr.printInd.equals("")) printInd = rr.printInd;
                                prevOffset = Offset;

                                length = 0;

                            } else { // not new acc

                                printOutput = rr.strASCII.replace("ETSXBASENEW", "ETSXBASE");

                            }

                        }

                        if (printOutput.length() > 1) {

                            writer2.write(printOutput + "\n");

                        }


                        // (4) update post-set globals

                        if (!rr.topAcct.equals("")) topAcct = rr.topAcct;
                        if (!rr.uanNum.equals("")) uanNum = rr.uanNum;

                        if (!rr.xferFromDate.equals("")) xferFromDate = rr.xferFromDate;
                        if (!rr.xferToDate.equals("")) xferToDate = rr.xferToDate;

                        if (!rr.xferFromAccount.equals("")) xferFromAccount = rr.xferFromAccount;
                        if (!rr.xferToAccount.equals("")) xferToAccount = rr.xferToAccount;

                        if (!xferFromDate.trim().equals("") || !xferToDate.trim().equals("")) {
                            isXferInd = "T";
                        } else {
                            isXferInd = "F";
                        }

                        if (printOutput.length() > 0) {

                            String[] linenumbers = printOutput.split("\\r?\\n");

                            lineNumber += linenumbers.length;

                            Offset += printOutput.length() + 1;

                            length += printOutput.length() + 1;

                        }

                        isNewAcct = false;


                    } catch (Exception e) {

                        logger.error(e);

                    }

                } // end for; last part of file

                futuresList.clear();

                executor.shutdown();
            } else {

                if (!splitJSON.equals("Y")) {
                    loadJSONObjectForAccount(futuresList);
                } else {
                    loadSplitJSONObjectForAccount(futuresList, bucketName, queueFolder, s3, outputQueueFolder);
                }

                futuresList.clear();

                executor.shutdown();

                // **ADD THIS: Write the last account's COMBO file**
                if (splitJSON.equals("Y") && acct != null && !acct.jsonAccount.isEmpty()) {
                    Map<String, Integer> serialNumbers = new HashMap<>();
                    processNewAccount(bucketName, queueFolder, s3, outputQueueFolder, serialNumbers);
                }

                // Write Trailers to JSON FILE WITH LAST ACCOUNT
                writeLastAccountWithTrailers(inputFile, trailerFile, splitJSON, bucketName, outputQueueFolder, queueFolder, s3);


            }
            //	if(writerRecon!=null) writerRecon.close();

        } catch (Exception e) {

            logger.error("An error occurred", e);

        } finally {

            writerSperateTridList(bucketName, headerQueueFolder, holdQueueFolder, queueFolder, headerInputFolder, s3);


            if (outputFormat.equals("ASCII")) {
                writer3.write(uanNum + "|" + topAcct + "|" + accNum + "|" + lineNumber + "|"

                        + prevOffset + "|" + String.valueOf(length + 1) + "|"

                        + millionInd + "|" + printInd + "|" + isXferInd + "|" + xferFromAccount + "|" + xferFromDate + "|" + xferToAccount + "|" + xferToDate + "\n");

                if (printInd.equals("0")) {
                    writer4.write(accNum.substring(0, 3) + " " + accNum.substring(3) + " ");
                }

                writer3.flush();

                writer3.close();

                writer4.flush();

                writer4.close();
            }

            if (writer2 != null) {
                writer2.close();
            }

            br1.close();

            inputr1.close();

            ConfigSplit.detailConfigMap5030.clear();

            ConfigSplit.detailConfigMap0530.clear();

            ConfigSplit.detailConfigMap504141.clear();

            ConfigSplit.detailConfigMap5040.clear();

            ConfigSplit.detailConfigMap3010.clear();

            ConfigSplit.detailConfigMap2010.clear();

            ConfigSplit.detailConfigMap1010.clear();

            ConfigSplit.detailConfigMap2410.clear();

            ConfigSplit.detailConfigMap2582.clear();

            ConfigSplit.detailConfigMap3011.clear();

            ConfigSplit.detailConfigMap504150.clear();

            ConfigSplit.detailConfigMap3020.clear();

            ConfigSplit.detailConfigMap3050.clear();

            ConfigSplit.detailConfigMap22NN.clear();

            ConfigSplit.detailConfigMap5085.clear();

            ConfigSplit.detailConfigMap.clear();

        }
        // create Delimited file
        createDelimitedFile(s3,bucketName, headerInputFolder, true, " ","50300", inputFile);
        createDelimitedFile(s3,bucketName, headerInputFolder, true, " ","27200", inputFile);
        createDelimitedFile(s3,bucketName, headerInputFolder, true, " ","30100", inputFile);
        flushAllBatches(s3, bucketName);
        logger.info("Flushing remaining S3 upload tasks before shutdown...");
        flushS3UploadTasks();

        // Wait for all submitted tasks to complete
        logMemoryStatus("Before executor shutdown");
        s3UploadExecutor.shutdown();
        try {
            // Use a phased approach for shutdown with status updates
            for (int phase = 1; phase <= 8; phase++) {
                logger.info("S3 executor shutdown phase " + phase + " of 8...");

                int remainingTasks = ((ThreadPoolExecutor)s3UploadExecutor).getQueue().size();
                int activeTasks = ((ThreadPoolExecutor)s3UploadExecutor).getActiveCount();

                if (remainingTasks == 0 && activeTasks == 0) {
                    logger.info("All S3 upload tasks completed successfully");
                    break;
                }

                logger.info("Waiting for S3 uploads: " + remainingTasks + " pending and " + activeTasks + " active tasks");

                // Wait up to 60 minutes per phase (total 5 hours possible)
                if (!s3UploadExecutor.awaitTermination(60, TimeUnit.MINUTES)) {
                    if (phase == 8) {
                        logger.warn("Maximum shutdown time reached. Forcing shutdown with " +
                                remainingTasks + " pending and " + activeTasks + " active tasks");
                        List<Runnable> droppedTasks = s3UploadExecutor.shutdownNow();
                        logger.warn("Dropped " + droppedTasks.size() + " tasks from upload queue");
                    }
                    // Otherwise continue to next phase
                }
            }
        } catch (InterruptedException e) {
            logger.error("S3 upload executor was interrupted while waiting for completion", e);
            s3UploadExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // After all S3 upload executor tasks are done
        if(writerRecon != null) {
            try {
                writerRecon.flush();
                writerRecon.close();
                logger.info("Recon writer closed successfully");
            } catch (IOException e) {
                logger.error("Error closing recon writer", e);
            }
        }
        if(writerReconAcctList != null) {
            try {
                writerReconAcctList.flush();
                writerReconAcctList.close();
                logger.info("Recon Account List writer closed successfully");
            } catch (IOException e) {
                logger.error("Error closing recon writer", e);
            }
        }
        if(writerHistorical != null) {
            try {
                writerHistorical.flush();
                writerHistorical.close();
                logger.info("Historical writer closed successfully");
            } catch (IOException e) {
                logger.error("Error closing Historical writer", e);
            }
        }

        // Write suppressed accounts report if currency suppression is enabled
        if (printSuppInd && currencySuppressionService.isEnabled()) {
            logger.info("Writing suppressed accounts report...");
            writeSuppressedAccountsReport(reconFolderPath, s3, bucketName, outputQueueFolder);
        }

        logMemoryStatus("End of convert()");
        logger.info("End of convert()!");
    }

    private void writeLastAccountWithTrailers(String inputFile, String trailerFile, String splitJSON, String bucketName, String outputQueueFolder, String queueFolder, S3Client s3) {

        logger.info("Starting writeLastAccountWithTrailers()...");
        try {
            if (acct != null) {
                if (acct.jArr1010.size() > 0) {
                    acct.jsonAccount.put("ETS1010", acct.jArr1010);
                }
                if (acct.jArr1013.size() > 0) {
                    acct.jsonAccount.put("ETS1013", acct.jArr1013);
                }
                if (acct.jArr2010.size() > 0) {
                    acct.jsonAccount.put("ETS2010", acct.jArr2010);
                }
                if (acct.jMap22NN.size() > 0) {
                    for (Entry<String, JSONArray> entry : acct.jMap22NN.entrySet()) {
                        acct.jsonAccount.put(entry.getKey(), entry.getValue());
                    }
                }
                if (acct.jArr2582.size() > 0) {
                    acct.jsonAccount.put("ETS2582", acct.jArr2582);
                }
                if (acct.jArr2410.size() > 0) {
                    acct.jsonAccount.put("ETS2410", acct.jArr2410);
                }
                if (acct.jMap503.size() != 0) {
                    for (Entry<String, JSONArray> entry : acct.jMap503.entrySet()) {
                        acct.jsonAccount.put(entry.getKey(), entry.getValue());
                    }
                }
                if (acct.jArr5050.size() > 0) {
                    acct.jsonAccount.put("ETS5050", acct.jArr5050);
                }
                if (acct.jArr5040.size() > 0) {
                    acct.jsonAccount.put("ETS5040", acct.jArr5040);
                }
                if (acct.jArr3010.size() > 0) {
                    acct.jsonAccount.put("ETS3010", acct.jArr3010);
                }
                if (acct.jArr2720.size() > 0) {
                    acct.jsonAccount.put("ETS2720", acct.jArr2720);
                }
                if (acct.jArr1120.size() > 0) {
                    acct.jsonAccount.put("ETS1120", acct.jArr1120);
                }
                if (acct.jArr3011.size() > 0) {
                    acct.jsonAccount.put("ETS3011", acct.jArr3011);
                }
                if (acct.jArr3020.size() > 0) {
                    acct.jsonAccount.put("ETS3020", acct.jArr3020);
                }
                if (acct.jArr3050.size() > 0) {
                    acct.jsonAccount.put("ETS3050", acct.jArr3050);
                }
                if (acct.extraRecords.size() != 0) {
                    for (Entry<String, JSONArray> entry : acct.extraRecords.entrySet()) {
                        acct.jsonAccount.put(entry.getKey(), entry.getValue());
                    }
                }
                if (!splitJSON.equals("Y")) {
                    if(firstAcct) writer2.write( mapper.writeValueAsString(acct.jsonAccount)+ "\n] \n}\n}");
                    else writer2.write(", \n" + mapper.writeValueAsString(acct.jsonAccount)+ "\n] \n}\n}");
                } else {
                    String name = outFile.getName();
                    name = accNum + "_" + name;

                    String key = name;
                    //				System.out.println("print key" +key);
	/*				String content = "{\"B228_FILE\":{ \n"
							+ "\"HEADER\":" + mapper.writeValueAsString(global.headerArray) + ", \n"
//        					+ "\"CLIENT_HEADER\":"+mapper.writeValueAsString(global.clientArray)+", \n"
							+ "\"ACCOUNTS\":[" + mapper.writeValueAsString(acct.jsonAccount) + "] \n}\n}";
					System.out.println("lastaccount::" +content);

////        			logger.info("checking the key ->" + key);
					boolean writeIt = true;
					s3FileWrite(s3, bucketName, queueFolder, key, content, writeIt, outputQueueFolder); */
                }

            }

/* Not needed in this class
        addTrailersToOutputFiles(inputFile , trailerFile, s3, bucketName, outputQueueFolder);
*/
        } catch (Exception e) {
            logger.error("An error occurred", e);
        }
        logger.info("End of writeLastAccountWithTrailers()!");
    }

    private void loadSplitJSONObjectForAccount(List<Future<RecordResult>> futuresList, String bucketName, String queueFolder, S3Client s3, String outputQueueFolder) {
        try {
            Map<String, Integer> serialNumbers = new HashMap<>(); // Initialize serial numbers for each record type

            for (Future<RecordResult> aFuture : futuresList) {
                RecordResult rr = aFuture.get();

                if (rr.historical_yy.trim().length() > 0) {
                    hist_yy = rr.historical_yy;
                }
                if (rr.historical_mm.trim().length() > 0) {
                    hist_mm = rr.historical_mm;
                }

                // Process headers
                if (rr.isHeader) {
                    processHeader(rr);
                }
                // Check for new account
                if (rr.accNumCheck && (accNum.isEmpty() || !accNum.equals(rr.recAccNum))) {
                    // Check if we should process the NEW account
                    String printIndicator = shouldPrintAccount(rr.recAccNum, rr.clientId);

                    if ("P".equals(printIndicator)) {
                        // Process the old account before moving to new one
                        if (!accNum.isEmpty()) {
                            // ACF filter: apply before writing old account COMBO (done inside processNewAccount)
                            processNewAccount(bucketName, queueFolder, s3, outputQueueFolder, serialNumbers);
                        }
                        accNum = rr.recAccNum;
                        acct = new Account();
                        acctCount++;
                        fmt_clientID = rr.clientId;
                        // Reset ACF tracking fields for the new account
                        currentAcctAltBranch = "";
                        currentAcctRep = "";
                    } else {
                        // Skip this account - marked as suppress
                        logger.debug("Suppressing account " + rr.recAccNum + " based on filter rules");
                        // Process old account if it exists
                        if (!accNum.isEmpty() && acct != null) {
                            processNewAccount(bucketName, queueFolder, s3, outputQueueFolder, serialNumbers);
                        }
                        accNum = rr.recAccNum; // Update to new account but don't process
                        acct = null; // Don't create new account object
                        fmt_clientID = rr.clientId;
                        // Reset ACF tracking fields even for suppressed accounts
                        currentAcctAltBranch = "";
                        currentAcctRep = "";
                    }
                }

                // Capture ACF filter fields from 2010 and suppress the account before writing record JSONs.
                if (rr.jObj2010 != null && acct != null) {
                    Object altBranchObj = rr.jObj2010.get("20100-NAP-ALT-BRANCH");
                    Object repObj       = rr.jObj2010.get("20100-NAP-RR-NUMBER");
                    if (altBranchObj != null) currentAcctAltBranch = altBranchObj.toString().replace("\u0000", " ").trim();
                    if (repObj       != null) currentAcctRep       = repObj.toString().replace("\u0000", " ").trim();

                    if (acfFilterService.isEnabled()) {
                        String firmId = acfFilterService.lookupFirmId(currentAcctAltBranch);
                        if (!acfFilterService.shouldEmit(firmId, currentAcctAltBranch, currentAcctRep, accNum)) {
                            logger.info("ACF filter: suppressing account={} firm={} altBranch={} rep={} before record writes",
                                    accNum, firmId, currentAcctAltBranch, currentAcctRep);
                            acct = null;
                            continue;
                        }
                    }
                }

                // Process record types dynamically
                if (!rr.isHeader && !rr.isTrailer) {
                    if (acct == null) {
                        continue;
                    }
                    Map<String, JSONObject> recordTypeMap = getRecordTypeMap(rr);
                    for (Map.Entry<String, JSONObject> entry : recordTypeMap.entrySet()) {
                        handleRecordType(rr, entry.getKey(), entry.getValue(), serialNumbers, bucketName, queueFolder, s3, outputQueueFolder);
                    }
                }

                // Process trailers
                if (rr.isTrailer && rr.jObj != null) {
                    processTrailer(rr);
                }
            }
//            if (acct != null && !acct.jsonAccount.isEmpty()) {
//                processNewAccount(bucketName, queueFolder, s3, outputQueueFolder, serialNumbers);
//            }
        } catch (Exception e) {
            logger.error("An error occurred", e);
        }
    }

    private void processHeader(RecordResult rr) {
        if (rr.jObj0000 != null) {
            global.jArr0000.add(rr.jObjXbase0000);
            global.jArr0000.add(rr.jObj0000);
        } else if (rr.jObj0420 != null) {
            global.jArr0420.add(rr.jObj0420);
        } else if (rr.jObj0520 != null) {
            global.jArr0520.add(rr.jObj0520);
        } else if (rr.jObj0530 != null) {
            global.jArr0530.add(rr.jObj0530);
        } else if (rr.jObj0422 != null) {
            global.jArr0422.add(rr.jObj0422);
            //	System.out.println("0422 data:" + global.jArr0422);
        } else if (rr.jObj != null) {
            JSONArray jarr = new JSONArray();
            if (global.jArrClientHeaders.containsKey(rr.recordType)) {
                jarr = global.jArrClientHeaders.get(rr.recordType);
                jarr.add(rr.jObj);
                //		System.out.println("header data::" + global.jArrClientHeaders);
            } else {
                jarr.add(rr.jObj);
                global.jArrClientHeaders.put(rr.recordType, jarr);

            }
        }
    }

    private void processNewAccount(String bucketName, String queueFolder, S3Client s3, String outputQueueFolder,Map<String, Integer> serialNumbers) {
        if (acct != null && !acct.jsonAccount.isEmpty()) {
            String name = accNum + "_" + outFile.getName();
            String key = name;
            //code for writing the file with combined trids
            try {
                // ACF (B228) filter: when enabled, suppress the account COMBO if it fails the rule check.
                // This is flag-driven and does not affect the existing shouldPrintAccount filter above.
                if (acfFilterService.isEnabled()) {
                    String firmId = acfFilterService.lookupFirmId(currentAcctAltBranch);
                    if (!acfFilterService.shouldEmit(firmId, currentAcctAltBranch, currentAcctRep, accNum)) {
                        logger.info("ACF filter: suppressing COMBO for account={} firm={} altBranch={} rep={}",
                                accNum, firmId, currentAcctAltBranch, currentAcctRep);
                        acct = new Account();  // discard accumulated data
                        global.headerArray.clear();
                        return;
                    }
                }

                String combo = "COMBO";
                int serialNumber = serialNumbers.getOrDefault(combo, 1);
                String serialNumberStr = String.format("%08d", serialNumber);
                name = outFile.getName().replaceFirst("\\.json$", "");
                name = accNum + "_" + name + "_" + combo + "_" + serialNumberStr + ".json";
                key = name;
                loadglobalJSON();
                String content = "{\"B228_FILE\":{ \n"
                        + "\"HEADER\":" + mapper.writeValueAsString(global.headerArray) + ", \n"
                        + "\"ACCOUNTS\":[" + mapper.writeValueAsString(acct.jsonAccount) + "] \n}\n}";
                String finalKey = key;
                //	System.out.println("finalKey::" +key);
                //    logger.info("Submitting S3 upload task for account: " + accNum + " with key: " + finalKey);
                // Check queue size and possibly wait before adding more
                int queueSize = ((ThreadPoolExecutor)s3UploadExecutor).getQueue().size();
                if (queueSize > 1000) {
                    logger.info("S3 upload queue is large (" + queueSize + " tasks). Waiting for processing...");
                    // Force a flush of some tasks before continuing
                    flushS3UploadTasks(500); // Process at least 100 tasks
                }
                if(LOCAL_ENV){
                    String tempFilePath = queueFolder+key;
                    WriteFile(tempFilePath,content);
                }else submitS3UploadTask(() -> s3FileWrite(s3, bucketName, queueFolder, finalKey, content, true, outputQueueFolder));
//                submitS3UploadTask(() -> (s3, bucketName, queueFolder, finalKey, content, true, outputQueueFolder));
//                tempFolderWrite(key, true, outputQueueFolder);
                tempFolderWrite(key, true, outputQueueFolder, accNum, fmt_clientID);

            } catch (JsonProcessingException e) {
                logger.error("Error processing JSON", e);
                throw new RuntimeException("Error processing JSON", e);

            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            global.headerArray.clear();
        }
    }

    private Map<String, JSONObject> getRecordTypeMap(RecordResult rr) {
        Map<String, JSONObject> recordTypeMap = new HashMap<>();
        if (rr.jObj1010 != null) recordTypeMap.put("ETS1010", rr.jObj1010);
        if (rr.jObj1013 != null) recordTypeMap.put("ETS1013", rr.jObj1013);
        if (rr.jObj2010 != null) recordTypeMap.put("ETS2010", rr.jObj2010);
        if (rr.jObj22NN != null) recordTypeMap.put(rr.recordType, rr.jObj22NN);
        if (rr.jObj2582 != null) recordTypeMap.put("ETS2582", rr.jObj2582);
        if (rr.jObj3011 != null) recordTypeMap.put("ETS3011", rr.jObj3011);
        if (rr.jObj3020 != null) recordTypeMap.put("ETS3020", rr.jObj3020);
        if (rr.jObj3050 != null) recordTypeMap.put("ETS3050", rr.jObj3050);
        if (rr.jObj5030 != null) recordTypeMap.put(rr.recordType, rr.jObj5030);
        if (rr.jObj5040 != null) recordTypeMap.put("ETS5040", rr.jObj5040);
        if (rr.jObj5050 != null) recordTypeMap.put("ETS5050", rr.jObj5050);
        if (rr.jObj3010 != null) recordTypeMap.put("ETS3010", rr.jObj3010);
        if (rr.jObj2720 != null) recordTypeMap.put("ETS2720", rr.jObj2720);
        if (rr.jObj1120 != null) recordTypeMap.put("ETS1120", rr.jObj1120);

        if (rr.jObj2410 != null) recordTypeMap.put("ETS2410", rr.jObj2410);
        if (rr.jObj != null) recordTypeMap.put(rr.recordType, rr.jObj);
        if (rr.jObjCLDT != null) recordTypeMap.put("ETSCLDT", rr.jObjCLDT);
        return recordTypeMap;
    }

    private void processTrailer(RecordResult rr) {
        switch (rr.recordType) {
            case "ETSTTSS":
                global.jArrTTSS.add(rr.jObj);
                break;
            case "ETS9010":
                global.jArr9010.add(rr.jObj);
                break;
            case "ETS9050":
                global.jArr9050.add(rr.jObj);
                break;
            case "ETS9080":
                global.jArr9080.add(rr.jObj);
                break;
            case "ETS9090":
                global.jArr9090.add(rr.jObj);
                break;
        }
    }
    private void handleRecordType(RecordResult rr, String recordType, JSONObject jObj, Map<String, Integer> serialNumbers, String bucketName, String queueFolder, S3Client s3, String outputQueueFolder) {
        // Apply account filtering check at the beginning
        /*if (filterAccountsEnabled && (accNum == null || !validAccountNumbers.contains(accNum)))  {
            //	if (filterAccountsEnabled && (accNum != null && validAccountNumbers.contains(accNum)))  {
            // Skip processing for accounts not in filter list
            return;
        }*/

        if (acct == null) {
            return;
        }
        // Check currency suppression before processing any record
        if (currencySuppressionService.isEnabled() && !currencySuppressionService.shouldEmit(accNum, rr.currency)) {
            logger.debug("Suppressing record type {} for account={}, currency={}", recordType, accNum, rr.currency);
            return; // Skip this record - do not process
        }
        if (jObj != null) {
            //	if (!serialNumbers.containsKey(recordType)) {
            //		serialNumbers.clear();
            //	}
            try {
                // Check the record type is part of combinedTridDataList
                if (Main.combinedTridDataList.contains(recordType)) {
                    // Synchronize access to the account's JSON data
                    synchronized(acct) {
                        // Accumulate data for TRIDs in combinedTridDataList
                        // Add to account's combined data
                        if (!acct.jsonAccount.containsKey(recordType)) {
                            JSONArray jArr = new JSONArray();
                            jArr.add(jObj);
                            acct.jsonAccount.put(recordType, jArr);
                        } else {
                            JSONArray jArr = (JSONArray) acct.jsonAccount.get(recordType);
                            jArr.add(jObj);
                        }
                    }

                }  else {
                    // Split as usual
					/*if (!serialNumbers.containsKey(recordType)) {
						serialNumbers.put(recordType, 1);
					}
					int serialNumber = serialNumbers.get(recordType);
					String serialNumberStr = String.format("%08d", serialNumber);*/
                    String serialNumberStr="";
                    if (jObj.containsKey("XBASE-HDR-RECSEQ")) {
                        serialNumberStr = jObj.get("XBASE-HDR-RECSEQ").toString().trim();
                    }
                    // Ensure it's properly formatted (8 digits with leading zeros)
                    //	sequenceNumber = String.format("%08d", Integer.parseInt(sequenceNumber));
                    String name = outFile.getName().replaceFirst("\\.json$", "");
                    name = accNum + "_" + name + "_" + recordType + "_" + serialNumberStr + ".json";
                    String key = name;

                    loadglobalJSON();
                    JSONObject accountObj = new JSONObject();
                    JSONArray accountArray = new JSONArray();
                    accountArray.add(jObj);
                    accountObj.put(recordType, accountArray);
                    String content = "{\"B228_FILE\":{ \n"
                            + "\"HEADER\":" + mapper.writeValueAsString(global.headerArray) + ", \n"
                            + "\"ACCOUNTS\":[" + mapper.writeValueAsString(accountObj) + "] \n}\n}";
                    //	System.out.println("contentcheck::"+content);
                    //	logger.info("Submitting S3 upload task for account: " + accNum + " with key: " + key);
                    // Check queue size and throttle if necessary
                    if(LOCAL_ENV){
                        String tempFilePath = queueFolder+key;

                        WriteFile(tempFilePath,content);
                        return;
                    }
                    int queueSize = ((ThreadPoolExecutor)s3UploadExecutor).getQueue().size();
                    if (queueSize > 1000) { // Much lower threshold
                        logger.info("S3 upload queue is large (" + queueSize + " tasks). Waiting...");
                        while (((ThreadPoolExecutor)s3UploadExecutor).getQueue().size() > 500) { // earlier 200
                            // Process more tasks before continuing
                            flushS3UploadTasks(500); // earlier 500
                            try {
                                Thread.sleep(700); // Allow time for processing
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                    s3NonComboFileWrite(s3, bucketName, queueFolder, key, content, true, outputQueueFolder);
                    //	tempFolderWrite(key, true, outputQueueFolder);
                    //	serialNumbers.put(recordType, serialNumber + 1);
                }
            } catch (JsonProcessingException e) {
                logger.error("Error processing JSON", e);
                System.exit(404);
                //throw new RuntimeException("Error processing JSON", e);
            } catch (InterruptedException e) {
                System.exit(404);
                //	throw new RuntimeException(e);
            } finally {
                global.headerArray.clear(); // Clear once in the finally block
            }
        }
    }

    private void s3NonComboFileWrite(S3Client s3, String bucketName, String folderName, String key, String content, boolean writeIt, String reconFolder) {

        String[] underParts = key.split("_");

        String[] temp=	ebcdicFileName.split("\\\\");
        String inputFileName = temp[temp.length -1];

        // Add FILENAME to the JSON content
        String input = "\\{\"B228_FILE\":\\{ ";
        String output = "\\{\"B228_FILE\":\\{\n\"FILENAME" +
                "\" : \"" + key + "\"," +
                "\n\"ORIGIN\" : \"" +inputFileName+"_"+timestamp+ "\",";
        String contentEdit = content.replaceAll(input, output);

        // Extract client ID and account information from key
        String clientId = "";
        String accountNum = "";
        String accountHDRAct = "";
        String accountHDRBr = "";
        if (underParts.length > 1) {
            accountNum = underParts[0];
            if (!accountNum.isBlank() && accountNum.length() == 8) {
                accountHDRAct = accountNum.substring(3);
                accountHDRBr = accountNum.substring(0, 3);
            }
            // Split the second part by .
            String[] dotParts = underParts[1].split("\\.");
            if (dotParts.length > 1) {
                String segment = dotParts[1];
                clientId = segment.replaceAll("\\D+", "");
            }
        }

        // Update client ID if valid
        if (!clientId.isBlank() && clientId.length() == 3 && !clientId.equals("000")) {
            String inputCLId = "\"XBASE-HDR-CL\" : \"000\"";
            String outputCLId = "\"XBASE-HDR-CL\" : \"" + clientId + "\"";
            contentEdit = contentEdit.replace(inputCLId, outputCLId);
        }

        // Update account information if valid
        if (!accountHDRAct.isBlank() && accountHDRAct.length() == 5 && !accountHDRBr.isBlank() && accountHDRBr.length() == 3) {
            String inputHDRAct = "\"XBASE-HDR-ACT\" : \"     \"";
            String outputHDRAct = "\"XBASE-HDR-ACT\" : \"" + accountHDRAct + "\"";
            contentEdit = contentEdit.replace(inputHDRAct, outputHDRAct);

            String inputHDRBr = "\"XBASE-HDR-BR\" : \"   \"";
            String outputHDRBr = "\"XBASE-HDR-BR\" : \"" + accountHDRBr + "\"";
            contentEdit = contentEdit.replace(inputHDRBr, outputHDRBr);
        }
        addToBatch(s3, bucketName, folderName, key, contentEdit, writeIt, reconFolder);


    }
    //batching of individual non-combo jsons
    private void addToBatch(S3Client s3, String bucketName, String folderName, String key,
                            String content, boolean writeIt, String reconFolder) {
        // Create batch key based on folder
        String batchKey = folderName;

        // Create batch if it doesn't exist
        uploadBatches.computeIfAbsent(batchKey, k -> Collections.synchronizedList(new ArrayList<>()))
                .add(new BatchItem(key, content));

        // Write to recon file if needed
        if (writeIt) {
//            tempFolderWrite(key, true, reconFolder);
            tempFolderWrite(key, true, reconFolder, accNum, fmt_clientID);
        }

        // Check if batch size reached
        if (uploadBatches.get(batchKey).size() >= s3UploadBatchSize) {
            flushBatch(s3, bucketName, folderName, batchKey);
        }
    }

    private void flushBatch(S3Client s3, String bucketName, String folderName, String batchKey) {
        List<BatchItem> batchItems = uploadBatches.remove(batchKey);
        if (batchItems == null || batchItems.isEmpty()) {
            return;
        }

        int batchSize = batchItems.size();
        // String batchFileName = BATCH_PREFIX + System.currentTimeMillis() + "_" + batchSize + ".json";

        String batchFileName;
        if (batchSize == 1) {
            // Use the original filename when batch size is 1
            batchFileName = batchItems.get(0).key;
        } else {
            // Use batch prefix for multiple items
            batchFileName = BATCH_PREFIX + System.currentTimeMillis() + "_" + batchSize + ".json";
        }

        // Create batch file content
        // Create batch file content - just concatenate the contents
        try {
            StringBuilder batchContent = new StringBuilder();

            for (BatchItem item : batchItems) {
                batchContent.append(item.content);
                // Add a newline between items for readability
                batchContent.append("\n");
            }
            int queueSize = ((ThreadPoolExecutor)s3UploadExecutor).getQueue().size();
            if (queueSize > 500) { // Lower threshold than in other methods
                logger.info("S3 upload queue is large (" + queueSize + " tasks). Waiting before submitting batch...");
                flushS3UploadTasks(200); // Process some tasks before continuing
            }

            // Submit batch upload as a task
            String finalContent = batchContent.toString();
            submitS3UploadTask(() -> {
                int maxRetries = 5;
                int retryCount = 0;
                boolean success = false;
                File tempFile = null;

                while (!success && retryCount < maxRetries) {
                    try {
                        // Ensure directories exist before writing file
                        String fullPath = folderName + batchFileName;
                        File directory = new File(fullPath).getParentFile();
                        if (directory != null && !directory.exists()) {
                            if (!directory.mkdirs()) {
                                logger.warn("Failed to create directory structure: " + directory.getPath());
                            }
                        }
                        PutObjectRequest request = PutObjectRequest.builder()
                                .bucket(bucketName)
                                .key(folderName + batchFileName)
                                .build();

                        WriteFile(fullPath,finalContent);
                        if(!LOCAL_ENV){
                            tempFile = new File(fullPath);
                            s3.putObject(request, RequestBody.fromFile(tempFile));
                        }
                        logger.info("Batch file successfully created and uploaded: " + batchFileName);
                        success = true;

                        //  s3.putObject(request, RequestBody.fromString(finalContent));
                        //  logger.info("Uploaded batch of " + batchSize + " files to S3 with key: " + batchFileName);
                        // success = true;
                    } catch (Exception e) {
                        handleRetryLogic(e, retryCount, maxRetries, batchFileName);
                        retryCount++;
                    } finally {
                        if (!LOCAL_ENV && tempFile != null) {
                            try {
                                Path tempFilePath = tempFile.toPath();
                                if (Files.exists(tempFilePath)) Files.delete(tempFilePath);
                            } catch (IOException e) {
                                logger.warn("Failed to delete temporary file: " + tempFile.getPath(), e);
                            }
                        }
                    }
                }
            });
        } catch (Exception e) {
            logger.error("Failed to create batch content", e);
            // Fall back to individual uploads for this batch
            for (BatchItem item : batchItems) {
                uploadSingleFile(s3, bucketName, folderName, item.key, item.content);
            }
        }
    }

    // Method to flush all batches
    public void flushAllBatches(S3Client s3, String bucketName) {
        logger.info("Flushing all remaining batches...");
		/*for (String batchKey : new ArrayList<>(uploadBatches.keySet())) {
			flushBatch(s3, bucketName, batchKey, batchKey);
		}*/
        // Flush batches in smaller groups to prevent overwhelming the upload queue
        List<String> batchKeys = new ArrayList<>(uploadBatches.keySet());
        int totalBatchCount = batchKeys.size();

        if (totalBatchCount > 10) {
            logger.info("Large number of batches to flush (" + totalBatchCount + "). Processing in groups.");

            for (int i = 0; i < totalBatchCount; i++) {
                String batchKey = batchKeys.get(i);
                flushBatch(s3, bucketName, batchKey, batchKey);

                // After every 10 batches, check queue and potentially wait
                if ((i + 1) % 10 == 0) {
                    int queueSize = ((ThreadPoolExecutor)s3UploadExecutor).getQueue().size();
                    if (queueSize > 300) {
                        logger.info("Pausing batch flushing. Queue size: " + queueSize);
                        flushS3UploadTasks(200);
                    }
                }
            }
        } else {
            // Small number of batches, process normally
            for (String batchKey : batchKeys) {
                flushBatch(s3, bucketName, batchKey, batchKey);
            }
        }
    }

    // Handle retry logic for failed uploads
    private void handleRetryLogic(Exception e, int retryCount, int maxRetries, String key) {
        if (retryCount < maxRetries) {
            try {
                long waitTime = (long) Math.pow(2, retryCount) * 1000; // Exponential backoff
                logger.warn("Retrying upload for key: " + key + " after " + waitTime + "ms");
                Thread.sleep(waitTime);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        } else {
            logger.error("Maximum retries reached for key: " + key, e);
            throw new RuntimeException("Failed to upload file: " + key, e);
        }
    }

    // Fallback method for single file upload
    private void uploadSingleFile(S3Client s3, String bucketName, String folderName, String key, String content) {
        int queueSize = ((ThreadPoolExecutor)s3UploadExecutor).getQueue().size();
        if (queueSize > 500) {
            logger.info("S3 upload queue is large (" + queueSize + " tasks). Waiting before submitting single file...");
            flushS3UploadTasks(200);
        }
        submitS3UploadTask(() -> {
            int maxRetries = 5;
            int retryCount = 0;
            boolean success = false;

            while (!success && retryCount < maxRetries) {
                try {
                    PutObjectRequest request = PutObjectRequest.builder()
                            .bucket(bucketName)
                            .key(folderName + key)
                            .build();

                    s3.putObject(request, RequestBody.fromString(content));
                    success = true;
                } catch (Exception e) {
                    handleRetryLogic(e, retryCount, maxRetries, key);
                    retryCount++;
                }
            }
        });
    }






/* Not needed in this class
	private void addTrailersToOutputFiles(String inputFile, String trailerFile, S3Client s3, String bucketName, String outputQueueFolder) {

		logger.info("Starting addTrailersToOutputFiles()...");
		try {


	    	String name = new File(trailerFile).getName();
	    	String key = name;

	    	JSONObject trailerJSON = new JSONObject();
	    	trailerJSON.put("B228", new File(inputFile).getName());
	    	trailerJSON.put("TRAILER-TOTAL" , B228TrailerRecords);
	    	trailerJSON.put("NUMBER-OF-RECORDS" , totalRecords);
	    	JSONObject trailerFileJSON = new JSONObject();
	    	trailerFileJSON.put("FILE" , trailerJSON);

	    	String content = mapper.writeValueAsString(trailerFileJSON);

	    	s3FileWrite(s3, bucketName, outputQueueFolder, key, content);

		} catch(Exception e) {
			logger.error("An error occurred", e);
		}
		logger.info("End of addTrailersToOutputFiles()!");
	}
*/

    private void loadJSONObjectForAccount(List<Future<RecordResult>> futuresList) {

        logger.info("Starting loadJSONObjectForAccount()...");
        for (Future<RecordResult> aFuture : futuresList) {

            try {

                RecordResult rr = aFuture.get();

                // (0) Is New Acct?

                if (rr.isHeader) {

                    if (rr.jObj0000 != null) {
                        global.jArr0000.add(rr.jObjXbase0000);
                        global.jArr0000.add(rr.jObj0000);
                    } else if (rr.jObj0420 != null) {
                        global.jArr0420.add(rr.jObj0420);
                    } else if (rr.jObj0520 != null) {
                        global.jArr0520.add(rr.jObj0520);
                    } else if (rr.jObj0530 != null) {
                        global.jArr0530.add(rr.jObj0530);
                    } else if (rr.jObj0422 != null) {
                        global.jArr0422.add(rr.jObj0422);
                        //	System.out.println("Test0422:"+ global.jArr0422);
                    } else if (rr.jObj != null) {
                        JSONArray jarr = new JSONArray();
                        if (global.jArrClientHeaders.containsKey(rr.recordType)) {
                            jarr = global.jArrClientHeaders.get(rr.recordType);
                            jarr.add(rr.jObj);

                        } else {
                            jarr.add(rr.jObj);
                            if (Main.separateTridDataList.contains(rr.recordType)) {
                                sperateTridData.put(rr.recordType, jarr);
                            } else if (Main.inputFolderTridDataList.contains(rr.recordType)) {
                                inputFolderTridData.put(rr.recordType, jarr);
                            } else {
                                global.jArrClientHeaders.put(rr.recordType, jarr);
                            }
                        }
                    }
                }

                if (accNum.equals("") || !accNum.equals(rr.recAccNum)) {
                    isNewAcct = true;
                }


                if (rr.accNumCheck) {

                    if (isNewAcct) { //should have ETSXBASENEW
                        if (acct != null) {
                            if (acct.jArr1010.size() > 0) {
                                acct.jsonAccount.put("ETS1010", acct.jArr1010);
                            }
                            if (acct.jArr1013.size() > 0) {
                                acct.jsonAccount.put("ETS1013", acct.jArr1013);
                            }
                            if (acct.jArr2010.size() > 0) {
                                acct.jsonAccount.put("ETS2010", acct.jArr2010);
                            }
                            if (acct.jMap22NN.size() > 0) {
                                for (Entry<String, JSONArray> entry : acct.jMap22NN.entrySet()) {
                                    acct.jsonAccount.put(entry.getKey(), entry.getValue());
                                }
                            }
                            if (acct.jArr2582.size() > 0) {
                                acct.jsonAccount.put("ETS2582", acct.jArr2582);
                            }
                            if (acct.jArr2410.size() > 0) {
                                acct.jsonAccount.put("ETS2410", acct.jArr2410);
                            }
                            if (acct.jMap503.size() != 0) {
                                for (Entry<String, JSONArray> entry : acct.jMap503.entrySet()) {
                                    acct.jsonAccount.put(entry.getKey(), entry.getValue());
                                }
                            }
                            if (acct.jArr5050.size() > 0) {
                                acct.jsonAccount.put("ETS5050", acct.jArr5050);
                            }
                            if (acct.jArr5040.size() > 0) {
                                acct.jsonAccount.put("ETS5040", acct.jArr5040);
                            }
                            if (acct.jArr3010.size() > 0) {
                                acct.jsonAccount.put("ETS3010", acct.jArr3010);
                            }
                            if (acct.jArr2720.size() > 0) {
                                acct.jsonAccount.put("ETS2720", acct.jArr2720);
                            }
                            if (acct.jArr1120.size() > 0) {
                                acct.jsonAccount.put("ETS1120", acct.jArr1120);
                            }
                            if (acct.jArr3011.size() > 0) {
                                acct.jsonAccount.put("ETS3011", acct.jArr3011);
                            }
                            if (acct.jArr3020.size() > 0) {
                                acct.jsonAccount.put("ETS3020", acct.jArr3020);
                            }
                            if (acct.jArr3050.size() > 0) {
                                acct.jsonAccount.put("ETS3050", acct.jArr3050);
                            }

                            if (acct.jArrcldt.size() > 0) {
                                acct.jsonAccount.put("ETSCLDT", acct.jArrcldt);
                            }
                            if (acct.extraRecords.size() != 0) {
                                for (Entry<String, JSONArray> entry : acct.extraRecords.entrySet()) {
                                    acct.jsonAccount.put(entry.getKey(), entry.getValue());
                                }
                            }
                            if (firstAcct) {
                                writer2.write(mapper.writeValueAsString(acct.jsonAccount));
                                firstAcct = false;
                            } else {
                                writer2.write(", \n" + mapper.writeValueAsString(acct.jsonAccount));
                            }
                        } else {
                            if (!loadedHeader) {
                                writeHeaderData(writer2);
                            }
                        }

                        acct = new Account();

                        accNum = rr.recAccNum; // update accNum
                        fmt_clientID = rr.clientId;
                        // Check if account should be suppressed
                        String printAction = shouldPrintAccount(accNum, fmt_clientID);
                        if ("S".equalsIgnoreCase(printAction)) {
                            logger.info("Suppressing account: " + accNum + " for client: " + fmt_clientID);
                            acct = null; // Ensure the account object is not processed further
                            continue; // Skip this account
                        }
                        // Process account normally if printAction is "P"
                    }
                }
                if (!rr.isHeader && !rr.isTrailer) {
                    // Check currency suppression before adding any record
                    if (currencySuppressionService.isEnabled() && !currencySuppressionService.shouldEmit(accNum, rr.currency)) {
                        logger.debug("Suppressing record type {} for account={}, currency={}", rr.recordType, accNum, rr.currency);
                        continue; // Skip this record - do not add to account
                    }

                    if (rr.jObj1010 != null) {
                        acct.jArr1010.add(rr.jObj1010);
                    } else if (rr.jObj1013 != null) {
                        acct.jArr1013.add(rr.jObj1013);
                    } else if (rr.jObj2010 != null) {
                        acct.jArr2010.add(rr.jObj2010);
                    } else if (rr.jObj22NN != null) {
                        JSONArray jarr = new JSONArray();
                        if (acct != null) {
                            if (acct.jMap22NN.containsKey(rr.recordType)) {
                                jarr = acct.jMap22NN.get(rr.recordType);
                                jarr.add(rr.jObj22NN);
                            } else {
                                jarr.add(rr.jObj22NN);
                            }
                            acct.jMap22NN.put(rr.recordType, jarr);
                        }
                    } else if (rr.jObj2582 != null) {
                        acct.jArr2582.add(rr.jObj2582);
                    } else if (rr.jObj3011 != null) {
                        acct.jArr3011.add(rr.jObj3011);
                    } else if (rr.jObj3020 != null) {
                        acct.jArr3020.add(rr.jObj3020);
                    } else if (rr.jObj3050 != null) {
                        acct.jArr3050.add(rr.jObj3050);
                    } else if (rr.jObj5030 != null) {
                        JSONArray jarr = new JSONArray();
                        if (acct != null) {
                            if (acct.jMap503.containsKey(rr.recordType)) {
                                jarr = acct.jMap503.get(rr.recordType);
                                jarr.add(rr.jObj5030);
                            } else {
                                jarr.add(rr.jObj5030);
                            }
                            acct.jMap503.put(rr.recordType, jarr);
                        }
                    } else if (rr.jObj5040 != null) {
                        acct.jArr5040.add(rr.jObj5040);
                    } else if (rr.jObj5050 != null) {
                        acct.jArr5050.add(rr.jObj5050);
                    } else if (rr.jObj3010 != null) {
                        //					System.out.println("method loadJsonObj4Acc::" +rr.jObj3010);
                        acct.jArr3010.add(rr.jObj3010);
                    } else if (rr.jObj2720 != null) {
                        acct.jArr2720.add(rr.jObj2720);
                    } else if (rr.jObj2410 != null) {
                        acct.jArr2410.add(rr.jObj2410);
                    }else if (rr.jObj1120 != null) {
                        acct.jArr1120.add(rr.jObj1120);
                    }  else if (rr.jObj != null) {
                        JSONArray jarr = new JSONArray();
                        if (acct != null) {
                            if (acct.extraRecords.containsKey(rr.recordType)) {
                                jarr = acct.extraRecords.get(rr.recordType);
                                jarr.add(rr.jObj);
                            } else {
                                jarr.add(rr.jObj);
                            }
                            acct.extraRecords.put(rr.recordType, jarr);
                        }

                    } else if (rr.jObjCLDT != null) {
                        acct.jArrcldt.add(rr.jObjCLDT);
                    }
                }

                if (rr.isTrailer && rr.jObj != null) {
                    if (rr.recordType.equals("ETSTTSS")) {
                        global.jArrTTSS.add(rr.jObj);
                    } else if (rr.recordType.equals("ETS9010")) {
                        global.jArr9010.add(rr.jObj);
                    } else if (rr.recordType.equals("ETS9050")) {
                        global.jArr9050.add(rr.jObj);
                    } else if (rr.recordType.equals("ETS9080")) {
                        global.jArr9080.add(rr.jObj);
                    } else if (rr.recordType.equals("ETS9090")) {
                        global.jArr9090.add(rr.jObj);
                    }
                }


                isNewAcct = false;

            } catch (Exception e) {
                logger.error("An error occurred", e);
            }
        }
        logger.info("End of loadJSONObjectForAccount()!");
    }


    private JSONArray loadTrailerJSON() {

        logger.info("Starting loadTrailerJSON()...");
        JSONObject jsontrailer = new JSONObject();

        if (global.jArrTTSS.size() > 0) {
            jsontrailer.put("ETSTTSS", global.jArrTTSS);
        }

        if (global.jArr9010.size() > 0) {
            jsontrailer.put("ETS9010", global.jArr9010);
        }

        if (global.jArr9050.size() > 0) {
            jsontrailer.put("ETS9050", global.jArr9050);
        }

        if (global.jArr9080.size() > 0) {
            jsontrailer.put("ETS9080", global.jArr9080);
        }

        if (global.jArr9090.size() > 0) {
            jsontrailer.put("ETS9090", global.jArr9090);
        }

        global.trailerArray.add(jsontrailer);
        logger.info("End of loadTrailerJSON()!");
        return global.trailerArray;
    }


    private void writeHeaderData(BufferedWriter writer2) throws InterruptedException {

        logger.info("Starting writeHeaderData()...");
        loadedHeader = true;
        firstAcct = true;
        loadglobalJSON();
        try {
            writer2.write("{\"B228_FILE\":{ \n"
                    + "\"HEADER\":" + mapper.writeValueAsString(global.headerArray) + ", \n"
//					+ "\"CLIENT_HEADER\":"+mapper.writeValueAsString(global.clientArray)+", \n"
                    + "\"ACCOUNTS\":[ \n");
            //		System.out.println("writeHeaderData:" +writer2);
        } catch (IOException e) {

            logger.error("An error occurred", e);
        }
        logger.info("End of writeHeaderData()!");
    }


    private void loadglobalJSON() throws InterruptedException {

        //	logger.info("Starting loadglobalJSON()...");
        // Adding global Entries by order
        JSONObject json0000 = new JSONObject();
        if(!global.jArr0000.isEmpty()) {
            json0000.put("ETSXBASE", global.jArr0000.get(0));
            json0000.put("ETS0000", global.jArr0000.get(1));
        }
        global.headerArray.add(json0000);
        //	System.out.println("global.headerArray:" + global.headerArray);
        //	global.jArr0000.clear();

        JSONObject jsonClientHeader = new JSONObject();
        if (global.jArr0420.size() > 0) {

            if (Main.separateTridDataList.contains("ETS0420")) {
                sperateTridData.put("ETS0420", global.jArr0420);
            } else {
                jsonClientHeader.put("ETS0420", global.jArr0420);
            }

        }

        if (global.jArr0422.size() > 0) {
            if (Main.separateTridDataList.contains("ETS0422")) {
                sperateTridData.put("ETS0422", global.jArr0422);
            } else {
                jsonClientHeader.put("ETS0422", global.jArr0422);
            }

        }

        if (global.jArr0520.size() > 0) {
            if (Main.separateTridDataList.contains("ETS0520")) {
                sperateTridData.put("ETS0520", global.jArr0520);
            } else {
                jsonClientHeader.put("ETS0520", global.jArr0520);
            }

        }

        if (global.jArr0530.size() > 0) {
            if (Main.separateTridDataList.contains("ETS0530")) {
                sperateTridData.put("ETS0530", global.jArr0530);
            } else {
                jsonClientHeader.put("ETS0530", global.jArr0530);
            }
        }

        if (global.jArrClientHeaders.size() != 0) {
            for (Map.Entry<String, JSONArray> entry : global.jArrClientHeaders.entrySet()) {

                //	logger.info("entry.getKey() : " + entry.getKey());
                if (Main.separateTridDataList.contains(entry.getKey())) {
                    sperateTridData.put(entry.getKey(), entry.getValue());
                } else {
                    jsonClientHeader.put(entry.getKey(), entry.getValue());
                }
            }
        }
        global.clientArray.add(jsonClientHeader);
        //	logger.info("End of loadglobalJSON()!");
    }


   /* private void writerSperateTridList(String bucketName, String headerQueueFolder, String holdQueueFolder, String queueFolder, String headerInputFolder, S3Client s3) throws InterruptedException {

        logger.info("Starting writerSperateTridList()...");
        loadglobalJSON();
        for (Map.Entry<String, JSONArray> entry : sperateTridData.entrySet()) {

            String headerName = entry.getKey();
            //    logger.info("headerName : " + headerName);
            logger.info("Processing headerName: " + headerName + " with " + entry.getValue().size() + " entries");

            String inputFilename = outFile.getName();
            //    System.out.println("Header file name" +inputFilename);


            String key = headerName + "_" + inputFilename;
            //  loadglobalJSON();
            try {
                String content = "{\"B228_FILE\":{ \n" + "\"HEADER\":" + mapper.writeValueAsString(global.headerArray)
                        + ", \n" + "\"CLIENT_HEADER\":[ { \n" + "\"" + entry.getKey() + "\":"
                        + mapper.writeValueAsString((entry.getValue())) + "\n   } ]\n }}";
                //	System.out.println("sepTridlistmethod" +content);

                boolean writeIt = false;
                String reconOutputNotNeeded = " ";
                if(LOCAL_ENV) {
                    WriteFile(key, content);
                }else {
                    if (Main.holdTridDataList.contains(headerName)) {
                        s3FileWrite(s3, bucketName, holdQueueFolder, key, content, writeIt, reconOutputNotNeeded);
                    } else if (Main.inputFolderTridDataList.contains(headerName)) {
                        s3FileWrite(s3, bucketName, headerInputFolder, key, content, writeIt, reconOutputNotNeeded);
                    } else if (Main.separateTridDataList.contains(headerName)) {
                        s3FileWrite(s3, bucketName, headerQueueFolder, key, content, writeIt, reconOutputNotNeeded);
                    } else {
                        s3FileWrite(s3, bucketName, queueFolder, key, content, writeIt, reconOutputNotNeeded);
                    }
                }
                //   global.headerArray.clear();

            } catch (IOException e) {
                logger.error("An error occurred", e);
                System.exit(404);
                throw new RuntimeException("Failed to write separate TRID files: " + e.getMessage(), e);
            }
        }
        global.headerArray.clear();
        logger.info("End of writerSperateTridList()!");
    }*/

    //Changes to avoid textbuffer memory issue for large files greater than 2 GB.Now it will stream instead of loading entire content into memory as string
    private void writerSperateTridList(String bucketName, String headerQueueFolder, String holdQueueFolder, String queueFolder, String headerInputFolder, S3Client s3) throws InterruptedException {

        logger.info("Starting writerSperateTridList()...");
        loadglobalJSON();

        String[] temp = ebcdicFileName.split("\\\\");
        String inputFileName = temp[temp.length - 1];

        for (Map.Entry<String, JSONArray> entry : sperateTridData.entrySet()) {
            String headerName = entry.getKey();
            JSONArray data = entry.getValue();
            logger.info("Processing headerName: " + headerName + " with " + data.size() + " entries");

            String inputFilename = outFile.getName();
            String key = headerName + "_" + inputFilename;
            Path tempFile = null;

            try {
                // Stream to temp file to avoid 2GB buffer limit
                tempFile = Files.createTempFile("trid_", ".json");

                try (OutputStream out = Files.newOutputStream(tempFile);
                     JsonGenerator gen = mapper.getFactory().createGenerator(out)) {

                    gen.writeStartObject();
                    gen.writeFieldName("B228_FILE");
                    gen.writeStartObject();

                    // Add FILENAME and ORIGIN keys
                    gen.writeStringField("FILENAME", key);
                    gen.writeStringField("ORIGIN", inputFileName + "_" + timestamp);

                    gen.writeFieldName("HEADER");
                    mapper.writeValue(gen, global.headerArray);
                    gen.writeFieldName("CLIENT_HEADER");
                    gen.writeStartArray();
                    gen.writeStartObject();
                    gen.writeFieldName(headerName);
                    mapper.writeValue(gen, data);
                    gen.writeEndObject();
                    gen.writeEndArray();
                    gen.writeEndObject();
                    gen.writeEndObject();
                }

                boolean writeIt = false;
                String reconOutputNotNeeded = " ";

                if(LOCAL_ENV) {
                    // For local env, read and write file
                    Path destination = Path.of(holdQueueFolder+key);
                    Files.copy(tempFile, destination, StandardCopyOption.REPLACE_EXISTING);
                } else {
                    // For S3, stream file directly without loading to memory
                    s3FileWriteFromPath(s3, bucketName, headerQueueFolder, holdQueueFolder, queueFolder, headerInputFolder,
                            key, headerName, tempFile, inputFileName);
                }

            } catch (IOException e) {
                logger.error("An error occurred", e);
                System.exit(404);
                throw new RuntimeException("Failed to write separate TRID files: " + e.getMessage(), e);
            } finally {
                if (tempFile != null) {
                    try {
                        Files.deleteIfExists(tempFile);
                    } catch (IOException ignored) {}
                }
            }
        }
        global.headerArray.clear();
        logger.info("End of writerSperateTridList()!");
    }

/*     private void WriteFile(String filePath, String content){
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write(content);
            //	System.out.println("File written successfully!");
        } catch (IOException e) {
            e.printStackTrace();
        }
    } */

    private void WriteFile(String filePath, String content) {
        try {
            File file = new File(filePath);
            Path parent = file.toPath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            try (FileWriter writer = new FileWriter(file)) {
                writer.write(content);
            }
        } catch (IOException e) {
            logger.error("Failed to write file: " + filePath, e);
            throw new RuntimeException("Failed to write file: " + filePath, e);
        }
    }


    // New method to handle S3 upload from file path directly for header trids(e.g. 0510,0422,0420,0530) avoiding the out of memory error for file size large than 2 GB
    private void s3FileWriteFromPath(S3Client s3, String bucketName, String headerQueueFolder,
                                     String holdQueueFolder, String queueFolder, String headerInputFolder,
                                     String key, String headerName, Path tempFile, String inputFileName) throws IOException {

        int maxRetries = 5;
        int retryCount = 0;
        boolean success = false;

        while (!success && retryCount < maxRetries) {
            File tempFileObj = null;
            try {
                // Determine target folder
                String targetFolder = queueFolder;
                if (Main.holdTridDataList.contains(headerName)) {
                    targetFolder = holdQueueFolder;
                } else if (Main.inputFolderTridDataList.contains(headerName)) {
                    targetFolder = headerInputFolder;
                } else if (Main.separateTridDataList.contains(headerName)) {
                    targetFolder = headerQueueFolder;
                }

                // Upload file directly from temp file without loading entire content into memory
                tempFileObj = tempFile.toFile();

                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(targetFolder + key)
                        .build();

                // Stream the file directly to S3
                s3.putObject(request, RequestBody.fromFile(tempFileObj));
                logger.info("File uploaded to S3: " + targetFolder + key);
                success = true;

            } catch (Exception e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    logger.error("Maximum retries reached for key: " + key, e);
                    throw new IOException("Failed to upload file: " + key, e);
                }
                try {
                    Thread.sleep(1000 * retryCount);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Upload interrupted", ie);
                }
            }
        }
    }


/*    private void tempFolderWrite(String key, boolean writeIt, String reconFolder, String accNum, String fmt_clientID) {
        String reconFileName = outFile.getName().replaceFirst(".json", ".recon_rpt");
        String reconAccountListFileName = outFile.getName().replaceFirst(".json", ".reconAccountList_rpt");

        //System.out.println("recon file name::" + reconFileName);

        // Create directory if it doesn't exist
        File directory = new File(reconFolder);
        if (!directory.exists()) {
            directory.mkdirs();
            logger.info("Created directory: " + reconFolder);
        }

        // Make sure path ends with a separator
        if (!reconFolder.endsWith(File.separator)) {
            reconFolder = reconFolder + File.separator;
        }

        if (writerRecon == null) {
            try {
                writerRecon = new BufferedWriter(new FileWriter(reconFolder + reconFileName, false));
                writerRecon.write("filename,accountnumber,clientid,status,partytype,errorjson,runtype,historicaltrigger");
                writerRecon.newLine();
            } catch (Exception e) {
                logger.error("Exception creating recon file : ", e);
                throw new RuntimeException(e);
            }
        }

        if (writeIt) {
            try {
                if (!writtenKeys.contains(key)) {
                    writerRecon.write(key);
                    writerRecon.write("," + accNum + "," + fmt_clientID + ",,,,,");
                    writerRecon.newLine();
                    writtenKeys.add(key);
                }
            } catch (IOException e) {
                logger.error("Exception writing files: ", e);
                throw new RuntimeException(e);
            }
        }

        //Writing the Recon Account List Report
        if (writerReconAcctList == null) {
            try {
                writerReconAcctList = new BufferedWriter(new FileWriter(reconFolder + reconAccountListFileName, false));
///				writerReconAcctList.write("accountnumber,clientid,partytype,brx_status,extraction_status");
////			writerReconAcctList.write("accountnumber,status");
                writerReconAcctList.write("accountnumber,clientid,status,runtype,partytype,brxstatus,extractionstatus,historicaltrigger");
                writerReconAcctList.newLine();
            } catch (Exception e) {
                logger.error("Exception creating recon Account List file : ", e);
                throw new RuntimeException(e);
            }
        }

        if (writeIt) {
            if (key.contains("COMBO")) {
                try {
                    // check if we’ve already written this accNum
                    if (!writtenAccNums.contains(accNum)) {
////		            	writerReconAcctList.write(accNum + ",") ;
////		            	writerReconAcctList.write(accNum + "," + fmt_clientID + ",,,");
                        writerReconAcctList.write(accNum + "," + fmt_clientID + ",,,,,,");
                        writerReconAcctList.newLine();
                        // store accNum as already written
                        writtenAccNums.add(accNum);
                    }
                } catch (IOException e) {
                    logger.error("Exception writing Account List files: ", e);
                    throw new RuntimeException(e);
                }
            }
        }


    }*/

    private void histforicalFileCreate(String reconFolder, String fmt_clientID) {
        if (outFile == null || reconFolder == null || reconFolder.isBlank()) {
            return;
        }

        String normalizedFolder = reconFolder.endsWith(File.separator)
                ? reconFolder
                : reconFolder + File.separator;

        File directory = new File(normalizedFolder);
        if (!directory.exists() && directory.mkdirs()) {
            logger.info("Created directory: " + normalizedFolder);
        }

        reconFolderPath = normalizedFolder;

////LINE UNDER NEEDS TO BE HISTORICAL FILE NAME
        String historicalFileName = "HISTORICAL_BY_SECID_20" + hist_yy + "_"+ hist_mm;

        Path filePath = Paths.get(reconFolderPath + historicalFileName);

        try {
            if (writerHistorical == null) {
                // Atomically create the file; fails if it already exists
                Files.createFile(filePath);


//            	System.out.print("***I AM HERE***");
                writerHistorical = new BufferedWriter(new FileWriter(reconFolderPath + historicalFileName, false));
                writerHistorical.write("KEY,JSON_DATA");
                writerHistorical.newLine();
                writerHistorical.write("000|000|00000|000|0000000|DummyRecord,{\"MONTH\":\"00\",\"YEAR\":\"0000\"}");
                writerHistorical.newLine();

                writerHistorical.flush();
                writerHistorical.close();

            }
        } catch (FileAlreadyExistsException e) {
            // File already exists, skip writing
            //logger.info("Historical file already exists, skipping creation: " + filePath);
        } catch (IOException e) {
            logger.error("Exception creating Historical files : ", e);
            throw new RuntimeException(e);
        }
    }




    //This method ensure that recon reports with headers are generated even when none of account from filter files are present in b228 file
    private void ensureReconFilesInitialized(String reconFolder) {
        if (outFile == null || reconFolder == null || reconFolder.isBlank()) {
            return;
        }

        String normalizedFolder = reconFolder.endsWith(File.separator)
                ? reconFolder
                : reconFolder + File.separator;

        File directory = new File(normalizedFolder);
        if (!directory.exists() && directory.mkdirs()) {
            logger.info("Created directory: " + normalizedFolder);
        }

        reconFolderPath = normalizedFolder;

        String reconFileName = outFile.getName().replaceFirst("\\.json$", ".recon_rpt");
        String reconAccountListFileName = outFile.getName().replaceFirst("\\.json$", ".reconAccountList_rpt");

        try {
            if (writerRecon == null) {
                writerRecon = new BufferedWriter(new FileWriter(reconFolderPath + reconFileName, false));
                writerRecon.write("filename,accountnumber,clientid,status,partytype,partyid,partyparentid,errorjson,runtype,historicaltrigger");
                writerRecon.newLine();
            }
            if (writerReconAcctList == null) {
                writerReconAcctList = new BufferedWriter(new FileWriter(reconFolderPath + reconAccountListFileName, false));
                writerReconAcctList.write("accountnumber,clientid,status,runtype,partytype,partyid,partyparentid,brxstatus,extractionstatus,historicaltrigger");
                writerReconAcctList.newLine();
            }
        } catch (IOException e) {
            logger.error("Exception creating recon files : ", e);
            throw new RuntimeException(e);
        }
    }

    private void tempFolderWrite(String key, boolean writeIt, String reconFolder, String accNum, String fmt_clientID) {
        ensureReconFilesInitialized(reconFolder);
        histforicalFileCreate(reconFolder, fmt_clientID);

        if (!writeIt) {
            return;
        }

        try {
            if (writerRecon != null && !writtenKeys.contains(key)) {
                writerRecon.write(key);
                writerRecon.write("," + accNum + "," + fmt_clientID + ",,,,,,,");

                writerRecon.newLine();
                writtenKeys.add(key);
            }

            if (writerReconAcctList != null && key.contains("COMBO") && !writtenAccNums.contains(accNum)) {
                writerReconAcctList.write(accNum + "," + fmt_clientID + ",,,,,,,,");
                writerReconAcctList.newLine();
                writtenAccNums.add(accNum);
            }
        } catch (IOException e) {
            logger.error("Exception writing Recon files: ", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Write suppressed accounts report (accountnumber,currency) and upload to S3
     */
    private void writeSuppressedAccountsReport(String reconFolder, S3Client s3, String bucketName, String outputQueueFolder) {
        if (outFile == null || reconFolder == null || reconFolder.isBlank()) {
            logger.warn("Cannot write suppressed accounts report - missing outFile or reconFolder");
            return;
        }

        String normalizedFolder = reconFolder.endsWith(File.separator)
                ? reconFolder
                : reconFolder + File.separator;

        File directory = new File(normalizedFolder);
        if (!directory.exists() && directory.mkdirs()) {
            logger.info("Created directory: " + normalizedFolder);
        }

        String suppressedFileName = outFile.getName().replaceFirst("\\.json$", ".suppressed");
        String suppressedFilePath = normalizedFolder + suppressedFileName;

        try (BufferedWriter writer = new BufferedWriter(new FileWriter(suppressedFilePath, false))) {
            // Write header
            writer.write("accountnumber,currency");
            writer.newLine();

            // Get all suppressed accounts from the service
            Map<String, String> suppressedAccounts = currencySuppressionService.getSuppressedAccounts();

            int suppressedCount = 0;
            for (String key : suppressedAccounts.keySet()) {
                String achPrintInd = suppressedAccounts.get(key);
                // Only write if suppressed (ACH-PRINT-IND = "0")
                if ("0".equals(achPrintInd)) {
                    String[] parts = key.split("\\|");
                    if (parts.length == 2) {
                        writer.write(parts[0] + "," + parts[1]);
                        writer.newLine();
                        suppressedCount++;
                    }
                }
            }
            writer.flush();
            logger.info("Suppressed accounts report written successfully to: " + suppressedFilePath +
                    " with " + suppressedCount + " suppressed accounts");

            // Upload to S3
            if (s3 != null && bucketName != null && !bucketName.isBlank() && outputQueueFolder != null) {
                uploadSuppressedAccountsToS3(s3, bucketName, outputQueueFolder, suppressedFilePath);
            } else {
                logger.warn("S3 upload skipped - missing S3 client or bucket configuration");
            }
        } catch (IOException e) {
            logger.error("Error writing suppressed accounts report: " + suppressedFilePath, e);
        }
    }

    /**
     * Upload suppressed accounts report to S3
     */
    private void uploadSuppressedAccountsToS3(S3Client s3, String bucketName, String outputQueueFolder, String suppressedFilePath) {
        int maxRetries = 5;
        int retryCount = 0;
        boolean success = false;

        File suppressedFile = new File(suppressedFilePath);
        if (!suppressedFile.exists()) {
            logger.warn("Suppressed accounts file does not exist: " + suppressedFilePath);
            return;
        }

        String suppressedFileName = suppressedFile.getName();
        String s3Key = outputQueueFolder + suppressedFileName;

        while (!success && retryCount < maxRetries) {
            try {
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(s3Key)
                        .build();

                s3.putObject(request, RequestBody.fromFile(suppressedFile));
                logger.info("Suppressed accounts file uploaded to S3: s3://" + bucketName + "/" + s3Key);
                success = true;
            } catch (Exception e) {
                retryCount++;
                if (retryCount >= maxRetries) {
                    logger.error("Maximum retries reached uploading suppressed accounts file to S3", e);
                    throw new RuntimeException("Failed to upload suppressed accounts file to S3: " + suppressedFilePath, e);
                }
                try {
                    long waitTime = (long) Math.pow(2, retryCount) * 1000; // Exponential backoff
                    logger.warn("Retrying suppressed accounts file upload after " + waitTime + "ms. Attempt: " + retryCount);
                    Thread.sleep(waitTime);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    logger.error("Thread interrupted during suppress file upload retry", ie);
                    throw new RuntimeException("Upload interrupted", ie);
                }
            }
        }
    }

    private void s3FileWrite(S3Client s3, String bucketName, String folderName, String key, String content, boolean writeIt, String reconFolder) {
        int maxRetries = 5;
        int retryCount = 0;
        boolean success = false;

/*
		try {
//		if (key.contains("2MA00922") || key.contains("1AB21577") || key.contains("1AB22924") || key.contains("1AB21024") || key.contains("1BC56843"))
//			{
		        BufferedWriter writer59 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(folderName + key), "UTF-8"));
				String input = "\\{\"B228_FILE\":\\{ ";
				String output = "\\{\"B228_FILE\":\\{\n\"FILENAME\" : \"" + key + "\",";
				String contentEdit = content.replaceAll(input, output);
				writer59.write(contentEdit);
				writer59.flush();
				writer59.close();
//			}
		} catch (Exception e) {
			logger.error("Exception writing files : ", e);
			throw new RuntimeException(e);
		}
*/


        while (!success && retryCount < maxRetries) {
            String[] underParts = key.split("_");

            String[] temp=	ebcdicFileName.split("\\\\");
            String inputFileName = temp[temp.length -1];
            File tempFile = null;
            String tempFilePath =folderName+key;
            try {
                // Add FILENAME to the JSON content
                String input = "\\{\"B228_FILE\":\\{ ";
                String output = "\\{\"B228_FILE\":\\{\n\"FILENAME" +
                        "\" : \"" + key + "\"," +
                        "\n\"ORIGIN\" : \"" +inputFileName+"_"+timestamp+ "\",";
                String contentEdit = content.replaceAll(input, output);

                // Extract client ID and account information from key
                String clientId = "";
                String accountNum = "";
                String accountHDRAct = "";
                String accountHDRBr = "";
                if (underParts.length > 1) {
                    accountNum = underParts[0];
                    if (!accountNum.isBlank() && accountNum.length() == 8) {
                        accountHDRAct = accountNum.substring(3);
                        accountHDRBr = accountNum.substring(0, 3);
                    }
                    // Split the second part by .
                    String[] dotParts = underParts[1].split("\\.");
                    if (dotParts.length > 1) {
                        String segment = dotParts[1];
                        clientId = segment.replaceAll("\\D+", "");
                    }
                }

                // Update client ID if valid
                if (!clientId.isBlank() && clientId.length() == 3 && !clientId.equals("000")) {
                    String inputCLId = "\"XBASE-HDR-CL\" : \"000\"";
                    String outputCLId = "\"XBASE-HDR-CL\" : \"" + clientId + "\"";
                    contentEdit = contentEdit.replace(inputCLId, outputCLId);
                }

                // Update account information if valid
                if (!accountHDRAct.isBlank() && accountHDRAct.length() == 5 && !accountHDRBr.isBlank() && accountHDRBr.length() == 3) {
                    String inputHDRAct = "\"XBASE-HDR-ACT\" : \"     \"";
                    String outputHDRAct = "\"XBASE-HDR-ACT\" : \"" + accountHDRAct + "\"";
                    contentEdit = contentEdit.replace(inputHDRAct, outputHDRAct);

                    String inputHDRBr = "\"XBASE-HDR-BR\" : \"   \"";
                    String outputHDRBr = "\"XBASE-HDR-BR\" : \"" + accountHDRBr + "\"";
                    contentEdit = contentEdit.replace(inputHDRBr, outputHDRBr);
                }

                // Upload to S3 with proper request configuration
                PutObjectRequest request = PutObjectRequest.builder()
                        .bucket(bucketName)
                        .key(folderName + key)
                        .build();
                // Temporarily writing file to the local, which then will be uploaded to s3 by passing the local file to
                // the put object
                WriteFile(tempFilePath,contentEdit);
                if(!LOCAL_ENV){
                    tempFile = new File(tempFilePath);
                    s3.putObject(request, RequestBody.fromFile(tempFile));
                }
                //	logger.info("File uploaded to " + bucketName + " folder: " + folderName + " with key: " + key);
                success = true;
            } catch (Exception e) {
                retryCount++;
                if (e instanceof software.amazon.awssdk.core.exception.AbortedException) {
                    logger.warn("Upload interrupted for key: " + key + ". Retry attempt: " + retryCount);
                    if (retryCount >= maxRetries) {
                        logger.error("Maximum retries reached for key: " + key, e);
                        throw e;
                    }
                    try {
                        // Exponential backoff
                        Thread.sleep(1000 * retryCount);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        logger.error("Thread interrupted during retry wait", ie);
                        throw new RuntimeException("Upload interrupted and cannot be completed", ie);
                    }
                } else {
                    logger.error("Exception from S3Client method for key: " + key, e);
                    throw new RuntimeException("Failed to upload file: " + key, e);
                }
            }finally {
                try {
                    //Deleting the file if we are not running the local, as file is not required in the non-local system
                    if(!LOCAL_ENV) {
                        Path tempFilePathPath = Paths.get(tempFilePath);
                        if (Files.exists(tempFilePathPath)) Files.delete(tempFilePathPath);
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

    }

    private void lookupTableWrite(S3Client s3, String bucketName, String folderName, String key, String content) throws IOException {
        File directory = new File(folderName);
        if (!directory.exists()) {
            directory.mkdirs();
            logger.info("Created directory: " + folderName);
        }

        if (!folderName.startsWith(File.separator)) {
            folderName = File.separator + folderName;
        }

        // Make sure path ends with a separator
        if (!folderName.endsWith(File.separator)) {
            folderName = folderName + File.separator;
        }

        File file = new File(folderName + key);
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(file, false))) {
            writer.write(content);
            logger.info("Successfully wrote to file: " + file.getAbsolutePath());
        } catch (Exception e) {
            logger.error("Exception writing Look Table File : ", e);
            System.exit(404);
            throw new RuntimeException(e);
        }

/*

		try
		{
			BufferedWriter writer59 = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(folderName + key), "UTF-8"));
			writer59.write(content);
			writer59.flush();
			writer59.close();
		} catch (Exception e) {
			logger.error("Exception writing files : ", e);
			throw new RuntimeException(e);
		}
*/

//		try {
//			// Create a PutObjectRequest
//			PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(folderName + key).build();
//			// Upload the object to the S3 bucket
//			s3.putObject(request, RequestBody.fromString(content));
//			logger.info("File uploaded to " + bucketName + " folder: " + folderName + " with key: " + key);
//
//		} catch (Exception e) {
//			logger.error("Exception from S3Client method : ", e);
//			throw new RuntimeException(e);
//		}
////		logger.info("End of s3FileWrite()!");
    }


	/*private void createDelimitedFile(S3Client s3, String bucketName, String folderName, boolean writeToS3, String reconFolder, String record, String inputFilePath) throws IOException {
		String fullPath = inputFilePath;
		File file = new File(fullPath);
		String inputFileName = file.getName();

		Queue<JSONObject> recordList = new ConcurrentLinkedQueue<>();
		int recordCount = 0;
		String key = "ETS" + record + "_" + inputFileName;

		if ("50300".equals(record)) {
			recordList = EBCDICRecordToASCIItask.getRecord50300();
		} else if ("27200".equals(record)) {
			recordList = EBCDICRecordToASCIItask.getRecord27200();
		} else if ("30100".equals(record)) {
			recordList = EBCDICRecordToASCIItask.getRecords30100();
		}

		StringBuilder contentBuilder = new StringBuilder();
		contentBuilder.append(buildHeaderForDelimitedFile(record)).append("\n");
		Set<String> seenKeys = new HashSet<>();

		for (JSONObject data : recordList) {
			String line = buildDelimitedLine(seenKeys, data, record);
			if (line == null || line.isEmpty()) continue;
			contentBuilder.append(line).append("\n");
			recordCount++;
		}

		contentBuilder.append("TRAILER|").append(recordCount).append("\n");
		String finalContent = contentBuilder.toString();

		if (writeToS3) {
			lookupTableWrite(s3, bucketName, folderName, key, finalContent);
		} else {
			//System.out.println("Generated Content:\n" + finalContent);
		}
	}*/

    private void createDelimitedFile(S3Client s3, String bucketName, String folderName, boolean writeToS3,
                                     String reconFolder, String record, String inputFilePath) throws IOException {
        String fullPath = inputFilePath;
        File file = new File(fullPath);
        String inputFileName = file.getName();
        String key = "ETS" + record + "_" + inputFileName;

        // Close writers to ensure all data is flushed
        EBCDICRecordToASCIItask.closeWriters();

        // Build the final file with header and trailer
        StringBuilder contentBuilder = new StringBuilder();
        contentBuilder.append(buildHeaderForDelimitedFile(record)).append("\n");

        // Get record count
        int recordCount = EBCDICRecordToASCIItask.getRecordCount(record);

        // Read the temporary file and copy its contents
        String tempDir = System.getProperty("java.io.tmpdir") + "/records_temp/";
        File recordFile = new File(tempDir + record + ".txt");
        if (recordFile.exists()) {
            try (BufferedReader reader = new BufferedReader(new FileReader(recordFile))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    contentBuilder.append(line).append("\n");
                }
            }
        } else {
            logger.info("No {} temp file at {}.Generating header/trailer only.", record, recordFile.getAbsolutePath());
        }

        // Add trailer
        contentBuilder.append("TRAILER|").append(recordCount).append("\n");
        String finalContent = contentBuilder.toString();
        //	System.out.println("Final Content for " + record + ":\n" + finalContent);

        if (writeToS3) {
            lookupTableWrite(s3, bucketName, folderName, key, finalContent);
        }
    }


    private String buildHeaderForDelimitedFile(String record) {
        if ("50300".equals(record)) {
            return "KEY||" + String.join("||", Main.fields50300);
        } else if ("27200".equals(record)) {
            return "KEY||" + String.join("||", Main.fields27200);
        } else if ("30100".equals(record)) {
            return "KEY||" + String.join("||", Main.fields30100);
        }
        return "";
    }

	/*private String buildDelimitedLine(Set<String> seenKeys, JSONObject map, String record) {
		if (map == null) return "";

		if (record.contains("50300")) {
			String keyValue = Main.KeyFields50300.stream()
					.map(key -> cleanValue(map.get(key)))
					.collect(Collectors.joining("|"));

			String delimitedFields = Main.fields50300.stream()
					.map(key -> cleanValue(map.get(key)))
					.collect(Collectors.joining("||"));

			return keyValue + "||" + delimitedFields;
		}

		if (record.contains("30100")) {
			String keyValue = Main.KeyFields30100.stream()
					.map(key -> cleanValue(map.get(key)))
					.collect(Collectors.joining("|"));

			// Skip duplicate keys
			if (seenKeys.contains(keyValue)) return null;
			seenKeys.add(keyValue);

			String delimitedFields = Main.fields30100.stream()
					.map(key -> cleanValue(map.get(key)))
					.collect(Collectors.joining("||"));

			return keyValue + "||" + delimitedFields;
		}

		if (record.contains("27200")) {
			String keyValue = Main.KeyFields27200.stream()
					.map(key -> cleanValue(map.get(key)))
					.collect(Collectors.joining("|"));

			String delimitedFields = Main.fields27200.stream()
					.map(key -> cleanValue(map.get(key)))
					.collect(Collectors.joining("||"));

			return keyValue + "||" + delimitedFields;
		}

		return "";
	}

	private String cleanValue(Object value) {
		if (value == null) return " ";
		String val = value.toString().trim();
		return val.isEmpty() ? " " : val;
	}*/

}


