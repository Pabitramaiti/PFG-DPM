package com.broadridge.ebcdic;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.simple.JSONObject;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import software.amazon.awssdk.core.sync.RequestBody;

import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;




class file_validator implements Callable<RecordResult> {
	public String Context = Globals.awsAccountDetail+" "+ Globals.region.toString();
	public Map<String,Object>logMapper = Globals.logMapper;
	

	private String strEBCDIC = null;

	private int index = 0;

	private int record = 0;

	private long lengthOfChar = 0L;

	private RecordResult RR = new RecordResult();

	private volatile static boolean indexerFlag = false;
	


	public file_validator(String strIn, int index, int record, long lengthOfChar, Globals global) {

		this.strEBCDIC = strIn;

		this.index = index;

		this.record = record;

		this.lengthOfChar = lengthOfChar;

	}

	public static int XBASE_REC_LENGTH_ENDIDX;

	public static int XBASE_TRID12_STARTIDX;

	public static int XBASE_TRID12_ENDIDX;
	public static boolean trid9090 = false;
	public static int count = 0;

	static {

		XBASE_REC_LENGTH_ENDIDX = ConfigSplit.detailConfigMap.get(FileConverter_MT.XBASERECLEN).getEndIdx();

		XBASE_TRID12_STARTIDX = ConfigSplit.detailConfigMap.get(FileConverter_MT.XBASETRID).getStartIdx();

		XBASE_TRID12_ENDIDX = ConfigSplit.detailConfigMap.get(FileConverter_MT.XBASESUBTRID).getEndIdx();

	}
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

	
	
	
	
	@Override
	public RecordResult call() throws Exception {

		byte[] bytesEBCDIC = strEBCDIC.getBytes("CP819");
		Validation.totalRecords.incrementAndGet();
		String convertedASCII = new String(bytesEBCDIC, "Cp1047");
		String recordType = convertedASCII.substring(XBASE_TRID12_STARTIDX - 1, XBASE_TRID12_ENDIDX);
		RR.recordType = "ETS" + recordType;

		if (RR.recordType.equals("ETS9090")) {
			Validation.tailerRecordFound = true;
			for (String p : ConfigSplit.detailConfigMap.keySet()) {
				if (p.contains("90900")) {
					if (p.equals("90900-TLR-TOTAL")) {
						StringBuilder sbOutput2 = new StringBuilder();
						Validation.tailerTotalFound = true;
//						=======================================================================
//						 for comp3
//						=======================================================================
						if (strEBCDIC.length() > 32 + ConfigSplit.detailConfigMap.get(p).getEndIdx()) {

							String ss = strEBCDIC.substring(32 + ConfigSplit.detailConfigMap.get(p).getStartIdx() - 1, 32 + ConfigSplit.detailConfigMap.get(p).getEndIdx());

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

								num = Integer.parseInt(ConfigSplit.detailConfigMap.get(p).getOutType().substring(1, ConfigSplit.detailConfigMap.get(p).getOutType().length()));

							}

							sbOutput2.append(unpackData(q, dec, num));
							Validation.B228TrailerRecords = new AtomicLong(Long.parseLong(unpackData(q, dec, num)));

						}
//						=======================================================================
//						=====================================================================


						if (sbOutput2 == null) {

							Validation.tailerTotalFound = false;

						}

					}
				}
			}

		}
		return RR;
	}
}
	public class Validation {
		public String Context = Globals.awsAccountDetail+" "+ Globals.region.toString();
		public Map<String,Object>logMapper = Globals.logMapper;
		private static final Logger logger = LogManager.getLogger(Validation.class);
		public static AtomicInteger totalRecords = new AtomicInteger();
		public static boolean tailerRecordFound;
		public static boolean tailerTotalFound;
		public static boolean isValid;

		public static AtomicLong B228TrailerRecords = new AtomicLong(0);

		public static final String XBASERECLEN = "XBASE-HDR-RECLEN";

		public static final String XBASETRID = "XBASE-HDR-TRID-00";

		public static final String XBASESUBTRID = "XBASE-HDR-SUBTRID-00";

		public static int XBASE_REC_LENGTH_ENDIDX;

		public static int XBASE_TRID12_STARTIDX;

		public static int XBASE_TRID12_ENDIDX;

		public Globals global = new Globals();

		public Account acct = null;

		BufferedWriter writer59 = null;		
		
		ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
		public File ValOutFile = null;
		private static final Date date = new Date();
		
		public void validate(String inputFile, String configFile,int NUM_THREADS, int BATCH_SIZE,
				String outputFormat, String b1Settings) throws IOException {

			logger.info("Starting validation...");
			logMapper.put("Message",  "Starting validation...");
        	logMapper.put("Status", "success");
        	logMapper.put("Class", "Validation.class");        	
			
			Splunk.logMessage(logMapper, Context);
			logMapper.clear();

			ConfigSplit.readB1Settings(b1Settings);
			ConfigSplit.readConfig(configFile);


			InputStreamReader inputr1 = null;

			BufferedReader br1 = null;

			int ch;

			try {

				// Specify the Input file path

				inputr1 = new InputStreamReader(new FileInputStream(inputFile), "CP819");

				br1 = new BufferedReader(inputr1, 8192);

				long chara = 0;

				String lineLength = "";

				long totalLine = 0;

				int record = 0;

				String ebcdic = "";

				int index = 0;

				boolean writeFlag = false;

				long lengthOfChar = 0;

				StringBuilder sbEbcdic = new StringBuilder();

				ExecutorService executor = Executors.newFixedThreadPool(NUM_THREADS);

				List<Future<RecordResult>> futuresList = new ArrayList<>(BATCH_SIZE + 5);

				int futuresIndex = 0;

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

						Callable<RecordResult> task = new file_validator(ebcdic, index, record, lengthOfChar, global);

						Future<RecordResult> future = executor.submit(task);

						futuresList.add(future);
						if (index % BATCH_SIZE == 0) {

							if (outputFormat.equals("ASCII")) {
								for (Future<RecordResult> aFuture : futuresList) {

									try {

										futuresIndex++;

										RecordResult rr = aFuture.get();

									} catch (InterruptedException | ExecutionException e) {
										throw new RuntimeException(e);
									}

								}

								futuresList.clear();

								executor.shutdown();

								logger.info("End of convert()!");
								
							} else {
								futuresList.clear();
							}

						}

						lengthOfChar = chara;

						ebcdic = "";

						writeFlag = false;
					}

				}
			} finally {
				logger.info("-------validated---------");
				logMapper.put("message",  "file validated ");
	        	logMapper.put("Status", "success");
	        	logMapper.put("Class", "validation.class");        	
				
				Splunk.logMessage(logMapper, Context);
				logMapper.clear();
			}

		}
		
		
//////////////////////////////////////////////
//		private void addTrailersToOutputFiles(String inputFile, String trailerFile, S3Client s3, String bucketName, String outputQueueFolder) {
		void addTrailersToOutputFiles(String inputFile, S3Client s3, String bucketName, String trailerOutputFile, String outputQueueFolder) {
 
			logger.info("Starting addTrailersToOutputFiles()...");
			try {

				String key = inputFile;
				totalRecords.decrementAndGet(); //Subtracting 1 for the header rec
				JSONObject trailerJSON = new JSONObject();
				trailerJSON.put("FILE-NAME", new File(inputFile).getName());
				trailerJSON.put("TRAILER-TOTAL" , Validation.B228TrailerRecords);
				trailerJSON.put("NUMBER-OF-RECORDS" , Validation.totalRecords);
				JSONObject trailerFileJSON = new JSONObject();
				trailerFileJSON.put("FILE" , trailerJSON);

				String content = mapper.writeValueAsString(trailerFileJSON);

				key = trailerOutputFile;
				s3FileWrite(s3, bucketName, outputQueueFolder, key, content);

			} catch(Exception e) {
				logger.error("An error occurred", e);
				logMapper.put("message",  "AN error occurred "+e.toString());
	        	logMapper.put("Status", "failed");
	        	logMapper.put("Class", "Validation.class");        	
				logger.error("An error occurred", e);
				Splunk.logMessage(logMapper, Context);
				logMapper.clear();
			}
			logger.info("End of addTrailersToOutputFiles()!");
		}

		
		private void s3FileWrite(S3Client s3, String bucketName, String folderName, String key, String content) {

			logger.info("Starting s3FileWrite()...");
			logger.info("    Bucket Name : [" + bucketName + "]");
			logger.info("    Folder Name : [" + folderName + "]");
			logger.info("      File Name : [" + key + "]");
			logger.info("B228TrailerRecords : [" + Validation.B228TrailerRecords + "]");
			logger.info("    totalRecords : [" + Validation.totalRecords + "]");

			/*
			
				try {
					writer59 = new BufferedWriter(new FileWriter(folderName + key));
					writer59.write(content);
					writer59.flush();
	                writer59.close();				
				} catch (Exception e) {
					logger.error("Exception writing files : ", e);
					logMapper.put("Message",  "Exception writing files : "+ e);
		        	logMapper.put("Status", "failed");
		        	logMapper.put("Class", "Validation.class"); 
					Splunk.logMessage(logMapper, Context);
					logMapper.clear();
					logger.error("Exception : ", e);
					throw new RuntimeException(e);
	    		}
*/				
				
				try {
					// Create a PutObjectRequest
					PutObjectRequest request = PutObjectRequest.builder().bucket(bucketName).key(folderName + key).build();
					// Upload the object to the S3 bucket
					s3.putObject(request, RequestBody.fromString(content));
					logger.info("File uploaded to " + bucketName + " folder: " + folderName + " with key: " + key);

				} catch (Exception e) {
					logger.error("Exception from S3Client method : ", e);
					throw new RuntimeException(e);
				}

			logger.info("End of s3FileWrite()!");
		}
		

/////////////////////////////////////////////	
		
	}
