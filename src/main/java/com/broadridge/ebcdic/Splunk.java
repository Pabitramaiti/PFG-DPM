
package com.broadridge.ebcdic;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URI;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Splunk {

	private final String url;
	private final String token;
	private String payload;

	public Splunk(String url, String token) {
		this.url = url;
		this.token = token;
		this.payload = "";
	}

	public void logEvent(Map<String, Object> data) {
		data.put("time", Instant.now().getEpochSecond());
		//System.out.println(Instant.now().getEpochSecond());
		//System.out.println(Instant.now());
		data.put("host", "serverless");
		data.put("source", "dpm-di-docker:Ebcdic to Json");
		data.put("sourcetype", "httpevent");
		this.payload = mapToJson(data);
		//System.out.println(this.payload);
	}

	private String mapToJson(Map<String, Object> data) {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> JsonData = new HashMap<>();
		JsonData.put("time", Instant.now().getEpochSecond());
		//System.out.println(Instant.now().getEpochSecond());
		//System.out.println(Instant.now());
		JsonData.put("host", "serverless");
		JsonData.put("source", "dpm-di-docker:Ebcdic to Json");
		JsonData.put("sourcetype", "httpevent");
		JsonData.put("event", data);
		try {
			return objectMapper.writeValueAsString(JsonData);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}

	public void flushAsync(Runnable callback) {
		Runnable effectiveCallback = (callback != null) ? callback : () -> {
		};

		new Thread(() -> {
			try {
				URI uri = new URI(url);
				HttpURLConnection connection = (HttpURLConnection) uri.toURL().openConnection();
				connection.setRequestMethod("POST");
				connection.setRequestProperty("Content-Type", "application/json");
				connection.setRequestProperty("Authorization", "Splunk " + token);
				connection.setDoOutput(true);

				try (OutputStream os = connection.getOutputStream()) {
					// os.write(payload.getBytes(StandardCharsets.UTF_8));
					byte[] input = payload.getBytes("utf-8");
					os.write(input, 0, input.length);
				}

				int statusCode = connection.getResponseCode();
				//System.out.println("Response code is: " + statusCode);

				try (BufferedReader reader = new BufferedReader(
						new InputStreamReader(getInputStreamOrErrorStream(connection, statusCode)))) {
					String response = reader.lines().reduce("", (acc, line) -> acc + line);
					//System.out.println("Response body: " + response);
				}

				effectiveCallback.run();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}).start();
	}

	private InputStream getInputStreamOrErrorStream(HttpURLConnection connection, int statusCode) throws IOException {
		return (statusCode >= 200 && statusCode < 400) ? connection.getInputStream() : connection.getErrorStream();
	}

	public static void logMessage(Map<String, Object> input, String accountData) {

		Map<String, Object> data = new HashMap<>(input);
		data.put("application_name", "Data Preparation Manager");
		data.put("application_short_name", "DPM");
		
		data.put("aws_region", accountData.trim().split(" ")[1]);
	
		String currentStage = determineStage((String) accountData.trim().split(" ")[0]);
		//System.out.println("==============>"+ accountData.trim().split(" ")[0] + " account " +accountData);
		data.put("stage", currentStage);	
		//System.out.println("stage ==>" + currentStage);

		Splunk customLogger = new Splunk(getSplunkUrl(currentStage), getSplunkToken(currentStage));

		customLogger.logEvent(data);
		customLogger.flushAsync(() -> System.out.println("Async operation completed"));
	}

	public static String determineStage(String awsAccountId) {
		if ("187777304606".equals(awsAccountId))
			return "DEV";
		if ("471112732183".equals(awsAccountId))
			return "PROD";
		if ("556144470667".equals(awsAccountId))
			return "QA";
//		if (functionName.contains("dpmdev-di"))
//			return "INT";
//		if (functionName.contains("br_icsuat"))
//			return "UAT";
		return "PROD";
	}

	private static String getSplunkUrl(String stage) {
		return (stage.equals("DEV") || stage.equals("INT"))
				? "https://http-inputs-br-nprd.splunkcloud.com/services/collector"
				: "https://http-inputs-br.splunkcloud.com/services/collector";
	}

	private static String getSplunkToken(String stage) {
		return (stage.equals("DEV") || stage.equals("INT")) ? "47ae2988-d442-1344-498a-d18179ff4adb"
				: "56a47b4b-12e2-4855-a687-2b8224a39ef0";
	}

	
}