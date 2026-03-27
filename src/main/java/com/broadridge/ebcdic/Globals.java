package com.broadridge.ebcdic;

import java.util.LinkedHashMap;
import java.util.Map;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import software.amazon.awssdk.regions.Region;

public class Globals {
	
	protected JSONObject B228File = new JSONObject();
	protected JSONArray headerArray = new JSONArray();
	protected JSONArray clientArray = new JSONArray();
	protected JSONArray accountArray = new JSONArray();
	protected JSONArray trailerArray = new JSONArray();
	protected JSONArray jArr0000 = new JSONArray();
	protected JSONArray jArr0420 = new JSONArray();
	protected JSONArray jArr0520 = new JSONArray();
	protected JSONArray jArr0530 = new JSONArray();
	protected JSONArray jArr0422 = new JSONArray();
	protected Map<String , JSONArray> jArrClientHeaders = new LinkedHashMap<String , JSONArray>();
	protected JSONArray jArrTTSS = new JSONArray();
	protected JSONArray jArr9010 = new JSONArray();
	protected JSONArray jArr9050 = new JSONArray();
	protected JSONArray jArr9080 = new JSONArray();
	protected JSONArray jArr9090 = new JSONArray();
//	Default Account Details
	public static Region region =  Region.US_EAST_1;
	public static String awsAccountDetail ="187777304606";
	public  static Map<String, Object> logMapper = new LinkedHashMap<>();
}
