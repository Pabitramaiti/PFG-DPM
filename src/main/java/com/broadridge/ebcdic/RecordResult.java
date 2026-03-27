package com.broadridge.ebcdic;

import org.json.simple.JSONObject;

public class RecordResult {



    public String strASCII = "";

    public String accNum = "";

    public String millionInd = "";

    public String topAcct = "";

    public String recAccNum = "";

    public String clientId = "";

    public String uanNum = "";  // ACCT_SWING

    public String printInd = "" ;

    public String xferToDate = "" ;

    public String xferFromDate = "" ;

    public String xferToAccount = "" ;

    public String xferFromAccount = "" ;

    public String recordType = "";
    
    public String currency = "";

    public boolean isHeader = false;

    public boolean isTrailer = false;

    public boolean isXxfer = false;

    public boolean isWriteIndexerFile = false;

    public boolean accNumCheck = false;

    public boolean updatePrevOffset =  false;

    public Account acct = null;

    protected JSONObject jObj1010 = null;

    protected JSONObject jObj2010 = null;

    protected JSONObject jObj3010 = null;

    protected JSONObject jObj5030 = null;

    protected JSONObject jObj5040 = null;

    protected JSONObject jObj5050 = null;

    protected JSONObject jObj5085 = null;

    protected JSONObject jObj2582 = null;

    protected JSONObject jObj22NN = null;

    protected JSONObject jObj3011 = null;

    protected JSONObject jObj3020 = null;

    protected JSONObject jObj3050 = null;

    protected JSONObject jObj1013 = null;

    protected JSONObject jObj = null;

    protected JSONObject jObjXbase0000 = null;

    protected JSONObject jObj0000 = null;

	protected JSONObject jObj0420 = null;

	protected JSONObject jObj0520 = null;

	protected JSONObject jObj0530 = null;

	protected JSONObject jObj2410 = null;

	protected JSONObject jObj0422 = null;

	protected JSONObject jObjCLDT = null;

	protected JSONObject jObj0410 = null;

	protected JSONObject jObj2720 = null;
    protected JSONObject jObj1120 = null;

    public String historical_yy = "";
    public String historical_mm = "";
    

}

