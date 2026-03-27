package com.broadridge.ebcdic;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;


public class ConfigSplit {

	public static Map<String, InputCFG> detailConfigMap = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap5030 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap3P30 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMapPositions = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMapClientDefined = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap5040 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap5062 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap504141 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap5080 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap5060 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap5070 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap3010 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap2010 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap1010 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap2410 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap2582 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap0530 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap3011 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap504150= new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap3020 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap3050 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap22NN = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap0422 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap0420 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap5085 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMap2720 = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailConfigMapcldt = new LinkedHashMap<String, InputCFG>();
	public static Map<String, InputCFG> detailCarmaConfigMap = new LinkedHashMap<String, InputCFG>();
    public static Map<String,InputCFG> detailConfigMap504151 = new LinkedHashMap<>();
	public static InputCFG inputCFG = null;
	private static BufferedReader reader = null;
	private static BufferedReader B1reader = null;
	public static ArrayList<String> dynamicTrids = new ArrayList<String>();
	public static Map<String , String> clientDefinedTrids = new HashMap<String , String>();
	public static Map<String, Integer> recordCount = new LinkedHashMap<String, Integer>();

	public static void readConfig(String configFileName){

		try {
			reader = new BufferedReader(new FileReader(configFileName));
			String line = null;
			String key = "";
			while ((line = reader.readLine()) != null) {
				if (!line.startsWith("#")) {
					String[] tokens = line.split("\\|");
					if (tokens.length > 1) {
						try {
							inputCFG = new InputCFG();
							inputCFG.setfId(tokens[0].trim());
							inputCFG.setFieldName(tokens[2].trim());
							inputCFG.setRecordType(tokens[1].trim());
							if(recordCount.containsKey(tokens[1].trim())) {
								int l = recordCount.get(tokens[1].trim());
								l++;
								recordCount.put(tokens[1].trim(), l);
							}else {
								recordCount.put(tokens[1].trim(), 1);
							}
							inputCFG.setStartIdx(Integer.parseInt(tokens[3].trim()));
							if(!tokens[5].trim().equals("")) {

							inputCFG.setEndIdx(Integer.parseInt(tokens[4].trim()));

							}
							inputCFG.setFieldLength(Integer.parseInt(tokens[5].trim()));
							inputCFG.setInType(tokens[6].trim());
							inputCFG.setOutType((tokens[7].trim()));
							if(tokens[8].trim().startsWith("OCCURS")) {
								inputCFG.setOccurs(true);
								String [] occurs = tokens[8].trim().split("\\s+");
								key = tokens[2].trim();
								if(occurs.length > 1) {
									key += Integer.parseInt(occurs[1]) < 10 ? "0"+occurs[1] : occurs[1];
								}
								inputCFG.setOccursArrayKey(occurs[occurs.length -1]);
							} else {
								inputCFG.setOccurs(false);
								key = tokens[2].trim();
							}
							if(key.startsWith("FILLER")) {

							}
							inputCFG.setRedefine((tokens[9].trim()));
							if(inputCFG.getRecordType().contains("ETS5030")) {
								detailConfigMap5030.put(key, inputCFG);
								// String key5040 = key.replace("503", "504");
								// InputCFG ipCfg = inputCFG;
								// ipCfg.setFieldName(inputCFG.getFieldName().replace("503", "504"));
								// ipCfg.setRecordType(ipCfg.getRecordType().replace("503", "504"));
								// detailConfigMap5040.put(key5040, ipCfg);
							}
							else if (inputCFG.getRecordType().contains("ETS3P3")) {
								detailConfigMap3P30.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS5040")) {
								detailConfigMap5040.put(key, inputCFG);
							}

							else if (inputCFG.getRecordType().contains("ETS5070")) {
								detailConfigMap5070.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS5060")) {
								detailConfigMap5060.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS3010")) {
								detailConfigMap3010.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS2010")) {
								detailConfigMap2010.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS1010")) {
								detailConfigMap1010.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS2582")) {
								detailConfigMap2582.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS2410")) {
								detailConfigMap2410.put(key, inputCFG);
							}
                            else if (inputCFG.getRecordType().contains(clientDefinedTrids.get("504150"))) {
                                detailConfigMap504150.put(key, inputCFG);

                            }
                            else if(inputCFG.getRecordType().contains(clientDefinedTrids.get("504151"))){
                                detailConfigMap504151.put(key,inputCFG);
                            }
                            else if (inputCFG.getRecordType().contains(clientDefinedTrids.get("504141"))) {
                                detailConfigMap504141.put(key, inputCFG);
                            }
							else if (inputCFG.getRecordType().contains("ETS3011")) {
								detailConfigMap3011.put(key, inputCFG);
							}

                            else if (inputCFG.getRecordType().contains("ETS3020")) {
								detailConfigMap3020.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS3050")) {
								detailConfigMap3050.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().matches("ETS22.*")) {
								detailConfigMap22NN.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS0422")) {
								detailConfigMap0422.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS0420")) {
								detailConfigMap0420.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS5085")) {
								detailConfigMap5085.put(key, inputCFG);
							}
							else if (inputCFG.getRecordType().contains("ETS0530")) {
								detailConfigMap0530.put(key, inputCFG);
							}
							else if(inputCFG.getRecordType().contains("ETSCLDT")) {
								detailConfigMapcldt.put(key, inputCFG);
							}
							else if(inputCFG.getRecordType().contains("ETS5062")) {
								detailConfigMap5062.put(key, inputCFG);
							}
							else if(inputCFG.getRecordType().contains("ETS5080")) {
								detailConfigMap5080.put(key, inputCFG);
							}
							else if(inputCFG.getRecordType().contains("ETS2720")) {
								detailConfigMap2720.put(key, inputCFG);
							}
							else {
								detailConfigMap.put(key, inputCFG);
							}


						} catch (Exception e) {
							e.printStackTrace();
						}
					}
				}
			}
		} catch (IOException e) {
			Globals.logMapper.put("Message",  "an error occured at "+e.toString());
			Globals.logMapper.put("Status", "failed");
			Globals.logMapper.put("Class", "Validation.class");
			Splunk.logMessage(Globals.logMapper, Globals.awsAccountDetail+" "+Globals.region.toString());
			Globals.logMapper.clear();

			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				Globals.logMapper.put("Message",  "an error occured at "+e.toString());
				Globals.logMapper.put("Status", "failed");
				Globals.logMapper.put("Class", "Validation.class");
				Splunk.logMessage(Globals.logMapper, Globals.awsAccountDetail+" "+Globals.region.toString());
				Globals.logMapper.clear();

				e.printStackTrace();
			}
		}
	}

	public static void readCarmaConfig(String carmaConfig) {
		// TODO Auto-generated method stub
		try {
			reader = new BufferedReader(new FileReader(carmaConfig));
			String line = null;

			while ((line = reader.readLine()) != null) {
				if (!line.startsWith("#")) {
					String[] tokens = line.split("\\|");
					if (tokens.length > 1) {
						try {
//								buildConfigmap(detailConfigMap, tokens);
							inputCFG = new InputCFG();
							inputCFG.setfId(tokens[0].trim());
							inputCFG.setFieldName(tokens[2].trim());
							inputCFG.setRecordType(tokens[1].trim());
							if(recordCount.containsKey(tokens[1].trim())) {
								int l = recordCount.get(tokens[1].trim());
								l++;
								recordCount.put(tokens[1].trim(), l);
							}else {
								recordCount.put(tokens[1].trim(), 1);
							}
							inputCFG.setStartIdx(Integer.parseInt(tokens[3].trim()));
							if(!tokens[5].trim().equals("")) {

							inputCFG.setEndIdx(Integer.parseInt(tokens[4].trim()));
							}
							inputCFG.setFieldLength(Integer.parseInt(tokens[5].trim()));
							inputCFG.setInType(tokens[6].trim());
							inputCFG.setOutType((tokens[7].trim()));
							detailCarmaConfigMap.put(tokens[2].trim(), inputCFG);

						} catch (Exception e) {
							Globals.logMapper.put("Message",  "an error occured at "+e.toString());
							Globals.logMapper.put("Status", "failed");
							Globals.logMapper.put("Class", "Validation.class");
							Splunk.logMessage(Globals.logMapper, Globals.awsAccountDetail+" "+Globals.region.toString());
							Globals.logMapper.clear();

							e.printStackTrace();
						}
					}
				}
			}

		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void readB1Settings(String b1Settings) throws IOException {
		// TODO Auto-generated method stub
		B1reader = new BufferedReader(new FileReader(b1Settings));
        List<String> b1ToTrid =Arrays.asList("504150","504151","504141");

        try {

			String line = null;
			String [] lineArray;
			while ((line = B1reader.readLine()) != null) {
				lineArray = line.split(",");
				if(lineArray[1].equals("504250") && lineArray[2].equals("ST2 CLIENT DETAIL DATA ID")) {
					dynamicTrids.add(lineArray[3].substring(0, 4));
				} else if(lineArray[1].equals("504140") || lineArray[1].equals("504141") || lineArray[1].equals("504142") ||
						lineArray[1].equals("504143") || lineArray[1].equals("504145") || lineArray[1].equals("504150")||lineArray[1].equals("504151")) {
					String tridValue = lineArray[3].replace("-", "");
					if (b1ToTrid.contains(lineArray[1])){
                        if (tridValue.length()>3){
                            tridValue = tridValue.substring(0,4);
                        }
						clientDefinedTrids.put(lineArray[1],tridValue );
					}else {
						clientDefinedTrids.put(tridValue , lineArray[1]);
					}


				}
			}
			// B1reader.close();  => removed due to the vernarabilitys
		} catch(Exception e) {
			e.printStackTrace();
		}
		finally {
			B1reader.close();
		}

	}}
