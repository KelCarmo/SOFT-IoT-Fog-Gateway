package br.ufba.dcc.wiser.soft_iot.fog_gateway;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.xml.bind.JAXBException;

import org.json.JSONArray;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;


public class ControllerImpl{
	
	private String ip;
	private String port;
	private String user;
	private String pass;
	private String ip_up;
	private String port_up;
	private String user_up;
	private String pass_up;
	private boolean debugModeValue;
	private  ClientMQTT clienteMQTT;
	private ClientMQTT clienteMQTT_UP;
	public Map<String,List<String>> topk_k_scoresByIdrequi = new HashMap<String,List<String>>();
	
	
	public void start(){
		//printlnDebug("Starting mapping of connected devices...");		
		// TODO Auto-generated method stub
		System.out.println("Opaa");
			// Se conectando a um broker acima ...
//		clienteMQTT = new ClientMQTT("tcp://" + this.ip + ":" + this.port, user, pass);
		 	clienteMQTT = new ClientMQTT("tcp://localhost:8080", user, pass);
		 	clienteMQTT.iniciar(); // PUBLISH
	 	clienteMQTT_UP = new ClientMQTT("tcp://" + this.ip_up + ":" + this.port_up, user_up, pass_up);
		 	clienteMQTT_UP = new ClientMQTT("tcp://192.168.0.175:8282", user_up, pass_up);
		 	clienteMQTT_UP.iniciar();
//	        if(devices != null) System.out.println("Conectado com Broker de FOG com sucesso!!!");
//	        new Listener(this, clienteMQTT_Send, clienteMQTT , "TOP_K_HEALTH/#", 1);
//        	new Listener(this, clienteMQTT, clienteMQTT_Send, "TOP_K_HEALTH_RES/#", 1);
//        	new Listener(this, clienteMQTT, clienteMQTT_Send, "CONNECTED/#", 1);
			System.out.println("TOP_K SCORES: " + clienteMQTT.getQtdConnecteds());
		 	new Listener(this, clienteMQTT, clienteMQTT_UP, "#", 1); // SUBSCRIBE
        	new Listener(this, clienteMQTT_UP, clienteMQTT, "TOP_K_HEALTH/#", 1);
			System.out.println("TOP_K SCORES: " + clienteMQTT.getQtdConnecteds());
        	
	        
	}
	
	public void calculateTopK (final String id, final int k) {
		new Thread(new Runnable() {
			public void run() {
				System.out.println("Esperando todos nós de Gateway mandarem seus top-k's");
				while(topk_k_scoresByIdrequi.get(id).size() < clienteMQTT.getQtdConnecteds()) {
					System.out.println("TOP_K SCORES: " + topk_k_scoresByIdrequi.get(id).size());
					System.out.println("TOP_K SCORES_Gateways: " + clienteMQTT.getQtdConnecteds());
					//break;
				}
				
				System.out.println("OK .. agora vou calcular o TOP-K dos TOP-K's");
				Map<String, Integer> myMap = new HashMap<String, Integer>();
				for(String t: topk_k_scoresByIdrequi.get(id)) {
					t = t.replace("{", "");
					t = t.replace("}", "");
					t = t.replace(" ", "");
					String[] pairs = t.split(",");
					for (int i=0;i<pairs.length;i++) {
					    String pair = pairs[i];
					    String[] keyValue = pair.split("=");
					    myMap.put(keyValue[0], Integer.valueOf(keyValue[1]));
					}
				}
				
				Object[] a = myMap.entrySet().toArray();
				Arrays.sort(a, new Comparator<Object>() {
				    @SuppressWarnings("unchecked")
					public int compare(Object o1, Object o2) {
				        return ((Map.Entry<String, Integer>) o2).getValue()
				                   .compareTo(((Map.Entry<String, Integer>) o1).getValue());
				    }
				});
				Map<String,Integer> top_k = new HashMap<String,Integer>();
				// Pegando os k piores ...
				for (int i =0; i< k; i++) {
					Map.Entry<String, Integer> e = (Map.Entry<String, Integer>) a[i];
					top_k.put(e.getKey(), e.getValue());
					
				}
				System.out.println("TOP_K => " + top_k.toString());
				System.out.println("=========================================");
				byte[] b = top_k.toString().getBytes();					
				clienteMQTT_UP.publicar("TOP_K_HEALTH_RES/" + id, b, 1);
				
			}
		}).start();
	}
	
	public static void main(String[] args) throws JAXBException {
		ControllerImpl ctrl= new ControllerImpl();
    	ctrl.start();
//    	ctrl.updateValuesSensors();
//    	System.out.print(ctrl.getListDevices().get(0).getSensors().get(0).getValue());
       
    }
	
	public void stop(){
		
	        this.clienteMQTT.finalizar();
	        this.clienteMQTT_UP.finalizar();
	    
	}
	
	private void printlnDebug(String str){
		if (debugModeValue)
			System.out.println(str);
	}

	// public void setStrJsonDevices(String strJsonDevices) {
	// 	this.strJsonDevices = strJsonDevices;
	// }

	public void setDebugModeValue(boolean debugModeValue) {
		this.debugModeValue = debugModeValue;
	}

}
