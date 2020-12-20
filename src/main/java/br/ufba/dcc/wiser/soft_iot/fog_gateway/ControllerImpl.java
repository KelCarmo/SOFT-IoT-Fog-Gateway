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


public class ControllerImpl implements Controller {
	
	private String ip;
	private String port;
	private String user;
	private String pass;
	private String ip_up;
	private String port_up;
	private String user_up;
	private String pass_up;
	private String childs;
	private boolean debugModeValue;
	private  ClientMQTT clienteMQTThost;
	private ClientMQTT clienteMQTT_UP;
	public Map<String,List<String>> topk_k_scoresByIdrequi = new HashMap<String,List<String>>();
	
	public void start(){
		//printlnDebug("Starting mapping of connected devices...");		
		// TODO Auto-generated method stub
		
		// Se conectando ao broker local
	 	clienteMQTThost = new ClientMQTT("tcp://" + this.ip + ":" + this.port, this.user, this.pass);
	 	clienteMQTThost.iniciar();
	 	// Se conectando ao broker acima ...
	 	clienteMQTT_UP = new ClientMQTT("tcp://" + this.ip_up + ":" + this.port_up, this.user_up, this.pass_up);
	 	clienteMQTT_UP.iniciar();
    	
	 	new Listener(this, clienteMQTThost, "TOP_K_HEALTH_RES/#", 1);
    	
    	new ListenerTopK(this, clienteMQTT_UP, clienteMQTThost, "TOP_K_HEALTH/#", 1);
	}
	
//	/**
//	 * Métodos para testar o bundle FORA do Service Mix. AO fazer o build, comente-os. 
//	 * @param args
//	 * @throws JAXBException
//	 */
//	public static void main(String[] args) throws JAXBException {
//		ControllerImpl ctrl= new ControllerImpl("localhost", "1884", "localhost", "1885", "1");
//
//		ctrl.start();
//    }
//	
//	public ControllerImpl(String ip, String port, String ip_up, String port_up, String childs) {
//		this.ip = ip;
//		this.port = port;
//		this.ip_up = ip_up;
//		this.port_up = port_up;
//		this.childs = childs;
//		
//	}
	
	@Override
	public void calculateTopK(final String id, final int k) {

		System.out.println("Esperando todos nós de Gateway mandarem seus top-" + k + " QTD: " + this.topk_k_scoresByIdrequi.get(id).size());
		
		while(this.topk_k_scoresByIdrequi.get(id).size() < Integer.parseInt(this.childs)) {}
		
		System.out.println("OK .. agora vou calcular o TOP-K dos TOP-K's");
		System.out.println("TOP_K SCORES RECEIVED: " + this.topk_k_scoresByIdrequi.get(id).size());
		
		Map<String, Integer> myMap = new HashMap<String, Integer>();
		
		for(String t: this.topk_k_scoresByIdrequi.get(id)) {
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
	
	public void stop() {
        this.clienteMQTThost.finalizar();
        this.clienteMQTT_UP.finalizar();
	}
	
	private void printlnDebug(String str){
		if (debugModeValue)
			System.out.println(str);
	}

	public void setDebugModeValue(boolean debugModeValue) {
		this.debugModeValue = debugModeValue;
	}
}
