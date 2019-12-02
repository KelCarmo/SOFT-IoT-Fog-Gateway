package br.ufba.dcc.wiser.soft_iot.fog_gateway;
import java.util.List;
import java.util.Random;

import javax.naming.ServiceUnavailableException;
import javax.xml.bind.JAXBException;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.json.JSONArray;
import org.json.JSONObject;
import java.util.HashMap;
import java.util.Map;
import java.util.Comparator;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;

import static java.util.Map.Entry.*;


public class Listener implements IMqttMessageListener {
	
	private boolean debugModeValue;
	private ControllerImpl impl;
	private ClientMQTT clienteMQTT;
	private ClientMQTT clienteMQTT_Send;
	

    public Listener(ControllerImpl impl, ClientMQTT clienteMQTT, ClientMQTT clienteMQTT_Send, String topico, int qos) {
        this.clienteMQTT = clienteMQTT;
        this.clienteMQTT_Send = clienteMQTT_Send;
        this.clienteMQTT.subscribe(qos, this, topico);
        this.impl = impl;
    }
    
    public Listener(ControllerImpl impl) {       
        this.impl = impl;
    }
    
    public static void main(String[] args) throws Exception {
    	ControllerImpl ctrl= new ControllerImpl();
    	Listener novo = new Listener(ctrl);
    	novo.messageArrived("TOP_K_HEALTH/1/1", new MqttMessage());
    }

    @Override
    public synchronized void messageArrived( final String topic, final MqttMessage message) throws Exception {
//        System.out.println("Mensagem recebida:");
//        System.out.println("\tTópico: " + topico);
//        System.out.println("\tMensagem: " + new String(mm.getPayload()));
//        System.out.println("");
    	
    	final String [] params = topic.split("/");
    	final int k = Integer.valueOf(params[2]);
    	System.out.println("=========================================");
    	
//    	this.topk_k_scoresByIdrequi.put(params[1], null);
        new Thread(new Runnable() {
			public void run() {
				String messageContent = new String(message.getPayload());
				printlnDebug("topic: " + topic + "message: " + messageContent);
				if(params[0].equals("TOP_K_HEALTH_RES")) {
					byte[] b = messageContent.getBytes();
					List<String> n = impl.topk_k_scoresByIdrequi.get(params[1]);
					n.add(messageContent);
					impl.topk_k_scoresByIdrequi.put(params[1], n);
					System.out.println("TOP-K RES REcebido: " + messageContent);
				}
				if(params[0].equals("TOP_K_HEALTH")) {
					byte[] b = messageContent.getBytes();
					clienteMQTT_Send.publicar(topic, b, 1);
					List<String> n = new ArrayList<String>();
					impl.topk_k_scoresByIdrequi.put(params[1], n);
					impl.calculateTopK(params[1], k);
					System.out.println("REQUI TOP-K REcebido: " + messageContent);
				}
				if(params[0].equals("CONNECTED")) {
					clienteMQTT.add_G_Connecteds();
					System.out.println("Gateways connecteds: " + clienteMQTT.getQtdConnecteds());
				}
				

			}
		}).start();
    }
    
    public void saveMsg() {
    	
    	
    }
    
    private void printlnDebug(String str){
		if (debugModeValue)
			System.out.println(str);
	}

}