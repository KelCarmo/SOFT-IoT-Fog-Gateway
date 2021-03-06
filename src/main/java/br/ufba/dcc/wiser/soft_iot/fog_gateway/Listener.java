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
	private ClientMQTT clienteMQTT_Host;
	

    public Listener(ControllerImpl impl, ClientMQTT clienteMQTT_Host, String topico, int qos) {
        this.clienteMQTT_Host = clienteMQTT_Host;
        this.clienteMQTT_Host.subscribe(qos, this, topico);
        this.impl = impl;
    }
    
    public Listener(ControllerImpl impl) {       
        this.impl = impl;
    }
    
    @Override
    public synchronized void messageArrived( final String topic, final MqttMessage message) throws Exception {
    	
    	final String [] params = topic.split("/");
    	System.out.println("=========================================");
    	
        new Thread(new Runnable() {
			public void run() {
				String messageContent = new String(message.getPayload());
				printlnDebug("topic: " + topic + "message: " + messageContent);
				
				if(params[0].equals("TOP_K_HEALTH_RES")) {
					byte[] b = messageContent.getBytes();
					List<String> n = impl.topk_k_scoresByIdrequi.get(params[1]);
					n.add(messageContent);
					impl.topk_k_scoresByIdrequi.put(params[1], n);
					System.out.println("TOP-K RES REcebido: " + impl.topk_k_scoresByIdrequi.get(params[1]).toString());
				}
			}
		}).start();
    }
    
    private void printlnDebug(String str){
		if (debugModeValue)
			System.out.println(str);
	}
}