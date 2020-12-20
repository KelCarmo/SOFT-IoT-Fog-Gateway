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


public class ListenerTopK implements IMqttMessageListener {
	
	private boolean debugModeValue;
	private ControllerImpl impl;
	private ClientMQTT clienteMQTT_Up;
	private ClientMQTT clienteMQTT_Host;
	
	/**
	 * 
	 * @param impl
	 * @param clienteMQTT_Up
	 * @param clienteMQTT_Host
	 * @param topico
	 * @param qos
	 */
    public ListenerTopK(ControllerImpl impl, ClientMQTT clienteMQTT_Up, ClientMQTT clienteMQTT_Host, String topico, int qos) {
        this.clienteMQTT_Up = clienteMQTT_Up;
        this.clienteMQTT_Host = clienteMQTT_Host;
        this.clienteMQTT_Up.subscribe(qos, this, topico);
        this.impl = impl;
    }
    
    public ListenerTopK(ControllerImpl impl) {       
        this.impl = impl;
    }
    
    @Override
    public synchronized void messageArrived( final String topic, final MqttMessage message) throws Exception {
    	
    	final String [] params = topic.split("/");
    	final int k = Integer.valueOf(params[2]);
    	System.out.println("=========================================");
    	
        new Thread(new Runnable() {
			public void run() {
				String messageContent = new String(message.getPayload());
				printlnDebug("topic: " + topic + "message: " + messageContent);
				
				// Repassa a requisição vindo de um fog_gateway de cima para o broker local, com isso o bottom_broker terá conhecimento do cálculo.
				if(params[0].equals("TOP_K_HEALTH")) {
					byte[] b = messageContent.getBytes();
					clienteMQTT_Host.publicar(topic, b, 1);
					List<String> n = new ArrayList<String>();
					impl.topk_k_scoresByIdrequi.put(params[1], n);
					
					System.out.println("REQUI TOP-K REcebido: " + messageContent);
					
					// Inicia o processo de calcular o TOP_K
					impl.calculateTopK(params[1], k);
				}
			}
		}).start();
    }
    
    private void printlnDebug(String str){
		if (debugModeValue)
			System.out.println(str);
	}

}