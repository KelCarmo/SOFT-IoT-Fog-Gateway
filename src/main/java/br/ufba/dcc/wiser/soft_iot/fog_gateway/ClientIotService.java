package br.ufba.dcc.wiser.soft_iot.fog_gateway;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;

//import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBException;

public class ClientIotService {
	 
    private static int HTTP_COD_SUCESSO = 200;
    
    public String getApiIot(String url_Api) {
    	
    	 try {
             URL url = new URL(url_Api);
             HttpURLConnection con = (HttpURLConnection) url.openConnection();
  
             if (con.getResponseCode() != HTTP_COD_SUCESSO) {
                 throw new RuntimeException("HTTP error code : "+ con.getResponseCode());
             }
             
             BufferedReader br = new BufferedReader(new InputStreamReader((con.getInputStream())));
             String s = null;
             String all = null;
             while ((s=br.readLine())!=null)
                 {
             	all = s;
                 }             
  
             con.disconnect();
             return all;
  
         } catch (MalformedURLException e) {
             e.printStackTrace();
         } catch (IOException e) {
             e.printStackTrace();
         }
		
    	return null;
    }
 
    public static void main(String[] args) throws JAXBException {
    	ClientIotService novo = new ClientIotService();
    	System.out.println(novo.getApiIot("http://localhost:8181/cxf/iot-service/devices"));
       
    }
}
