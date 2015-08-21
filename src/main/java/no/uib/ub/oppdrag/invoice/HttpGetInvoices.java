/*
 This is a standalone application which harvests 
 invoices from Redmine CRM through HTTP and
 index them to Elasticsearch. <br>

 @author Hemed Ali 
 @since 17-08-2015
 Location : The University of Bergen.
*/



package no.uib.ub.oppdrag.invoice;

import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.JsonPath;
import java.io.IOException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.net.ssl.SSLContext;
import net.minidev.json.JSONArray;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.conn.ssl.AllowAllHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.SSLContexts;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import no.uib.ub.oppdrag.settings.InvoiceSettings;


public class HttpGetInvoices {
    
    public final static void main(String[] args) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();   
        
       //Create cluster settings for Elasticsearch
        Settings clusterSettings = ImmutableSettings.settingsBuilder()
                .put("cluster.name" , "elasticsearch").build();

       //Establish HTTP Transport client to join the Elasticsearch cluster
        Client elasticsearchClient = new TransportClient(clusterSettings)
               .addTransportAddress(new InetSocketTransportAddress("127.0.0.1" , 9300));

        //Get Redmine API Key from a user
        String redmineApiKey = System.getProperty("apiKey");
        
        if(redmineApiKey == null){
            Logger.getLogger(HttpGetInvoices.class.getName())
                    .log(Level.INFO,
                          "Input parameter \"apiKey\" was not found. Using default API-key for authentication.");
            redmineApiKey = InvoiceSettings.DEFAULT_API_KEY;
        }
       else{
           Logger.getLogger(HttpGetInvoices.class.getName())
                   .log(Level.INFO,
                           "Using \"apiKey\" %s% for authentication" , redmineApiKey);
        }
        
           try{
                  /*Trust any of the SSL certificate.
                    Note that this is not recommended in production environment unless you really trust the resources.
                   */
                    SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy(){
                    @Override
                    public boolean isTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
                        return true;
                       }
                     })
                            .useTLS()
                            .build();

                    SSLConnectionSocketFactory  connectionFactory = new SSLConnectionSocketFactory(
                                                                   sslContext, new AllowAllHostnameVerifier());
                    httpClient = HttpClients
                            .custom()
                            .setSSLSocketFactory(connectionFactory)
                            .build();

                     //Create a custom response handler
                     ResponseHandler<String> responseHandler = new ResponseHandler<String>() {
                        @Override
                        public String handleResponse(
                                final HttpResponse response) throws ClientProtocolException, IOException {
                            int status = response.getStatusLine().getStatusCode();
                            if (status >= 200 && status < 300) {
                                HttpEntity entity = response.getEntity();
                                return entity != null ? EntityUtils.toString(entity) : null;
                            } else {
                                throw new ClientProtocolException(status + ": " + response.getStatusLine().getReasonPhrase());
                            }
                        }

                    };

                   //Instantiate a bulk processor in order to index JSON documents in bulk.
                    BulkProcessor bulk = BulkProcessor.builder(elasticsearchClient, new BulkProcessor.Listener() {

                        @Override
                        public void beforeBulk(long executionId, BulkRequest request) {}

                        @Override
                        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {}

                        @Override
                        public void afterBulk(long executionId, BulkRequest request, Throwable thr) {

                         }
                      })
                         .setBulkActions(50)
                         .setFlushInterval(TimeValue.timeValueSeconds(5))
                         .build();

                     //Get all invoice ids
                     Set<Integer> invoiceIds = getAllInvoiceIds(
                             InvoiceSettings.INVOICE_BASE_URI + InvoiceSettings.JSON_FILE_EXTENSION,
                             redmineApiKey,
                             httpClient, 
                             responseHandler);

                    for(Integer invoiceId : invoiceIds){  
                        String invoiceURIWithAPIKey = InvoiceSettings.INVOICE_BASE_URI 
                                                      + "/" 
                                                      + invoiceId 
                                                      + InvoiceSettings.JSON_FILE_EXTENSION 
                                                      + "?key=" + redmineApiKey;

                        String responseBody = httpClient.execute(new HttpGet(invoiceURIWithAPIKey), responseHandler);
   
                        //Parse JSON string 
                        Object doc = Configuration.defaultConfiguration().jsonProvider().parse(responseBody);
                        
                        
                        //TO DO: Create a method to build a JSON object here.
                        String invoiceURI = InvoiceSettings.INVOICE_BASE_URI + "/" + invoiceId;
                        String orderNumber = JsonPath.read(doc, "$.invoice.number");
                        String invoiceDate = JsonPath.read(doc, "$.invoice.invoice_date");
                        String invoiceStatus = JsonPath.read(doc, "$.invoice.status.name");
                        String assignedTo = JsonPath.read(doc, "$.invoice.assigned_to.name");
                        JSONArray invoiceLines = JsonPath.read(doc,"$.invoice.lines[*].description");
                        JSONArray customerObjects = JsonPath.read(doc, "$..custom_fields[?(@.name=='Navn')]");
                        JSONArray customerNames = JsonPath.read(customerObjects, "$.[*].value");
                        
                        /**
                         bulk.add(new IndexRequest(InvoiceSettings.INDEX_NAME , InvoiceSettings.INDEX_TYPE, invoiceURI)
                                .source(responseBody));
                        **/ 

                        System.out.println(invoiceURI + " " + customerNames.toString() + "  " + orderNumber + " ");
                     }

                 } 
          finally {
            httpClient.close();
            elasticsearchClient.close();
       }    
      }

         //Get all invoice Ids
          private static Set<Integer> getAllInvoiceIds(
                  String url,
                  String apiKey, 
                  CloseableHttpClient httpClient, 
                  ResponseHandler responseHandler) 
          {
            Set<Integer> invoiceIds = new HashSet<>();
            boolean proceed = true;
            try{
                  for(int page = 1; proceed; page++){ 
                       String urlWithApiKey =  url 
                                            + "?key=" + apiKey
                                            + "&page=" + page;
                       String responseBody = (String)httpClient.execute(new HttpGet(urlWithApiKey), responseHandler);
                       List<Integer> listOfInvoiceIds = (List<Integer>)JsonPath.read(responseBody, "$.invoices[*].id");
                       if(!listOfInvoiceIds.isEmpty()){
                          invoiceIds.addAll(listOfInvoiceIds);
                       }
                       //Terminate the loop if list is empty
                       if(listOfInvoiceIds.isEmpty()){proceed = false;}
                    }
            }
            catch(IOException ex){ex.getLocalizedMessage();}
            return invoiceIds;
          }
 }

