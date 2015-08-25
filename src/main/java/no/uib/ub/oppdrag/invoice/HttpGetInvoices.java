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
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
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
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import no.uib.ub.oppdrag.settings.InvoiceSettings;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;


public class HttpGetInvoices {
    static final Logger logger = Logger.getLogger(HttpGetInvoices.class.getName());
    
    public final static void main(String[] args) throws Exception {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        long bulkLength = 0;
        long startTime = System.currentTimeMillis();

        Client elasticsearchClient = null;

        //Get Redmine API Key from a user
        String redmineApiKey = System.getProperty("apiKey");
        
        if(redmineApiKey == null){
            logger.log(Level.INFO,
                          "Input parameter \"apiKey\" was not found. Using default API-key for authentication.");
            redmineApiKey = InvoiceSettings.DEFAULT_API_KEY;
        }
       else{
           logger.log(Level.INFO,
                           "Using apiKey \"{0}\" for authentication" , redmineApiKey);
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
                                throw new ClientProtocolException("The request could not be processed. " +
                                        status + ": " + response.getStatusLine().getReasonPhrase());
                            }
                        }

                    };
                   
                  //Establish HTTP Transport client to join the Elasticsearch cluster
                  elasticsearchClient = new TransportClient(ImmutableSettings.settingsBuilder()
                          .put("cluster.name",InvoiceSettings.CLUSTER_NAME).build())
                          .addTransportAddress(
                                  new InetSocketTransportAddress("127.0.0.1" , 9300));
                  
                  
                   ClusterHealthResponse hr = elasticsearchClient.admin().cluster().prepareHealth().get();
                   logger.log(Level.INFO, "Joining a cluster with settings: {0}", hr.toString());
                   
                  
                   //Create a bulk processor in order to index JSON documents in bulk.
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
                        
                        
                        //TO DO: Parse JSON strings using JsonPath
                        String invoiceURI = InvoiceSettings.INVOICE_BASE_URI + "/" + invoiceId;
                        String orderNumber = JsonPath.read(doc, "$.invoice.number");
                        String invoiceDate = JsonPath.read(doc, "$.invoice.invoice_date");
                        String invoiceStatus = JsonPath.read(doc, "$.invoice.status.name");
                        String assignedTo = JsonPath.read(doc, "$.invoice.assigned_to.name");
                        JSONArray invoiceLines = JsonPath.read(doc,"$.invoice.lines[*].description");
                        JSONArray customerObjects = JsonPath.read(doc, "$..custom_fields[?(@.name=='Navn')]");
                        JSONArray customerNames = JsonPath.read(customerObjects, "$.[*].value");
    
                        //Build a JSON object using Elasticsearch helpers
                        XContentBuilder jsonObject = jsonBuilder()
                                .startObject()
                                    .field("id" , invoiceURI)
                                    .field("order_number" , orderNumber)
                                    .field("invoice_date" , invoiceDate)
                                    .field("status" , invoiceStatus)
                                    .field("assigned_to" , assignedTo)
                                    .field("invoice_line", invoiceLines)
                                    .field("customer_name", customerNames)
                                .endObject();
                                       
                         //Index documents 
                         bulk.add(new IndexRequest(InvoiceSettings.INDEX_NAME , InvoiceSettings.INDEX_TYPE, invoiceURI)
                                .source(jsonObject.string()));
                        bulkLength++;

                        //System.out.println(jsonObject.string());
                     }
                 } 
           catch(ElasticsearchException esEx){
            logger.log(Level.SEVERE , 
                    String.format(
                            "Exception [%s]. Please make sure Elasticsearch is configured with same settings." 
                             , esEx.getLocalizedMessage(), InvoiceSettings.CLUSTER_NAME ));
             }
           catch(NoSuchAlgorithmException | 
                   KeyStoreException | 
                   KeyManagementException | 
                   IOException | 
                   PathNotFoundException |
                   InvalidJsonException ex){
               logger.log(Level.SEVERE , "Exception {0}", ex.getLocalizedMessage());
           }
          
           finally {
               
            logger.log(Level.INFO , String.format("\n==========================================="
                  + "\n\tTotal documents indexed: %s" 
                  + "\n\tIndex: %s"  
                  + "\n\tType: %s"    
                  + "\n\tTime taken: %s seconds"
                  +"\n===========================================",
                  bulkLength,InvoiceSettings.INDEX_NAME,InvoiceSettings.INDEX_TYPE, 
                 (System.currentTimeMillis()-startTime)/1000.0));
                
             httpClient.close();
             if(elasticsearchClient != null) elasticsearchClient.close();
       }    
      }

         /**Get all invoice Ids **/
          private static Set<Integer> getAllInvoiceIds(
                  String url,
                  String apiKey, 
                  CloseableHttpClient httpClient, 
                  ResponseHandler responseHandler) 
          {
            Set<Integer> invoiceIds = new HashSet<>();
            boolean proceed = true;
            try{  
                  logger.log(Level.INFO , String.format("Starting harvesting from URI [%s?key=%s]" , url , apiKey));
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
            catch(IOException | PathNotFoundException ex){
                logger.log(Level.SEVERE , "Exception occured during harvesting [{0}]", ex.getLocalizedMessage());
            }
            return invoiceIds;
          }
 }

