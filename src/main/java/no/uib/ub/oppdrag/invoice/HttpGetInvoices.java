package no.uib.ub.oppdrag.invoice;


import com.jayway.jsonpath.Configuration;
import com.jayway.jsonpath.InvalidJsonException;
import com.jayway.jsonpath.JsonPath;
import com.jayway.jsonpath.PathNotFoundException;
import net.minidev.json.JSONArray;
import no.uib.ub.oppdrag.settings.InvoiceSettings;
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
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;

import javax.net.ssl.SSLContext;
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

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * This is a standalone application which harvests
 * invoices from Redmine CRM through HTTP and index them into Elasticsearch.
 * <br/>
 *
 * @author Hemed Ali Al Ruwehy
 * @since 17-08-2015
 * Location : The University of Bergen.
 **/
public class HttpGetInvoices {
        private static final Logger logger = Logger.getLogger(HttpGetInvoices.class.getName());
        private static long bulkLength = 0;

        /**
         * A method for getting all invoice IDs.
         * @param url URL to query from
         * @param apiKey an API key
         * @param httpClient HTTP client
         * @param responseHandler a response handler
         *
         * return a set of invoice IDs
         **/
        private static Set<Integer> getAllInvoiceIds(String url, String apiKey, CloseableHttpClient httpClient, ResponseHandler responseHandler) {
                Set<Integer> invoiceIds = new HashSet<>();
                boolean proceed = true;
                try {
                        logger.log(Level.INFO, String.format("Starting harvesting from URI [%s?key=%s]", url, apiKey));
                        for (int page = 1; proceed; page++) {
                                String urlWithApiKey = url
                                        + "?key=" + apiKey
                                        + "&page=" + page;
                                String responseBody = (String) httpClient.execute(new HttpGet(urlWithApiKey), responseHandler);
                                List<Integer> listOfInvoiceIds = JsonPath.read(responseBody, "$.invoices[*].id");
                                if (!listOfInvoiceIds.isEmpty()) {
                                        invoiceIds.addAll(listOfInvoiceIds);
                                }
                                //Terminate the loop if list is empty
                                if (listOfInvoiceIds.isEmpty()) {
                                        proceed = false;
                                }
                        }
                } catch (IOException | PathNotFoundException ex) {
                        logger.log(Level.SEVERE, "Exception occurred during harvesting [{0}]", ex.getLocalizedMessage());
                }
                return invoiceIds;
        }

        /**
         * Main method for executing the program
         **/
        public final static void main(String[] args) throws Exception {
                CloseableHttpClient httpClient = HttpClients.createDefault();
                long startTime = System.currentTimeMillis();
                Client transportClient;
                BulkProcessor bulkProcessor = null;

                //Get user inputs or fall to default settings if not provided.
                String clusterName = System.getProperty("cluster") != null ? System.getProperty("cluster") : InvoiceSettings.CLUSTER_NAME;
                String host = System.getProperty("host") != null ? System.getProperty("host") : InvoiceSettings.HOST_NAME;
                String indexName = System.getProperty("index") != null ? System.getProperty("index") : InvoiceSettings.INDEX_NAME;
                String typeName = System.getProperty("type") != null ? System.getProperty("type") : InvoiceSettings.INDEX_TYPE;

                String redmineApiKey = System.getProperty("apiKey");
                if (redmineApiKey == null) {
                        logger.log(Level.INFO, "Input parameter \"apiKey\" was not found. " + "Using default API-key for authentication.");
                        redmineApiKey = InvoiceSettings.DEFAULT_API_KEY;
                } else {
                        logger.log(Level.INFO, "Using apiKey \"{0}\" for authentication", redmineApiKey);
                }

                try {
                       /*
                         Trust any of the SSL certificate in this server.
                         Note that this is not recommended in production environment unless you really trust the resources.
                       */
                        SSLContext sslContext = SSLContexts.custom().loadTrustMaterial(null, new TrustStrategy() {
                                @Override
                                public boolean isTrusted(final X509Certificate[] chain, final String authType) throws CertificateException {
                                        return true;
                                }
                        }).useTLS().build();

                        SSLConnectionSocketFactory connectionFactory = new SSLConnectionSocketFactory(sslContext, new AllowAllHostnameVerifier());
                        httpClient = HttpClients.custom().setSSLSocketFactory(connectionFactory).build();

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
                        transportClient = new TransportClient(ImmutableSettings.settingsBuilder()
                                .put("cluster.name", clusterName).build())
                                .addTransportAddress(
                                        //new InetSocketTransportAddress(InetAddress.getLocalHost(), 9300));
                                        new InetSocketTransportAddress(host, 9300));

                        ClusterHealthResponse hr = transportClient.admin().cluster().prepareHealth().get();
                        logger.log(Level.INFO, "Joining a cluster with settings: {0}", hr.toString());

                        //Create a bulk processor in order to index JSON documents in bulk.
                        bulkProcessor = BulkProcessor.builder(transportClient, new BulkProcessor.Listener() {

                                @Override
                                public void beforeBulk(long executionId, BulkRequest request) {
                                        logger.log(Level.INFO, "Going to execute new bulk composed of {0} actions", request.numberOfActions());
                                }

                                @Override
                                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                                        logger.log(Level.INFO, "Executed bulk composed of {0} actions", request.numberOfActions());
                                        if (response.hasFailures()) {
                                                logger.log(Level.WARNING, "There was failures while executing bulk {0}", response.buildFailureMessage());
                                                for (BulkItemResponse item : response.getItems()) {
                                                        if (item.isFailed()) {
                                                                logger.log(Level.WARNING, String.format("Error for %s %s %s for %s operation: %s ", item.getIndex(),
                                                                        item.getType(), item.getId(), item.getOpType(), item.getFailureMessage()));
                                                        }
                                                }
                                        }
                                }

                                @Override
                                public void afterBulk(long executionId, BulkRequest request, Throwable thr) {
                                        logger.log(Level.WARNING, "Exception occured during bulk processing [{0}]", thr.getLocalizedMessage());
                                }
                        })
                                .setBulkActions(100)
                                .setFlushInterval(TimeValue.timeValueSeconds(5))
                                .build();

                        //Get all invoice IDs
                        Set<Integer> invoiceIds = getAllInvoiceIds(InvoiceSettings.INVOICE_BASE_URI + InvoiceSettings.JSON_FILE_EXTENSION, redmineApiKey, httpClient, responseHandler);

                        //Iterate through invoice IDs, build JSON objects and index them into Elasticsearch
                        for (Integer invoiceId : invoiceIds) {
                                String invoiceURIWithAPIKey = InvoiceSettings.INVOICE_BASE_URI
                                        + "/"
                                        + invoiceId
                                        + InvoiceSettings.JSON_FILE_EXTENSION
                                        + "?key=" + redmineApiKey;

                                String responseBody = httpClient.execute(new HttpGet(invoiceURIWithAPIKey), responseHandler);

                                //Parse JSON string
                                Object doc = Configuration.defaultConfiguration().jsonProvider().parse(responseBody);

                                //Parse JSON strings using JsonPath
                                String invoiceURI = InvoiceSettings.INVOICE_BASE_URI + "/" + invoiceId;
                                String orderNumber = JsonPath.read(doc, "$.invoice.number");
                                String invoiceDate = JsonPath.read(doc, "$.invoice.invoice_date");
                                String invoiceStatus = JsonPath.read(doc, "$.invoice.status.name");
                                String assignedTo = JsonPath.read(doc, "$.invoice.assigned_to.name");
                                JSONArray invoiceLines = JsonPath.read(doc, "$.invoice.lines[*].description");
                                JSONArray customerObjects = JsonPath.read(doc, "$..custom_fields[?(@.name=='Navn')]");
                                JSONArray customerNames = JsonPath.read(customerObjects, "$.[*].value");

                                //Build a JSON object using Elasticsearch helpers
                                XContentBuilder jsonObject = jsonBuilder()
                                        .startObject()
                                        .field("id", invoiceURI)
                                        .field("order_number", orderNumber)
                                        .field("invoice_date", invoiceDate)
                                        .field("status", invoiceStatus)
                                        .field("assigned_to", assignedTo)
                                        .field("invoice_line", invoiceLines)
                                        .field("customer_name", customerNames)
                                        .endObject();

                                //Create index request
                                IndexRequest indexRequest = new IndexRequest(indexName, typeName, invoiceURI)
                                        .source(jsonObject.string());
                                bulkProcessor.add(indexRequest);
                                bulkLength++;
                        }
                } catch (ElasticsearchException esEx) {
                        logger.log(Level.SEVERE,
                                String.format(
                                        "Exception [%s]. Please make sure Elasticsearch is running"
                                        , esEx.getLocalizedMessage()));
                } catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException |
                        IOException | PathNotFoundException | InvalidJsonException ex) {
                        logger.log(Level.SEVERE, "Exception [{0}]", ex.getLocalizedMessage());
                } finally {
                        httpClient.close();
                        if (bulkProcessor != null) {
                                bulkProcessor.flush();
                                bulkProcessor.close();
                        }
                        logger.log(Level.INFO, String.format("\n==========================================="
                                        + "\n\tTotal documents indexed: %s"
                                        + "\n\tIndex: %s"
                                        + "\n\tType: %s"
                                        + "\n\tTime taken: %s seconds"
                                        + "\n===========================================",
                                bulkLength, indexName, typeName, (System.currentTimeMillis() - startTime) / 1000.0));
                }
        }

}

