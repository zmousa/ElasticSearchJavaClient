package com.zenus.elasticsearch.demo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.zenus.elasticsearch.demo.Index.DocumentEntryController;
import com.zenus.elasticsearch.demo.Index.IndexController;
import com.zenus.elasticsearch.demo.Index.QueryController;
import com.zenus.elasticsearch.demo.client.ESClient;
import com.zenus.elasticsearch.demo.domain.ESDocument;

public class ElasticsearchApp {

    private static final Logger LOG = LoggerFactory
	    .getLogger(ElasticsearchApp.class);
    
    public static final String indexName = "persons";
    public static final String typeName = "person";

    public static void main(String[] args) {
    	ESClient esClient = new ESClient();
    	
    	// Create Index
    	IndexController indexController = new IndexController(esClient);
    	indexController.createIndex(indexName, typeName);
    	
    	// Create Documents
    	ElasticsearchApp elasticsearchApp = new ElasticsearchApp();
    	elasticsearchApp.indexDocument(esClient);
    	
    	// Search...
    	QueryController queryController = new QueryController(esClient);
    	queryController.queryTerm(indexName, typeName, "حمد");
    	queryController.queryFuzzy(indexName, typeName, "حمد");
    	
    	esClient.shutdown();
    }

    public void indexDocument(ESClient esClient) {
		try {
			DocumentEntryController entryController = new DocumentEntryController(esClient);
	    	entryController.createDocument(new ESDocument("أحمد", "محمد", 30, ""), indexName, typeName);
	    	entryController.createDocument(new ESDocument("محمود", "علي", 30, ""), indexName, "Person");
	    	entryController.createDocument(new ESDocument("حمد", "عبد الملك", 30, ""), indexName, typeName);
	    	entryController.createDocument(new ESDocument("عبد الله", "هادي", 30, ""), indexName, typeName);
	    	entryController.createDocument(new ESDocument("كرم", "عبيدة", 30, ""), indexName, typeName);
	    	entryController.createDocument(new ESDocument("عبد الكريم", "عبد الهادي", 30, ""), indexName, typeName);
		} catch (final Exception ex) {
		    LOG.error("Exception: ", ex);
		}
		
	}
}
