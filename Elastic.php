<?php
/*Author: Ibraheem Z. Abu-Kaff 
  Website: www.ibraheem-abukaff.com
  Version:1.0
  Last Update: 24/5/2015
  Notice: This class is a Model in "Yii Framework". To get advantage of it, you must place it in the Model folder.  
*/
require 'vendor/autoload.php';// you can get the elastic search library via PHP Composer

class Elastic
{	
	private $es;
	public function Elastic($ip,$port){
		$this->es = $this->connectToESServer($ip,$port);
	}
	
	//connecting to ElasticSearch Sevrer
	public function connectToESServer($ip,$port){
		$params = array();
		$params['hosts'] = array ("$ip:$port");
		$es = new Elasticsearch\Client($params);
		return $es;
	}
	
	// ( ^_^ Indexer function ^_^ )
	public function IndexSelectedRowsInTable($ids,$dbname,$table,$DBHandler) {
		$ids = implode(",",$ids);
		$sql = "select $ids from $table";
		$command=$DBHandler->createCommand($sql);
		$dataReader=$command->query();
		$rows=$dataReader->readAll();
		//begin indexing
		$counter = 1;
		foreach($rows as $row){
			$indexed = $this->es->index([
				'index'=>"$dbname",
				'type'=>"$table",
				'id'=>$counter++,
				'body'=>$row,
			]);
		}		
		echo $this->getCount($dbname,$table);	
	}
	
	//delete the entire index (index term in ElasticSearch is an opposite to db in mysql)
	public function deleteIndex($index){
		$deleteParams['index'] = "$index";
		$this->es->indices()->delete($deleteParams);
	}
	
	//make the selected index empty
	public function emptyByIndexAndType($index,$type){
		$query = $this->es->search([
			'index'=>"$index",
			'type'=>"$type",
			'body'=>[
				'query'=>[
					'bool'=>[
						'should'=>[		
						]
					]
				]
			],
		]);
		
		foreach($query['hits']['hits'] as $d){
			$deleteParams = array();
			$deleteParams["index"] = "$index";
			$deleteParams["type"] = "$type";
			$deleteParams['id'] = $d['_id'];
			$retDelete = $this->es->delete($deleteParams);
		}
		
	}
	
	//get the total number of documents===records inside index/type===dbname/tablename
	public function getCount($index,$type){
		$query = $this->es->count([
			'index'=>"$index",
			'type'=>"$type",
		]);
		return $query["count"];
	}	
	
	
	//fetching document by its mandatory vector(index,type,id)
	public function getSelectedDocument($index,$type,$id,$fields=array()){
		$query = $this->es->get([
			'index'=>"$index",
			'type'=>"$type",
			'id'=>"$id",
			'fields'=>$fields,
		]);
		return $query;
	}
	
	
	//get All docs
	public function getAllDocs($index,$type) {
		$query = $this->es->search([
			'index'=>"$index",
			'type'=>"$type",
			'body'=>[
				'query'=>[
					'bool'=>[
						'should'=>[	
							
						]
					]
				]
			],
		]);
		return $query;
	}
		
	//get All stored IDS in selected index/type
	public function getIds($index,$type){
		$ids = array();
		$query = $this->es->search([
			'index'=>"$index",
			'type'=>"$type",
			'body'=>[
				'query'=>[
					'bool'=>[
						'should'=>[	
						
						]
					]
				]
			],
		]);
		
		foreach($query['hits']['hits'] as $d){
			$ids[]= $d['_id'];
		}
		return $ids;
	}
	
}

/*
//In Controller class:

$elasticSearhHandler = new Elastic("X.X.X.X":"PORT_NUMBER");

$ids = array(1,2,3,4,5);//the IDS of each row you wanna index in target table.
$dbname="My_DB";//it will be the "index" name in ElasticSearch, which is equivalent to "database name" in MYSQL;
$table = "My_table";//it will be the "type" name in ElasticSearch, which is equivalent to "table" in MYSL;
$DBHandler = Yii::app()->db;//Connection

$numberOfIndexedRows = $elasticSearhHandler->IndexSelectedRowsInTable($ids,$dbname,$table,$DBHandler);
var_dump($numberOfIndexedRows);
//************************************************************************

//Now all selected rows from a selected table have been indexed, so to Fetch those documents you can do the following instruction:
$allIndexedDocs = $elasticSearhHandler->getAllDocs($dbname,$table);
var_dump($allIndexedDocs);
//************************************************************************


//To get the total number of documents in selected index and type:
var_dump($elasticSearhHandler->getCount($dbname,$table));
//************************************************************************

//to get all IDs of all indexed documents:
$allIDS = $elasticSearhHandler->getIds($dbname,$table);
var_dum($allIDS);
//************************************************************************

//To get One Document(for example the document with ID=3) from ElasticSearch Index:
var_dump($elasticSearhHandler->getSelectedDocument($dbname,$table,3));
//************************************************************************

//To make the index empty(I mean to delete all documents for a specific index in elastic search server), do as follows:
$elasticSearhHandler->emptyByIndexAndType($dbname,$table);
//************************************************************************

//To Delete the index permanently:
$elasticSearhHandler->deleteIndex($dbname);

*/

