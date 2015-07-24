<?php

// Debug mode
ini_set('display_startup_errors',1);
ini_set('display_errors',1);
error_reporting(-1);

// Connect to Cassandra
$cluster   = Cassandra::cluster()                 // connects to localhost by default
    ->withContactPoints('157.26.83.16')
    ->withPort(9042)
    ->build();

$keyspace  = 'twitter';
$session   = $cluster->connect($keyspace);        // create session, optionally scoped to a keyspace

// Default values
$tValue = 1;
$minVertices = 0;

//*************************************/
// Change T
//*************************************/
if (isset($_GET["value"])){
    $tValue = $_GET["value"];
}

//****************************************/
// Change minimum vertices in communities
//****************************************/
if (isset($_GET["minVertices"])){
    $minVertices = $_GET["minVertices"];
}

$whereStatementForVertice = null;
if($minVertices > 0){
    $whereStatementForVertice = "and nbv >= $minVertices";
}

//*************************************/
// Init
//*************************************/
$data = new stdClass();

$data->nodes = array();
$data->links = array(); 
$data->lda = array(); 
$data->cosine = array(); 

$indices = array();
$groups =  array();
$sg =  array();

function myfunction($num)
{
    return($num);
}

//*************************************/
// Get Source and Com ID -> Array
//*************************************/
$statement = new Cassandra\SimpleStatement("SELECT src_id, com_id, sg FROM twitter.communities where t = $tValue $whereStatementForVertice");
$result    = $session->execute($statement);

// On introduit tout les indices dans la map
foreach ($result as $row) {
    $indices[] = (int) $row['src_id'];
    $groups[] = (int) $row['com_id'];
    $sg[] = (int) $row['sg'];
}


//*************************************/
// Get Destination and Com ID -> Array
//*************************************/
$statement = new Cassandra\SimpleStatement("SELECT dst_id, com_id, sg FROM twitter.communities where t = $tValue $whereStatementForVertice");
$result    = $session->execute($statement);

foreach ($result as $row) {
    $indices[] = (int) $row['dst_id'];
    $groups[] = (int) $row['com_id'];
    $sg[] = (int) $row['sg'];
}

// Combine node and group
$c = array_combine($indices, $groups);
$csg = array_combine($indices, $sg);

// Get unique list of nodes
$map = array_map("myfunction", $indices);
$indices_unique = array_unique($map);
$nodes_index = array();


//*************************************/
// Create nodes
//*************************************/
foreach($indices_unique as $node){
    $nodes_index[] = $node;
    $data->nodes[] = array("name" => (int) $node, "group" => (int) $c[$node], "sg" => (int) $csg[$node]);
}


//*************************************/
// Create links
//*************************************/
$statement = new Cassandra\SimpleStatement("SELECT * FROM twitter.communities where t = $tValue $whereStatementForVertice");
$result    = $session->execute($statement);

foreach ($result as $row) {
    $data->links[] = array("source" => (int) array_search($row['src_id'],$nodes_index), "s" => (int) $row['src_id'], "d" => (int) $row['dst_id'], "target" => (int) array_search($row['dst_id'],$nodes_index), "value" => ((int) 1));
}

//*************************************/
// Get LDA
//*************************************/
$statement = new Cassandra\SimpleStatement("SELECT * FROM twitter.lda where t = $tValue");
$result    = $session->execute($statement);

foreach ($result as $row) {
    $data->lda[] = array("t" => (int) $row['t'], "sg" => (int) $row['sg'], "n_topic" => (int) $row['n_topic'], "words" => (string) $row['words']);
}

//*************************************/
// Get Cosine similarity
//*************************************/
$statementComm = new Cassandra\SimpleStatement("SELECT * FROM twitter.communities where t = $tValue $whereStatementForVertice");
$resultComm    = $session->execute($statementComm);

foreach ($resultComm as $row) {
    $data->cosine[] = array("t" => (int) $row['t'], "sg" => (int) $row['sg'], "cosines" => (string) $row['lda']);
}

echo json_encode($data); 
?>