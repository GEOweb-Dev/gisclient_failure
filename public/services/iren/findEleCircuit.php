<?php
  
  require_once "findEleFailure.config.php";
  require_once "../../../config/config.php";
  $dbSchema=DB_SCHEMA;
  $db = GCApp::getDataDB("geoirenweb_ut/".$dbSchema);
  
/*  error_log($_POST["var"]);
  $arr = json_decode(html_entity_decode($_POST["var"]));
  $data = array_values(array_slice($arr,1,1));*/
  $arr = json_decode($_POST["var"]);
  $data = array_values(array_map(function(object $b){
	  return str_replace("condotta.","",$b->id);
  },array_filter($arr,function(object $b){
  	return property_exists($b,'tipo') && strcasecmp($b->tipo,"condotta")==0;
  })));
  //$_POST["var"]
if(!empty($data)){
	$sql = "select b.circ_no from elettricita.ocl_ut_e_mt_circuit_v b "
		."inner join elettricita.fcl_e_mt_section a on a.circ_id=b.obj_id "
		."where a.obj_id='".$data[0]."'";
	error_log($sql);
	$stmt = $db->prepare($sql);
	$stmt->execute();	
	if($row = $stmt->fetch(PDO::FETCH_ASSOC))
		$red = json_encode(array('val'=>$row["circ_no"]));
	else
		$red = json_encode(array('error'=>'Impossibile trovare un circuito per id '.$data[0]));
		/*$red = reportNew($http_host.$baseUrl,$mapset,$circId);//chi è mapset*/
} else
	//hrow new Exception("Non specificato uno nodo origine");
  $red = json_encode(array('error'=>"Non specificato un nodo origine per recuperare il circuito"));
  ob_clean();
  echo $red;
?>
