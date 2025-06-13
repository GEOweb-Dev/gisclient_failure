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

  $tipo = min(array_values(array_map(function(object $b){
	  return $b->tipoCondotta;
  },array_filter($arr,function(object $b){
  	return property_exists($b,'tipo') && strcasecmp($b->tipo,"condotta")==0;
  }))));
  //$_POST["var"]
  if(!empty($data)){
	 $codiceCircuito = array();
	foreach($data as $single) {
		$sql = "select b.circ_no from elettricita.".(($tipo==1) ? "ocl_ut_e_bt_circuit" : "ocl_ut_e_mt_circuit_v")." b "
			."inner join elettricita.".(($tipo==1) ? "fcl_e_bt_section" : "fcl_e_mt_section")." a on a.circ_id=b.obj_id "
			."where a.obj_id='".$single."'";
		error_log($sql);
		$stmt = $db->prepare($sql);
		$stmt->execute();
		if($row = $stmt->fetch(PDO::FETCH_ASSOC))
			$codiceCircuito[] = $row["circ_no"];
	}
	 if(!empty($codiceCircuito))
		$red = json_encode(array('val'=>array_unique($codiceCircuito),'tipoElaborato'=>(($tipo==1) ? 190 : 182)));
		/*	$red = json_encode(array('val'=>$row["circ_no"],'tipoElaborato'=>(($tipo==1) ? 190 : 182)));*/
	else
		$red = json_encode(array('error'=>'Impossibile trovare circuiti dalle condotte passate'));
		/*$red = reportNew($http_host.$baseUrl,$mapset,$circId);//chi è mapset*/
} else
	//hrow new Exception("Non specificato uno nodo origine");
  	$red = json_encode(array('error'=>"Non specificata una condotta da cui recuperare il circuito"));
  ob_clean();
  echo $red;
?>
