<?php
  
  require_once "findEleFailure.config.php";
  require_once "../../../config/config.php";
  $dbSchema=DB_SCHEMA;
  $db = GCApp::getDataDB("geoirenweb_ut/".$dbSchema);

  $arr = html_entity_decode($_POST["var"]);
  $arr = json_decode($arr); 
  $operation = $_POST["operation"];
  $header = "";
  $result = "";

  //header
  $auth = $arr[0]->auth;
  $data = array_values(array_slice($arr,1));
  //rows
  usort($data, function($a, $b) {
    return strcmp($a->id, $b->id);
  });
  switch($operation) {
	case 2:
		$red = reportMT($db, $data, $auth, $SCHEMA);
		break;
    case 1:
      $red = reportBT($db, $data, $auth, $SCHEMA);
      break;
    default:
      break;
  }
//  error_log($red);  
  file_put_contents("/tmp/output.csv", $red);
  header("Cache-Control: public");
  header("Content-Description: File Transfer");
  header("Content-Disposition: attachment; filename=output.csv");
  header("Content-Type: application/octet-stream");
  header("Content-Transfer-Encoding: binary");
  header('Cache-Control: must-revalidate');
  header('Content-Length: ' . filesize('/tmp/output.csv'));
  readfile('/tmp/output.csv');

  function reportMT($db, $arr, $auth, $schema) {
	  $cols = array("c.obj_id"=>"objectId", "c.cod_cabina"=>"Cod_Cabina", "c.codice_sap"=>"Cod_Sap", "c.nomeasset"=>"Nome_Asset", "c.calc_n_pdf_bt"=>"N_POD_BT", "c.calc_pot_contr_bt"=>"POT_POD_BT", "c.id_tipologia_value"=>"Tipologia");
	  $header = implode(";",array_values($cols))."\r\n";
	  $result = "";
	  foreach($cols as $key=>$value)
		  $sqlList[] = ($key." as ".$value);
	  foreach($arr as $jj) {
		$idObj = substr($jj->id, strrpos($jj->id, ".")+1);
		$sql ="select ".implode(",",$sqlList)." from elettricita.fcl_E_installation c "
		."inner join elettricita.fcl_e_plant_area a on a.rel_oid=c.fid "
		."inner join elettricita.fcl_e_plant_Component b on a.obj_id=b.equipment_id "
		."where b.obj_id='$idObj'";
		error_log($sql);
		$stmt = $db->prepare($sql);
		$stmt->execute();
		while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			foreach($row as $pp)
				$result.=$pp.";";
			$result = substr($result,0,strlen($result)-1)."\r\n";
		}
	}
	return $header.$result;
  }
  
  function reportBT($db, $arr, $auth, $schema) {
	  //return "Ho chiamato BT";
	  $cols = array("a.pdf"=>"Punto_di_fornitura", "b.codice_sap_circuit"=>"Codice_SAP", "b.circ_no"=>"N_Circuito",/* ""=>"N_Sezione",*/ "a.codice_steel"=>"Codice_STEEL", "a.predisposto"=>"Predisposto", "b.codice_linea"=>"Codice_Linea_TLC");
	  if(!empty($auth)){
		  $ut = new GCUser();
		  $authorizedGroups = $ut->getGroups();
		  if($authorizedGroups!=null && in_array("geoweb_ee_ics", $authorizedGroups)) {
		  	error_log(".............----------------------------------".json_encode($authorizedGroups));
			$cols = array_merge($cols, array("a.ragione_sociale"=>"Ragione_Sociale", "a.comune"=>"Comune", "a.indirizzo"=>"Indirizzo", "a.piano"=>"Piano", "a.scala"=>"Scala", "a.interno"=>"Interno", "a.potenza_disponibile"=>"Potenza_Disponibile","a.potenza_imm"=>"Potenza_Immessa","a.stato_contratto"=>"Stato_Contratto", "a.data_inizio_contratto"=>"Inizio_Contratto", "a.data_fine_contratto"=>"Fine_Contratto", "a.cont_elettronico"=>"Contatore_Elettronico", "a.matricola"=>"Matricola_Contatore", "a.stato_contatore"=>"Stato_Contatore"));
		  }
	}
	$header = ("objectId;".implode(";",array_values($cols))."\r\n");
	$result = "";
	foreach($cols as $key=>$value)
		$sqlList[] = ($key." as ".$value);
	foreach($arr as $jj) {
		//if($operation==2) {
		//	$idObj = substr($jj->id, strpos($jj->id, ".")+1);
		//	$idObj = substr($idObj, 0, strpos($idObj, "."));
		//} else 
		$idObj = substr($jj->id, strrpos($jj->id, ".")+1);
		$sql = "select '$idObj',".implode(",",$sqlList)." from $schema.fcl_ut_e_pdf_v a "
			."inner join $schema.fcl_ut_e_circuit_utz_cd_v b on a.circ_id=b.fid "
			."where a.service_Id='".$idObj."' order by a.codice_steel";
		error_log($sql);
		$stmt = $db->prepare($sql);
		$stmt->execute();
		while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			foreach($row as $pp)
				$result.=$pp.";";
			$result = substr($result,0,strlen($result)-1)."\r\n";
		}
	}
	return $header.$result;
  }
?>
