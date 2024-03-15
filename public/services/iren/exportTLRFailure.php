<?php
  
  require_once "findTLRFailure.config.php";
  require_once "../../../config/config.php";
  $dbSchema=DB_SCHEMA;
  $db = GCApp::getDataDB("geoirenweb_ut/".$dbSchema);

  $arr = html_entity_decode($_POST["var"]);
  $arr = json_decode($arr); 
  $operation = $_POST["operation"];
  $header = "";
  $result = "";

  //header
  foreach($arr[0] as $key=>$value)
    $header.= $key.";";
  $header = substr($header, 0, strlen($header)-1);

  //rows
  usort($arr, function($a, $b) {
    return strcmp($a->id, $b->id);
  });
  switch($operation) {
    case 3:
      $red = reportSottostazioni($db, $arr, $SCHEMA);
      break;
    case 2:
      $red = reportComponenti($db, $arr, $SCHEMA);
      break;
    default:
      foreach($arr as $jj) {
        foreach($jj as $key=>$value)
          $result .= $value.";";
        $result = substr($result, 0, strlen($result)-1)."\r\n";
      }
      $red = $header."\r\n".$result;
      break;
  }
  error_log($red);  
  file_put_contents("/tmp/output.csv", $red);
  header("Cache-Control: public");
  header("Content-Description: File Transfer");
  header("Content-Disposition: attachment; filename=output.csv");
  header("Content-Type: application/octet-stream");
  header("Content-Transfer-Encoding: binary");
  header('Cache-Control: must-revalidate');
  header('Content-Length: ' . filesize('/tmp/output.csv'));
  readfile('/tmp/output.csv');

  function reportComponenti($db, $arr, $schema) {
    $resultValvola = "";
    $resultCamera = "";
    $resultPozzetto = "";
    $resultPompaggio = "";
    $resultStazione = "";
    $resultCentrale = "";
    $header = "";
    $dalmino = "";
    foreach($arr as $jj) {
      $idObj = substr($jj->id, strrpos($jj->id, ".") + 1);
      error_log($jj->tipo);
      if(substr($jj->tipo, 0, strlen("valvola")) == "valvola") {
	$sql = "select concat('fcl_h_isolation_device_',id_tipologia) as id_classe, codice_sap as sap_id, '".$jj->tipo."' as descrizione, case when id_territorio=1 then codice_valvola else null end as codice_oggetto from $schema.fcl_h_isolation_device where fid=".$idObj." and id_stato=3";
        error_log($sql);
        $stmt = $db->prepare($sql);
        $stmt->execute();
        while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
          $emp = empty($header);
          foreach($row as $pk=>$pp) {
            if($emp)
              $header.=$pk.";"; 
            $dalmino .= $pp.";";
          }
          $dalmino = substr($dalmino, 0, strlen($dalmino)-1);
        }
        $resultValvola .= $dalmino."\r\n"; 
        $dalmino ="";
      }
      if(substr($jj->tipo, 0, strlen("camera"))== "camera" || substr($jj->tipo, 0, strlen("pozzetto"))== "pozzetto") {
        $sql = "select concat('fcl_h_component_',gtype_id) as id_classe, codice_sap as sap_id, '".$jj->tipo."' as descrizione, '' as codice_oggetto from $schema.fcl_h_component where fid=".$idObj." and id_stato=3";
        error_log($sql);
        $stmt = $db->prepare($sql);
        $stmt->execute();
        while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
          $emp = empty($header);
          foreach($row as $pk=>$pp) {
            if($emp)
              $header.=$pk.";"; 
            $dalmino .= $pp.";";
          }
          $dalmino = substr($dalmino, 0, strlen($dalmino)-1);
	}
	if(substr($jj->tipo, 0, strlen("camera"))=="camera")
          $resultCamera .= $dalmino."\r\n"; 
	else 
          $resultPozzetto .= $dalmino."\r\n";
        $dalmino ="";
      }
      if(substr($jj->tipo, 0, strlen("stazione"))== "stazione" || substr($jj->tipo, 0, strlen("centrale"))== "centrale") {
        $sql = "select concat('fcl_h_installation_',gtype_id) as id_classe, descrizione as sap_id, '".$jj->tipo."' as descrizione, '' as codice_oggetto from $schema.fcl_h_installation where fid=".$idObj." and id_stato=3";
        error_log($sql);
        $stmt = $db->prepare($sql);
        $stmt->execute();
        while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
          $emp = empty($header);
          foreach($row as $pk=>$pp) {
            if($emp)
              $header.=$pk.";"; 
            $dalmino .= $pp.";";
          }
          $dalmino = substr($dalmino, 0, strlen($dalmino)-1);
	}
	if(substr($jj->tipo, 0, strlen("stazione"))=="stazione")
          $resultStazione .= $dalmino."\r\n"; 
	else 
          $resultCentrale .= $dalmino."\r\n";
        $dalmino ="";
      }
    }
    return $header."\r\n".$resultValvola.$resultCamera.$resultPozzetto.$resultStazione.$resultCentrale;
  }
  
  function reportSottostazioni($db, $arr, $schema) {
    $result = "";
    $dalmino = "";
    $cols = array('substring(sap_id from 1 for 5)'=>'ZONA', 'sap_num_sst'=>'NUMERO_SOTTOST', 'sap_indirizzo'=>'INDIRIZZO', 'sap_cout'=>'COUTENZA', 'sap_pot'=>'POTENZA_IMPEGNATA',
      'sap_vol'=>'VOLUMETRIA', 'sap_cod_utz'=>'TIPOLOGIA_UTENZA', 'sap_cod_forn'=>'TIPOLOGIA_FORNITURA', 'sap_cod_cesp'=>'TIPO_CESPITE', 'sap_id'=>'SEDE_TECNICA',
      "('LIV.' || sap_cod_ucr)"=>'SENSIBILITA');
    $header = implode(";", array_values($cols))."\r\n";
    $volumeTotale = 0;
    $tipologiaArr = array();
    $fornituraArr = array();
    $livelloArr = array();
    $sqlList = array();
    foreach($cols as $key=>$value)
      $sqlList[] = ($key." as ".$value);
    foreach($arr as $jj) {
      $singleRow ="";
      $dalmino = "";
      $sql = "select ".implode(",",$sqlList).", case when sap_cod_ucr is not null then 0 else 1 end as ording"
        ." from $schema.sap_h_service where sap_id='".$jj->codice_sap."' order by ording";
      error_log($sql);
      $stmt = $db->prepare($sql);
      $stmt->execute();
      //foreach($jj as $key=>$value)
      //$singleRow .= $value.";";
      $currentSens = null;
      while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
        $currentSens = ($row['sensibilita']==null ? $currentSens : $row['sensibilita']);
        $volumeTotale += (is_null($row['volumetria']) ? 0 : $row['volumetria']);
        if(!is_null($row['tipologia_utenza'])) {
          if(array_key_exists($row['tipologia_utenza'], $tipologiaArr))
            $tipologiaArr[$row['tipologia_utenza']]++;
          else
            $tipologiaArr[$row['tipologia_utenza']] = 1;
        }
        if(!is_null($row['tipologia_fornitura'])) {
          if(array_key_exists($row['tipologia_fornitura'], $fornituraArr))
            $fornituraArr[$row['tipologia_fornitura']]++;
          else
            $fornituraArr[$row['tipologia_fornitura']] = 1;
        }
        if(!is_null($row['sensibilita'])) {
          if(array_key_exists($row['sensibilita'], $livelloArr))
            $livelloArr[$row['sensibilita']]++;
          else
            $livelloArr[$row['sensibilita']] = 1;
        }
        foreach($row as $pk=>$pp)
          if(strcmp($pk, "sensibilita")==0)
            $dalmino .= $currentSens.";";
          else if(strcmp($pk,"ording")!=0)
            $dalmino .= $pp.";";
        $dalmino = substr($dalmino, 0, strlen($dalmino)-1)."\r\n";
      }
      if(empty($dalmino)) {
        //for($k=0; $k<(count($cols)-2); $k++)
        //$singleRow.=";";
        $singleRow.="\r\n";
        //$result .= $singleRow;
      } else       
        $result .= $dalmino;/*substr($dalmino, 0, strlen($dalmino)-1)."\r\n";*/
    }
    $result.= "\r\n\r\n";
    $result.= "NUMERO SOTTOSTAZIONI;".count($arr)."\r\nVOLUMETRIA TOTALE;".$volumeTotale."\r\n\r\n";
    $result.= "TIPOLOGIA;;\r\n";
    ksort($tipologiaArr);
    foreach($tipologiaArr as $key=>$value)
      $result.= $key.";".$value."\r\n";
    $result.="\r\n";
    $result.= "UTILIZZO;;\r\n";
    ksort($fornituraArr);
    foreach($fornituraArr as $key=>$value)
      $result.= $key.";".$value."\r\n";
    $result.="\r\n";
    $result.= "UTENZE SENSIBILI;;\r\n";
    ksort($livelloArr);
    foreach($livelloArr as $key=>$value) 
      $result.= $key.";".$value."\r\n"; 
    return $header.$result;
  }
?>
