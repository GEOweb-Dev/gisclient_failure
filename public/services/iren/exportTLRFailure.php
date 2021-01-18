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
    $headerValvola = "";
    $headerSott = "";
    $resultSott = "";
    $dalmino = "";
    foreach($arr as $jj) {
      if(substr($jj->tipo, 0, strlen("valvola")) == "valvola") {
        //teleriscaldamento.ocl_ut_h_baricentri baricentro da qui con objectid come chiave per bar_id--- io prendo baricentro
        $sql = "select a.fid, c.baricentro, '' as baricentro2, a.codice_impianto as cod_imp, b.cod_magliat as cod_magliat, b.sap_anno_costr,"
          ."b.sap_Cod_coib, b.sap_cod_man, b.sap_cod_posiz, b.sap_cod_tipo, b.sap_cod_uso, b.sap_data_funz, b.sap_diam, b.sap_id, b.sap_id_Valvola, b.sap_indirizzo, "
          ."b.sap_mese_costr, b.sap_prod, b.sap_serv, b.sap_serv_tipo, extract(year from a.data_Creazione) as sys_anno_ins, substring(a.comm_id from 5) as sys_cod_comm,"
          ."extract(day from a.data_creazione) as sys_giorno_ins, '' as sys_id_nodo, extract(month from a.data_creazione) as sys_mese_ins, "
          ."a.id_tipo_verso as sys_verso_nod from $schema.fcl_h_isolation_device as a left join $schema.ocl_ut_h_baricentri as c on a.bar_id=c.obj_id "
          ."inner join $schema.sap_h_isolation_device as b on "
          ."a.codice_sap = b.sap_id where a.codice_sap='".$jj->codice_sap."'";
        error_log($sql);
        $stmt = $db->prepare($sql);
        $stmt->execute();
        while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
          $emp = empty($headerValvola);
          foreach($row as $pk=>$pp) {
            if($emp)
              $headerValvola.=$pk.";"; 
            $dalmino .= $pp.";";
          }
          $dalmino = substr($dalmino, 0, strlen($dalmino)-1);
        }
        $resultValvola .= $dalmino."\r\n"; 
        $dalmino ="";
      }
      if(substr($jj->tipo, 0, strlen("sottostazione")) == "sottostazione") {
        $sql = "select a.fid as sys_gid, 'LIV.' || b.sap_cod_ucr as sap_cod_ucr, a.codice_impianto as cod_imp, "
          ."a.id_telecontrollo as cod_telec, "
          ."a.id_teleraffrescamento as cod_teler, '' as delta_p_calc, '' as delta_p_ril, a.potenza_calcolo, b.sap_Accesso, b.sap_anno_costr, b.sap_baricentro,"
          ."b.sap_cod_cesp, b.sap_cod_compet, b.sap_cod_forn, b.sap_cod_pozz, b.sap_cod_prop, b.sap_cod_utz, b.sap_cod_zona, b.sap_cout, b.sap_data_dis,b.sap_data_Funz, "
          ."b.sap_id, b.sap_indirizzo, b.sap_mese_costr, b.sap_num_sst, b.sap_pot, b.sap_prod, b.sap_Stato, b.sap_Vol, extract(year from a.data_creazione) as sys_anno_ins, "
          ."substring(a.comm_id from 5) as sys_cod_com, extract(day from a.data_creazione) as sys_giorno_ins, '' as sys_id_nodo, "
          ."extract(month from a.data_creazione) as sys_mese_ins, '' as sys_stato, '' as sys_update from $schema.fcl_h_service as a inner join "
          ."$schema.sap_h_service as b on a.codice_sap=b.sap_id and b.sap_cod_ucr is not null where a.codice_sap='".$jj->codice_sap."'";
        error_log($sql);
        $stmt = $db->prepare($sql);
        $stmt->execute();
        while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
          $emp = empty($headerSott);
          foreach($row as $pk=>$pp) {
            if($emp)
              $headerSott.=$pk.";"; 
            $dalmino .= $pp.";";
          }
          $dalmino = substr($dalmino, 0, strlen($dalmino)-1);
        }
        $resultSott .= $dalmino."\r\n"; 
        $dalmino = "";
      }
    }
    return (!empty($headerValvola) ? "SITAES_TLR_2070;\r\n".$headerValvola."\r\n".$resultValvola."\r\n" : "")
      .(!empty($headerSott) ? "SITAES_TLR_2230;\r\n".$headerSott."\r\n".$resultSott : "");
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
      foreach($jj as $key=>$value)
        $singleRow .= $value.";";
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
        for($k=0; $k<(count($cols)-2); $k++)
          $singleRow.=";";
        $singleRow.="\r\n";
        $result .= $singleRow;
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
