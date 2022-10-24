<?php
require_once "failureFunction.php";
require_once "find".$_REQUEST["domain"]."Failure.config.php";
require_once "../../../config/config.php";
$otherListStr = "('".implode("','", $OTHERS)."')";//"('altro', 'valvola sfiato', 'valvola drenaggio')";

$dbSchema=DB_SCHEMA;
$SRS = array(
	'3003'=>'+proj=tmerc +lat_0=0 +lon_0=9 +k=0.999600 +x_0=1500000 +y_0=0 +ellps=intl +units=m +no_defs +towgs84=-104.1,-49.1,-9.9,0.971,-2.917,0.714,-11.68',
	'900913'=>'+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +k=1.0 +units=m +nadgrids=@null +towgs84=0,0,0 +no_defs',
	'3857'=>'+proj=merc +a=6378137 +b=6378137 +lat_ts=0.0 +lon_0=0.0 +x_0=0.0 +y_0=0 +units=m +k=1.0 +nadgrids=@null +no_defs',
	'4326'=>'+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs',
	'25832'=>'+proj=utm +zone=32 +ellps=GRS80 +units=m +no_defs'
);

$x = floatval($_REQUEST["x"]);
$y = floatval($_REQUEST["y"]);

$db = GCApp::getDataDB("geoirenweb_ut/".$dbSchema);
$stmt = $db->prepare("SET statement_timeout TO $TIME_OUT;");
$stmt->execute();

$popoint = null;
$geom = "the_geom";
if($_REQUEST["srs"] == "EPSG:".$GEOM_SRID){
	$point ="'SRID=".$GEOM_SRID.";POINT($x $y)'";
	//$geom = "the_geom";
	if(!empty($_REQUEST["barN"])){
		$a = explode(",",$_REQUEST['barN']);
		$popoint = "'SRID".$GEOM_SRID.";POINT(".trim($a[0])." ".trim($a[1]).")'";
	}
} else {
	$srid = (explode(':', $_REQUEST["srs"]));
	$srid = $srid[1];
	//20211007 MZ -> modifica per deprecamento funzione postgis_transform_geometry
	$stmt = $db->prepare("select st_astext(st_transform(st_geomfromtext('POINT($x $y)',$srid),$GEOM_SRID))");
	$stmt->execute();
	$point = "st_geomfromtext('".$stmt->fetch(PDO::FETCH_NUM)[0]."',$GEOM_SRID)";
	//$geom = "the_geom";
	//fine 20211007 MZ
	if(!empty($_REQUEST["barN"])) { 
		$a = explode(",",$_REQUEST['barN']);
	        //20211007 MZ	
		$stmt = $db->prepare("select st_astext(st_transform(st_geomfromtext('POINT(".trim($a[0])." ".trim($a[1]).")',$srid),$GEOM_SRID))");
		$stmt->execute();
		$popoint = "st_geomfromtext('".$stmt->fetch(PDO::FETCH_NUM)[0]."',$GEOM_SRID)";
		//fine 20211007 MZ
	}
}

$includeVertex = $_REQUEST["include"];
if($popoint!=null) {
	$sql = "select id_nodo from grafo.nodi_".$_REQUEST['domain']." where ST_DISTANCE($popoint, $geom) <".floatVal($_REQUEST["distance"])." and tipo_nodo='altro' "
		." ORDER BY ST_DISTANCE($popoint,$geom) LIMIT 1;";
	error_log($sql);
	$stmt = $db->prepare($sql);
	$stmt->execute();
	$ex = false;
	while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
		$includeVertex .= (empty($includeVertex) ? "" : ",").$row['id_nodo'];
		$ex = true;
	}
	if(!$ex){
		header("Content-Type: application/json");
		die(json_encode(array("error"=>"Non è possibile posizionare una barriera su un nodo tipato del grafo.")));
	}
}
$includeVertex = empty($includeVertex) ? false : $includeVertex;
$bVertex = $includeVertex ? explode(",",$includeVertex) : array();

//ANALISI DEL GRAFO
$excludeVertex = false;
$aVertex=array();
//20211006 MZ
//Elementi da escludere:
$excludeVertex = (!empty($_REQUEST["exclude"]) || !empty($EXCLUDED_ELEMENTS)) ? ("SELECT id_nodo from grafo.nodi_".$_REQUEST['domain']
	.(!empty($_REQUEST["exclude"]) ? " where id_elemento in (".implode(",", array_map(function($a){
		return is_numeric($a) ? $a : "'".$a."'";
	},explode(",",$_REQUEST["exclude"]))).") " : "")
	.(!empty($EXCLUDED_ELEMENTS) ? ((!empty($_REQUEST["exclude"]) ? "or": " where")." tipo_nodo in ('".implode("','", $EXCLUDED_ELEMENTS)."')") : "")) : false;
//fine 20211006
//CON INCLUDI I NODI INVECE...
$joinFilter = "((sg.da_tipo not in $otherListStr ".($excludeVertex ? " AND sg.da_nodo not in ($excludeVertex)" : "").") OR (sg.a_tipo not in $otherListStr ".($excludeVertex ? " AND sg.a_nodo not in ($excludeVertex)" : "")."))".($includeVertex ? " OR (sg.da_tipo in $otherListStr and sg.da_nodo in ($includeVertex)) OR (sg.a_tipo in $otherListStr and sg.a_nodo in ($includeVertex))" : "");


//TROVO LA CONDOTTA SELEZIONATA COME ARCO DEL GRAFO - QUI!!
//CON INCLUDI I NODI INVECE...
$ff = "((da_tipo not in $otherListStr ".($excludeVertex ? " AND da_nodo NOT IN ($excludeVertex)" : "").") OR (a_tipo not in $otherListStr ".($excludeVertex ? " AND a_nodo NOT IN ($excludeVertex)" : "")."))".($includeVertex ? " OR (da_tipo in $otherListStr and da_nodo in ($includeVertex)) OR (a_tipo in $otherListStr and a_nodo in ($includeVertex))" : "");
$sql = ("SELECT id_arco, case when ($ff) then 1 else 0 end as flag FROM grafo.archi_".$_REQUEST['domain']." as sg "
	."WHERE ST_DISTANCE($point,$geom) < ".floatval($_REQUEST["distance"])
	." ORDER BY ST_DISTANCE($point,$geom) LIMIT 1;");
error_log($sql);
$stmt = $db->prepare($sql);
$stmt->execute();
$row = $stmt->fetch(PDO::FETCH_ASSOC);
$selectedPipe = $row["id_arco"];
$flag = $row["flag"];
if(!$selectedPipe)
	die();

//PATCH PERCHE' NON ATTIVA LA RICORSIONE SE INZIA DA UN ARCO TERMINALE: TROVO IL PRIMO ARCO NON TERMINALE SE QUELLO SELEZIONATO NON LO E'
if($flag == 1) {
	$sql = "SELECT sg.id_arco FROM grafo.archi_".$_REQUEST['domain']." g, grafo.archi_".$_REQUEST['domain']." sg WHERE g.id_arco = $selectedPipe AND g.id_arco <> sg.id_arco "
		."AND (sg.a_nodo = g.da_nodo OR sg.a_nodo = g.a_nodo OR sg.da_nodo = g.da_nodo OR sg.da_nodo = g.a_nodo)";
	//CON INCLUDI I NODI INVECE...
	$sql.= " AND (((sg.da_tipo in $otherListStr ".($includeVertex ? "and sg.da_nodo not in ($includeVertex)" : "").") "
		.($excludeVertex ? "or sg.da_nodo in ($excludeVertex)": "").") AND ((sg.a_tipo in $otherListStr ".($includeVertex ? " and sg.a_nodo not in ($includeVertex)" : "").") "
		.($excludeVertex ? "or sg.a_nodo in ($excludeVertex)" : "")."));";
	$stmt = $db->prepare($sql);
	$stmt->execute();
	$row = $stmt->fetch(PDO::FETCH_ASSOC);
	$selectedNextPipe = !$row ? false : $row["id_arco"];
	//CASO DI 2 ARCHI CON NODI TERMINALI UNITI DA NODO NON TERMINALE (?????)  VALVOLA - ALTRO - VALVOLA
	if(!$selectedNextPipe) {
		$sql="SELECT id_arco,id_elemento,da_nodo,a_nodo, da_tipo, a_tipo, $TIPO_FIELD as tipo FROM grafo.archi_".$_REQUEST['domain']." WHERE id_arco = $selectedPipe UNION "
			."SELECT g.id_arco, g.id_elemento, g.da_nodo, g.a_nodo, g.da_tipo, g.a_tipo, ".($TIPO_FIELD==1 ? $TIPO_FIELD : "g.".$TIPO_FIELD)." as tipo FROM grafo.archi_".$_REQUEST['domain']." g, grafo.archi_".$_REQUEST['domain']." sg "
			."WHERE sg.id_arco = $selectedPipe AND g.id_arco <> sg.id_arco";
		//CON INCLUDI I NODI INVECE
		$sql.= " AND ((g.a_nodo=sg.da_nodo AND ((g.a_tipo in $otherListStr ".($includeVertex ? " and g.a_nodo not in ($includeVertex)" : "").") ".($excludeVertex ? " or g.a_nodo in ($excludeVertex)" : "").")) OR (g.da_nodo=sg.a_nodo AND ((g.da_tipo in $otherListStr ".($includeVertex ? " and g.da_nodo not in ($includeVertex)" : "").") ".($excludeVertex ? " or g.da_nodo in ($excludeVertex)" : "").")) OR (g.a_nodo=sg.a_nodo AND ((g.a_tipo in $otherListStr".($includeVertex ? " and g.a_nodo not in ($includeVertex)" : "").") ".($excludeVertex ? " or g.a_nodo in ($excludeVertex)" : "").")) OR (g.da_nodo=sg.da_nodo AND ((g.da_tipo in $otherListStr ".($includeVertex ? " and g.da_nodo not in ($includeVertex)" : "").") ".($excludeVertex ? " or g.da_nodo in ($excludeVertex)" : "").")))";
		$flag = 2;
	} else
		$selectedPipe = $selectedNextPipe;	
}

if(!$selectedPipe)
	die();
$elements = array();
error_log("-->Flag: ".$flag);
//if($flag != 2){
	/*$sql = "WITH RECURSIVE search_graph(id_arco, id_elemento, da_nodo, a_nodo, da_tipo, a_tipo, tipo, the_geom, depth, path, stop) AS ("
		."SELECT g.id_arco, g.id_elemento, g.da_nodo, g.a_nodo, g.da_tipo, g.a_tipo, ".($TIPO_FIELD==1 ? $TIPO_FIELD." as tipo" : "g.tipo").", g.the_geom, 1, ARRAY[g.id_arco], false "
		."FROM grafo.archi_".$_REQUEST['domain']." g where g.id_arco = $selectedPipe UNION ALL "
		."SELECT g.id_arco, g.id_elemento, g.da_nodo, g.a_nodo, g.da_tipo, g.a_tipo, ".($TIPO_FIELD==1 ? $TIPO_FIELD." as tipo" : "g.tipo").", g.the_geom, sg.depth + 1, path || g.id_arco, g.id_arco = ANY(path) OR ($joinFilter) "
		."FROM grafo.archi_".$_REQUEST['domain']." g, search_graph sg "
		."WHERE (sg.a_nodo = g.da_nodo OR sg.a_nodo = g.a_nodo OR sg.da_nodo = g.da_nodo OR sg.da_nodo = g.a_nodo) AND g.id_arco<>sg.id_arco AND NOT stop) "
		."SELECT DISTINCT id_arco, id_elemento, da_nodo, a_nodo, da_tipo, a_tipo, tipo FROM search_graph WHERE NOT stop LIMIT 1000";*/	
	foreach($ELEMENTS as $key=>$value)
		$elements[$key] = array();
	costruisciGrafoRicorsivo($db, $selectedPipe, $_REQUEST['domain'], $TIPO_FIELD, $OTHERS, $elements, $bVertex, $_REQUEST['exclude']);
/*} else {
	//20210311 MZ -> aggiunto id_elemento, corrispondente al fid
	//ELENCO DEGLI OGGETTI TROVATI INDICIZZATI PER TIPO
	error_log($sql);
	$stmt = $db->prepare($sql);
	$stmt->execute();
	foreach($ELEMENTS as $key=>$value)
		$elements[$key] = array();
	while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
		$elements["condotta"][] = array($row["tipo"],$row["id_elemento"]);//20210311 MZ -> id_elemento non id_arco, corrispondente al fid
		if($row["da_tipo"]!="altro" || in_array($row["da_nodo"], $bVertex))
			$elements[$row["da_tipo"]][] = $row["da_nodo"]; 
		if($row["a_tipo"]!="altro" || in_array($row["a_nodo"], $bVertex))
			$elements[$row["a_tipo"]][] = $row["a_nodo"];
	}
	print_debug($sql,null,'condotta');
	}*/
print_debug($elements,null,'condotta');

$geom = ($_REQUEST["srs"] == "EPSG:".$GEOM_SRID) ? $GEOM_FIELD : "st_transform($GEOM_FIELD,$srid)";
//-- INIZIO 20211005 MZ
$indexCondottaArr = array_unique(array_map(function(array $single){
	return $single[0];
}, $elements["condotta"]));
$row = array();
foreach($indexCondottaArr as $singleIndexCondotta) {
	$table = $ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["table"];
	$condition = $ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["condition"];
	$gg = ($_REQUEST["srs"] == "EPSG:the_geom") ? $GEOM_FIELD : "st_transform(the_geom,$srid)";
	$sql = "select ST_XMin(ST_Extent($gg)), ST_YMin(ST_Extent($gg)),ST_XMax(ST_Extent($gg)),ST_YMax(ST_Extent($gg)) from grafo.archi_$SUFFIX "
		."where id_arco IN (".implode(",", array_map(function(array $single){
			return $single[2];
		},array_filter($elements["condotta"],function(array $single) use($singleIndexCondotta){
			return $single[0]==$singleIndexCondotta;
		}))).");";
	error_log($sql);
	$stmt = $db->prepare($sql);
	$stmt->execute();
	$row[$singleIndexCondotta] = $stmt->fetch(PDO::FETCH_NUM);
}
for($i=0; $i<4; $i++)
	$ELEMENTS["features_extent"][$i] = round($i<2 ? min(array_column($row,$i)): max(array_column($row, $i)),2);
//-- FINE 20211005 MZ
//AGGIUNGO LE ENTITA' TROVATE A ELEMENTS
$excludeRequested = (empty($_REQUEST["exclude"])) ? "0 as escluso" : "case when $FID_FIELD in (".implode(",",array_map(function($a){
return is_numeric($a) ? $a : "'".$a."'";
},explode(",",$_REQUEST["exclude"]))).") then 1 else 0 end as escluso";

//CONDOTTE:
$features = array();
foreach($indexCondottaArr as $singleIndexCondotta) {
	$fields = array();
	$table = $ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["table"];
	$ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["typeName"] = "condotta";
	unset($ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["table"]);
	$condition = $ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["condition"];
	foreach($ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["properties"] as $field)
		$fields[] = $field["name"];
	$gg = ($_REQUEST["srs"] == "EPSG:".$GEOM_SRID) ? "b.the_geom" : "st_transform(b.the_geom,$srid)";
	$sql = "SELECT $FID_FIELD, ST_AsText($gg) as geom,".implode(",", $fields)." FROM $SCHEMA.$table inner join grafo.archi_$SUFFIX b on $FID_FIELD=b.id_elemento "
		."WHERE $condition and b.id_arco IN (".implode(",",array_map(function(array $single){
			return $single[2];//is_numeric($single[1]) ? $single[1]  : ("'".$single[1]."'");
		},array_filter($elements["condotta"],function(array $single) use($singleIndexCondotta){
			return $single[0]==$singleIndexCondotta;
		}))).");";
	print_debug($sql, null, 'condotta');
	error_log($sql);
	$stmt = $db->prepare($sql);
	$stmt->execute();
	while($row = $stmt->fetch(PDO::FETCH_ASSOC)){
		$properties = array();
		//encode in utf8
		foreach($ELEMENTS["condotta"]["featureType"][$singleIndexCondotta]["properties"] as $field)
			$properties[$field["name"]]=utf8_encode($row[$field["name"]]);
		$properties["escluso"]=0;
		$properties["simbolo"]="";
		$properties["tipo"]=$singleIndexCondotta;
		$g = explode(",", str_replace(")","",str_replace("LINESTRING(","",$row["geom"])));
		foreach($g as $idx=>$value){
			list($x,$y) = explode(" ", $g[$idx]);
			$g[$idx]=array(round($x,2),round($y,2));
		}
		$features[] = array("type"=>"Feature", "id"=>"condotta".".".$row[$FID_FIELD], "properties"=>$properties, "geometry"=>array("type"=>"LineString","coordinates"=>$g));
	}
}

$ELEMENTS["condotta"]["features"] = array("type"=>"FeatureCollection","features"=>$features);
unset($elements["condotta"]);
foreach($elements as $key => $idList){
	error_log($key."----".json_encode($idList));
	$features = array();
	if(!empty($idList)){
		$fields = array();
		if(strcmp($key,"altro")!=0) {
			$table = $ELEMENTS[$key]["featureType"]["table"];
			$condition = $ELEMENTS[$key]["featureType"]["condition"];
			$exclusion = $ELEMENTS[$key]["featureType"]["exclusion"];
			$join = isset($ELEMENTS[$key]["featureType"]["join"]) ? $ELEMENTS[$key]["featureType"]["join"] : "";
			$groupBy = isset($ELEMENTS[$key]["featureType"]["groupby"]); 
			$working = isset($ELEMENTS[$key]["featureType"]["working"]) ? $ELEMENTS[$key]["featureType"]["working"] : "";
			$ELEMENTS[$key]["featureType"]["typeName"] = $key;
			unset ($ELEMENTS[$key]["featureType"]["table"]);
			foreach($ELEMENTS[$key]["featureType"]["properties"] as $field)
				$fields[]=$field["name"];
			$sql = "SELECT ".(empty($join) ? "" : "a.")."$FID_FIELD,ST_AsText($geom) as geom, "
				.(empty($exclusion) ? str_replace(" fid ", empty($join) ? " fid " : " a.fid ",$excludeRequested) : $exclusion)
				.(!empty($fields) ? "," : "").implode(",",$fields)." "
				.(empty($working) ? "" : ",".$working." as working ")
				."FROM $SCHEMA.$table $join WHERE $condition "
				.(!empty($condition) ? "and " : " ").(empty($join) ? "" : "a.")."$FID_FIELD IN (SELECT id_elemento FROM grafo.nodi_".$_REQUEST['domain']." WHERE id_nodo IN(".implode(",",$idList).")) "
				.($groupBy ? "group by a.$FID_FIELD, geom, ".implode(",",array_slice($fields,0,count($fields)-1)) : "").";";
			error_log($sql);
      			print_debug($sql,null,'condotta');
      			$stmt = $db->prepare($sql);
      			$stmt->execute();
      			while($row = $stmt->fetch(PDO::FETCH_ASSOC)){
        			$properties = array();
        			foreach($ELEMENTS[$key]["featureType"]["properties"] as $field) {
          				$num = strpos($field["name"], ".");
          				$as = strpos($field["name"], "count");
          				$ff = $num!==FALSE ? substr($field["name"],$num+1) : ($as!==FALSE ? "cont" : $field["name"]);
          				//error_log($ff.":".$field["name"].":".$num." - ".$as);
          				$properties[$ff]=utf8_encode($row[$ff]);
        			}
        			$properties["escluso"]=$row["escluso"];
				$properties["simbolo"]=(isset($ELEMENTS[$key]["featureType"]["simbolo"]) ? $ELEMENTS[$key]["featureType"]["simbolo"] : "");
				//error_log(json_encode($properties));
				list($x,$y) = explode(" ",str_replace(")","",str_replace("POINT(","",$row["geom"])));
				$g = array(round($x,2),round($y,2));
				$features[] = array("type"=>"Feature","id"=>$key.".".$row[$FID_FIELD].(isset($row['working']) ? ".".($row['working'] ? "true" : "false") :""),"properties"=>$properties,"geometry"=>array("type"=>"Point","coordinates"=>$g));	
			}
		} else {
			//$auxG = ($_REQUEST["srs"] == "EPSG:".$GEOM_SRID) ? "the_geom" :  ($transform."(the_geom,'".$SRS[$GEOM_SRID]."','".$SRS[$srid]."',".$srid.")");
			$auxG = ($_REQUEST["srs"] == "EPSG:".$GEOM_SRID) ? "the_geom" : "st_transform(the_geom,$srid)";
			$sql = "SELECT id_nodo, ST_AsText($auxG) as geom from grafo.nodi_".$_REQUEST['domain']." where id_nodo IN (".implode(",",$idList).")";
			$stmt = $db->prepare($sql);
			$stmt->execute();
			while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
				$properties = array();
				$properties["simbolo"] = (isset($ELEMENTS[$key]["featureType"]["simbolo"]) ? $ELEMENTS[$key]["featureType"]["simbolo"] : "") ;
				list($x,$y) = explode(" ",str_replace(")","",str_replace("POINT(","",$row["geom"])));
				$g = array(round($x,2),round($y,2));
				$features[] = array("type"=>"Feature","id"=>$key.".".$row["id_nodo"],"properties"=>$properties,"geometry"=>array("type"=>"Point","coordinates"=>$g));
      			}
    		}
	}
  	$ELEMENTS[$key]["features"] = array("type"=>"FeatureCollection","features"=>$features);
}
header("Content-Type: application/json");
die(json_encode($ELEMENTS));
