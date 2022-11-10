<?php
function costruisciGrafoRicorsivo($db, $selectedPipe, $dominio, $tipoField, $others, &$elements, &$bVertex, $exclude, $flag) {
	$res = array();
	singoloArco($db, $selectedPipe, $dominio, $tipoField, $others, $res, explode(",",str_replace('\"',"",$exclude)), $bVertex, 1, $flag);
	foreach($res as $row){
		$elements["condotta"][] = array($row["tipo"],$row["id_elemento"],$row["id_arco"]);
		if($row["da_tipo"]!="altro" || in_array($row["da_nodo"], $bVertex))
			$elements[in_array($row["da_nodo"],$bVertex) ? "altro" : $row["da_tipo"]][] = $row["da_nodo"]; 
		if($row["a_tipo"]!="altro" || in_array($row["a_nodo"], $bVertex))
			$elements[in_array($row["a_nodo"],$bVertex) ? "altro" : $row["a_tipo"]][] = $row["a_nodo"];
	}
}

function singoloArco($db, $selectedPipe, $dominio, $tipoField, $others, &$rs, $exclude, &$include, $depth, $flag) {
	if(!empty($selectedPipe) && count(array_filter($rs, function($single) use($selectedPipe){
		return $single['id_arco']==$selectedPipe;
	}))==0) {
		$sql = ("SELECT g.id_arco, g.id_elemento, g.da_nodo, g.a_nodo, g.da_tipo, g.a_tipo, "
			.($tipoField==1 ? $tipoField." as tipo" : "g.tipo")
			.", g.the_geom, $depth as depth "
			."FROM grafo.archi_$dominio g where g.id_arco = ".$selectedPipe);
		error_log($sql);
		$stmt = $db->prepare($sql);
		$stmt->execute();
		while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			$rs[] = $row;
			singoloNodo($db, $row['da_tipo'], $row['da_nodo'], $dominio, $tipoField, $others, $rs, $exclude, $include, $depth, $flag);	
			singoloNodo($db, $row['a_tipo'], $row['a_nodo'], $dominio, $tipoField, $others, $rs, $exclude, $include, $depth, $flag);
		}
	}
}

function singoloNodo($db,$tipo, $nodo, $dominio, $tipoField, $others, &$rs, $exclude, &$include, $depth, $flag) {
	$sql = ("select tipo_nodo, id_elemento, arco_entrante, arco_uscente from grafo.nodi_$dominio where id_nodo=".$nodo);
	error_log($sql);
	$st1 = $db->prepare($sql);
	$st1->execute();
	while($row = $st1->fetch(PDO::FETCH_ASSOC)) {
		if((in_array($row['tipo_nodo'],$others) || in_array($row['id_elemento'],$exclude) || custom($db, $flag, $row)) && !in_array($nodo, $include)){
			foreach(array_merge(explode(",",str_replace(array('{','}'),"",$row['arco_entrante'])), explode(",", str_replace(array('{','}'),"",$row['arco_uscente']))) as $bow)
				singoloArco($db, $bow, $dominio, $tipoField, $others, $rs, $exclude, $include, $depth+1, $flag);
		}	
	}
}
?>
