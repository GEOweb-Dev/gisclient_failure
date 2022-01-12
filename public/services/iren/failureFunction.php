<?php
function costruisciGrafoRicorsivo($db, $selectedPipe, $dominio, $tipoField, $others, &$elements, $bVertex, $exclude) {
	$res = array();
	singoloArco($db, $selectedPipe, $dominio, $tipoField, $others, $res, explode(",",str_replace('\"',"",$exclude)), 1);
	foreach($res as $row){
		$elements["condotta"][] = array($row["tipo"],$row["id_elemento"]);
		if($row["da_tipo"]!="altro" || in_array($row["da_nodo"], $bVertex))
			$elements[$row["da_tipo"]][] = $row["da_nodo"]; 
		if($row["a_tipo"]!="altro" || in_array($row["a_nodo"], $bVertex))
			$elements[$row["a_tipo"]][] = $row["a_nodo"];
	}
}

function singoloArco($db, $selectedPipe, $dominio, $tipoField, $others, &$rs, $bVertex, $depth) {
	if(!empty($selectedPipe) && count(array_filter($rs, function($single) use($selectedPipe){
		return $single['id_arco']==$selectedPipe;
	}))==0) {
		$stmt = $db->prepare("SELECT g.id_arco, g.id_elemento, g.da_nodo, g.a_nodo, g.da_tipo, g.a_tipo, "
			.($tipoField==1 ? $tipoField." as tipo" : "g.tipo")
			.", g.the_geom, $depth as depth "
			."FROM grafo.archi_$dominio g where g.id_arco = ".$selectedPipe);
		$stmt->execute();
		while($row = $stmt->fetch(PDO::FETCH_ASSOC)) {
			$rs[] = $row;
			singoloNodo($db, $row['da_tipo'], $row['da_nodo'], $dominio, $tipoField, $others, $rs, $bVertex, $depth);	
			singoloNodo($db, $row['a_tipo'], $row['a_nodo'], $dominio, $tipoField, $others, $rs, $bVertex, $depth);
		}
	}
}

function singoloNodo($db,$tipo, $nodo, $dominio, $tipoField, $others, &$rs, $bVertex, $depth){
	//if(in_array($tipo, $others) || in_array($nodo, $bVertex)) {
	$st1 = $db->prepare("select tipo_nodo, id_elemento, arco_entrante, arco_uscente from grafo.nodi_$dominio where id_nodo=".$nodo);
	$st1->execute();
	while($row = $st1->fetch(PDO::FETCH_ASSOC)) {
		if(in_array($row['tipo_nodo'],$others) || in_array($row['id_elemento'],$bVertex)) {
			foreach(array_merge(explode(",",str_replace(array('{','}'),"",$row['arco_entrante'])), explode(",", str_replace(array('{','}'),"",$row['arco_uscente']))) as $bow)
				singoloArco($db, $bow, $dominio, $tipoField, $others, $rs, $bVertex, $depth+1);
		}
	}
}
?>
