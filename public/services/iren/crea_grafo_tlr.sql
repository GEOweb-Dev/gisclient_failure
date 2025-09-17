DO $$
DECLARE
    arco_fid integer := 0;
    arco_tipo integer :=0;
    node_id_check integer := 0;
    num_splits integer := 1;
    geometry_arc geometry;
    geometry_point geometry;
    geometry_fence geometry;
    geometry_tmp geometry;
    geometry_tmp_coll geometry;
    geometry_single_split geometry;
    crs_split REFCURSOR;
    rcd   RECORD;
BEGIN
-- CREA UN ARRAY AGGREGANDO GLI OGGETTI
DROP AGGREGATE IF EXISTS array_accum (anyelement);
CREATE AGGREGATE array_accum (anyelement)
(
    sfunc = array_append,
    stype = anyarray,
    initcond = '{}'
);


--drop schema if exists grafo cascade;
create schema if not exists grafo;
-- TABELLA DEGLI ARCHI
DROP SEQUENCE if exists grafo.archi_arco_id_seq_TLR;
CREATE SEQUENCE grafo.archi_arco_id_seq_TLR INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.archi_TLR cascade;
CREATE TABLE grafo.archi_TLR AS
(SELECT 
   nextval('grafo.archi_arco_id_seq_TLR'::regclass)::integer as id_arco,
   fid AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   1 as tipo 
FROM
   teleriscaldamento.fcl_h_ww_section
WHERE id_tipo_verso in (1,3) and id_stato = 3
AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM teleriscaldamento.fcl_h_ww_section l, 
   (SELECT * from 
      ( -- Punti iniziali tratta
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3 
       UNION ALL 
        -- Punti finali tratta
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3
       UNION ALL
       -- 20211005 MZ -> introduzione caso di tratte potenziali che intersecano tratte
       select ST_StartPoint(geom) as the_geom
       from teleriscaldamento.fcl_h_ww_section_ptz
       where id_stato=3
       union all
       select ST_EndPoint(geom) as the_geom
       from teleriscaldamento.fcl_h_ww_section_ptz
       where id_stato=3
       union all
       -- fine 20211005 MZ
       -- Valvole sfiato
       SELECT geom AS the_geom 
       FROM teleriscaldamento.fcl_h_isolation_device
       WHERE id_tipologia=4 and id_stato=3
       UNION ALL 
        -- Valvole drenaggio
       SELECT geom AS the_geom 
       FROM teleriscaldamento.fcl_h_isolation_device
       WHERE id_tipologia=5 and id_stato=3
      ) AS foo GROUP BY the_geom
   ) AS x
   WHERE l.id_tipo_verso in (1,3) and id_stato = 3
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.03)
)) union all (
-- 20211005 MZ -> introduzione caso di tratte potenziali che intersecano tratte
SELECT 
   nextval('grafo.archi_arco_id_seq_TLR'::regclass)::integer as id_arco,
   fid AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   2 as tipo 
FROM
   teleriscaldamento.fcl_h_ww_section_ptz
WHERE id_stato = 3
AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM teleriscaldamento.fcl_h_ww_section_ptz l, 
   (SELECT * from 
      (
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 3
       UNION ALL
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3
      ) AS foo GROUP BY the_geom
   ) AS x
   WHERE l.id_stato = 3
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.03)
));

OPEN crs_split FOR 
   (SELECT DISTINCT l.fid,l.geom as the_geom,x.the_geom as the_geom_node, 1 as tipo 
   FROM teleriscaldamento.fcl_h_ww_section l, 
   (SELECT * FROM 
      (SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3
       UNION ALL
       -- 20211005 MZ
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 3
       -- fine MZ
       UNION ALL
       SELECT geom AS the_geom 
       FROM teleriscaldamento.fcl_h_isolation_device
       WHERE id_tipologia=4 and id_stato=3
       UNION ALL 
        -- Valvole drenaggio
       SELECT geom AS the_geom 
       FROM teleriscaldamento.fcl_h_isolation_device
       WHERE id_tipologia=5 and id_stato=3
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_tipo_verso IN (1,3) 
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.fid) union all (
   SELECT DISTINCT l.fid,l.geom as the_geom,x.the_geom as the_geom_node, 2 as tipo 
   FROM teleriscaldamento.fcl_h_ww_section_ptz l, 
   (SELECT * FROM 
      (SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3
       UNION ALL
       -- 20211005 MZ
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 3
       -- fine MZ
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.fid
   );
LOOP
   FETCH crs_split INTO rcd;
   IF ((arco_fid <> rcd.fid OR arco_tipo<>rcd.tipo) AND arco_fid <> 0 and arco_tipo <> 0) OR NOT FOUND THEN
      num_splits := 1;
      LOOP
	 IF ST_GeometryN(geometry_arc,num_splits) IS NULL THEN
            EXIT;
         END IF;
         INSERT INTO grafo.archi_TLR (id_arco,id_elemento,da_nodo,a_nodo,da_tipo,a_tipo,the_geom,tipo)
         VALUES (
            nextval('grafo.archi_arco_id_seq_TLR'::regclass)::integer,
            arco_fid,
            NULL,
            NULL,
            NULL,
            NULL,
            ST_GeometryN(geometry_arc,num_splits), arco_tipo);
            num_splits := num_splits+1;
      END LOOP;
   END IF;
   EXIT WHEN NOT FOUND;
   IF (arco_fid <> rcd.fid OR arco_tipo<>rcd.tipo) THEN
      arco_fid := rcd.fid;
      arco_tipo := rcd.tipo;
      geometry_arc := rcd.the_geom;
   END IF;
   num_splits := 1;
   geometry_tmp_coll := NULL;
   LOOP
      geometry_tmp := ST_GeometryN(geometry_arc,num_splits);
      geometry_tmp := ST_Split(ST_Snap(geometry_tmp,rcd.the_geom_node,0.03), rcd.the_geom_node);
      geometry_tmp_coll = ST_CollectionHomogenize(ST_Collect(geometry_tmp_coll, geometry_tmp));
      num_splits := num_splits+1;
      IF num_splits > ST_NumGeometries(geometry_arc) THEN
         EXIT;
      END IF;
   END LOOP;
   geometry_arc := geometry_tmp_coll;
END LOOP;

ALTER TABLE grafo.archi_TLR ADD CONSTRAINT archi_TLR_pkey PRIMARY KEY(id_arco); 


-- TABELLA DEI NODI RAGGRUPPATI PER GEOMETRIA E ASSEGNAZIONE DI ID UNIVOCO
DROP SEQUENCE if exists grafo.nodi_nodo_id_seq_TLR;
CREATE SEQUENCE grafo.nodi_nodo_id_seq_TLR INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.nodi_TLR cascade;
CREATE TABLE grafo.nodi_TLR AS
SELECT 
	nextval('grafo.nodi_nodo_id_seq_TLR'::regclass)::integer as id_nodo,
	array_remove(array_accum(arco_entrante),NULL) AS arco_entrante,
	array_remove(array_accum(arco_uscente),NULL) AS arco_uscente,
	the_geom
FROM (
  SELECT 
    ST_StartPoint(the_geom) AS the_geom, 
    id_arco AS arco_uscente, -- fid anzichè gs_id
    NULL::integer AS arco_entrante
  FROM grafo.archi_TLR
  UNION ALL
  SELECT 
    ST_EndPoint(the_geom) AS the_geom, 
    NULL::integer AS arco_uscente,
    id_arco AS arco_entrante -- fid anzichè gs_id
  --FROM acqua.ratraccia_g 
  FROM grafo.archi_TLR
) AS foo
GROUP BY the_geom;
ALTER TABLE grafo.nodi_TLR ADD PRIMARY KEY (id_nodo);
CREATE INDEX nodi_TLR_the_geom_gist ON grafo.nodi_TLR USING gist (the_geom);

--MG, 22/11/2024
--CERCO NODI ENTRO LA TOLLERANZA E LI UNISCO
CREATE TABLE grafo.nodi_tlr_cluster AS (
	SELECT x.*, ST_ClusterDBSCAN(x.the_geom, eps => 0.01, minpoints => 2) over() AS cluster FROM grafo.nodi_TLR x, grafo.nodi_TLR y WHERE ST_DWithin(x.the_geom,y.the_geom,0.01) AND x.id_nodo<>y.id_nodo
);
UPDATE grafo.nodi_tlr
SET arco_entrante=subquery.arco_entrante,
    arco_uscente=subquery.arco_uscente
FROM (
	SELECT MIN(id_nodo) AS id_nodo, array_remove(array_agg(arco_entrante),null) AS arco_entrante, array_remove(array_agg(arco_uscente),null) AS arco_uscente FROM (SELECT id_nodo, unnest(arco_entrante) AS arco_entrante, unnest(arco_uscente) AS arco_uscente, cluster FROM grafo.nodi_tlr_cluster) dummy GROUP BY cluster
) AS subquery
WHERE grafo.nodi_tlr.id_nodo=subquery.id_nodo;
DELETE FROM grafo.nodi_tlr WHERE id_nodo IN (SELECT MAX(id_nodo) FROM grafo.nodi_tlr_cluster GROUP BY cluster);
DROP TABLE grafo.nodi_tlr_cluster;

--ESPANDO LA TABELLA DEI NODI PER POTER FARE LE QUERY DI JOIN E AGGIORNARE LA TABELLA DEGLI ARCHI
UPDATE grafo.archi_TLR a SET da_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_TLR
	), 
	nodi_espansi AS(
		SELECT 
		  id_nodo, 
		  arco_uscente[uscente_upper], 
		  arco_entrante[entrante_upper]
		FROM nodi_serie
	)
	SELECT * FROM nodi_espansi) b
WHERE a.id_arco = b.arco_uscente;


UPDATE grafo.archi_TLR a SET a_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_TLR
	), 
	nodi_espansi AS(
		SELECT 
		  id_nodo, 
		  arco_uscente[uscente_upper], 
		  arco_entrante[entrante_upper]
		FROM nodi_serie
	)
	SELECT * FROM nodi_espansi) b
WHERE a.id_arco = b.arco_entrante;


-- FINE COSTRUZIONE DEL GRAFO


-- AGGIORNO LA TIPOLOGIA DEI NODI IN RELAZIONE AGLI OGGETTI CON IL NOME DEL QUERY LAYER IN AUTHOR PER AVERE LE DEFINIZIONE DEI CAMPI 
-- TODO DATO UN ELENCO DI LIVELLI GISCLIENT QUESTO VIENE FATTO AUTOMATICAMENTE
ALTER TABLE grafo.nodi_TLR ADD COLUMN tipo_nodo character varying;
ALTER TABLE grafo.nodi_TLR ADD COLUMN id_elemento integer;

-- VALVOLE ZONA (1)/MAGLIATURA (2)/SFIATO (4)/DRENAGGIO (5)/BARICENTRO (7)
update grafo.nodi_TLR set tipo_nodo='valvola zona', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=1 and e.id_stato=3 and ((e.id_tipoposa not in (4,18)) or (e.id_tipoposa is null))) as foo where nodi_TLR.id_nodo=foo.id_nodo;
update grafo.nodi_TLR set tipo_nodo='valvola zona interrato', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=1 and e.id_stato=3 and e.id_tipoposa=18) as foo where nodi_TLR.id_nodo=foo.id_nodo;
update grafo.nodi_TLR set tipo_nodo='valvola zona in cameretta', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=1 and e.id_stato=3 and e.id_tipoposa=4) as foo where nodi_TLR.id_nodo=foo.id_nodo;

--CHG
update grafo.nodi_TLR set tipo_nodo='valvola baricentro in cameretta', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=7 and e.id_stato=3 and e.id_tipoposa=4) as foo where nodi_TLR.id_nodo=foo.id_nodo;
update grafo.nodi_TLR set tipo_nodo='valvola baricentro interrato', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=7 and e.id_stato=3 and e.id_tipoposa=18) as foo where nodi_TLR.id_nodo=foo.id_nodo;

update grafo.nodi_TLR set tipo_nodo='valvola magliatura', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=2 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
update grafo.nodi_TLR set tipo_nodo='valvola sfiato', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=4 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
update grafo.nodi_TLR set tipo_nodo='valvola drenaggio', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=5 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;

-- CAMERE gtype=10, VALVOLA (4)/ POLIVALENTI (3)/ BARICENTRO (1)/
update grafo.nodi_TLR set tipo_nodo='camera valvola', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=10 and e.id_tipologia=4 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
update grafo.nodi_TLR set tipo_nodo='camera polivalente', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=10 and e.id_tipologia=3 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
--Rimozione per CHG
update grafo.nodi_TLR set tipo_nodo='camera baricentro', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=10 and e.id_tipologia=1 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;


-- POZZETTI gtype=20, VALVOLA (4)/ POLIVALENTE (5)/ BARICENTRO (3)/
update grafo.nodi_TLR set tipo_nodo='pozzetto valvola', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=20 and e.id_tipologia=4 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
--Rimozione per CHG
--update grafo.nodi_TLR set tipo_nodo='pozzetto baricentro', id_elemento = fid from
--(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_component e where
--ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=20 and e.id_tipologia=3 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
update grafo.nodi_TLR set tipo_nodo='pozzetto polivalente', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=20 and e.id_tipologia=5 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;

--
-- SOTTOSTAZIONI
update grafo.nodi_TLR set tipo_nodo='sottostazione utenza', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_service e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_tipologia=4 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
-- UTENZA POTENZIALE
update grafo.nodi_TLR set tipo_nodo='utenza potenziale', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_service_ptz e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=2) as foo where nodi_TLR.id_nodo=foo.id_nodo;
-- STAZIONI DI POMPAGGIO
update grafo.nodi_TLR set tipo_nodo='stazione di pompaggio', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_installation e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=20 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
-- CENTRALI IREN
update grafo.nodi_TLR set tipo_nodo='centrale IREN', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR n, teleriscaldamento.fcl_h_installation e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=10 and e.id_stato=3) as foo where nodi_TLR.id_nodo=foo.id_nodo;
--ELEMENTO GENERICO
update grafo.nodi_TLR set tipo_nodo='altro' where tipo_nodo is null;

CREATE INDEX nodi_TLR_tipo_idx ON grafo.nodi_TLR (tipo_nodo);

-- AGGIORNO LA TABELLA ARCHI CON I TIPI E INDICI
UPDATE grafo.archi_TLR set da_tipo = nodi_TLR.tipo_nodo FROM grafo.nodi_TLR WHERE da_nodo=nodi_TLR.id_nodo;
UPDATE grafo.archi_TLR set a_tipo = nodi_TLR.tipo_nodo FROM grafo.nodi_TLR WHERE a_nodo=nodi_TLR.id_nodo;
CREATE INDEX archi_TLR_da_nodo_idx ON grafo.archi_TLR (da_nodo);
CREATE INDEX archi_TLR_a_nodo_idx ON grafo.archi_TLR (a_nodo);
CREATE INDEX archi_TLR_the_geom_gist ON grafo.archi_TLR USING gist (the_geom);


-- 20201002 MZ - rimossa temporaneamente parte del TEST, da concordare eventualmente con Zio
-- TEST 

--drop table if exists grafo.ricerca;
--CREATE TABLE grafo.ricerca
--(
--  id serial,
--  a_nodo integer,
--  a_tipo character varying,
--  gs_id integer,
--  the_geom geometry
--);

-- SELEZIONE DALLA TRATTA CON ESCLUSIONE DI ELEMENTI
--insert into grafo.ricerca
--WITH RECURSIVE search_graph(da_nodo, a_nodo, a_tipo, gs_id, the_geom, depth, path, cycle) AS (
--        SELECT g.da_nodo, g.a_nodo, g.a_tipo, g.id_arco, g.the_geom, 1,
--          ARRAY[g.id_arco],
--          false
--        FROM grafo.archi g where g.id_arco=30421
--      UNION ALL
--        SELECT g.da_nodo, g.a_nodo, g.a_tipo, g.id_arco, g.the_geom, sg.depth + 1,
--          path || g.id_arco,
--          g.id_arco = ANY(path)
--        FROM grafo.archi g, search_graph sg
--        WHERE g.da_nodo = sg.a_nodo AND (g.da_tipo='altro' or g.da_nodo in(9137,9306)) AND NOT cycle
--)
--SELECT  da_nodo, a_nodo, a_tipo, gs_id, the_geom FROM search_graph  limit 10000;

-- SELEZIONE DALLA TRATTA CON ESCLUSIONE DI ELEMENTI
--WITH RECURSIVE search_graph(da_nodo, a_nodo, da_tipo, a_tipo, gs_id, the_geom, depth, path, cycle) AS (
--        SELECT g.da_nodo, g.a_nodo, g.da_tipo, g.a_tipo, g.id_arco, g.the_geom, 1,
--          ARRAY[g.id_arco],
--          false
--        FROM grafo.archi g where g.id_arco=6755
--      UNION ALL
--        SELECT g.da_nodo, g.a_nodo, g.da_tipo, g.a_tipo, g.id_arco, g.the_geom, sg.depth + 1,
--          path || g.id_arco,
--          g.id_arco = ANY(path) OR (g.da_tipo='altro')
--        FROM grafo.archi g, search_graph sg
--        WHERE g.da_nodo = sg.a_nodo AND (g.da_tipo='altro') AND NOT cycle
--)
--SELECT  da_nodo, a_nodo, da_tipo, a_tipo, gs_id, the_geom FROM search_graph  limit 10000;
END$$;
