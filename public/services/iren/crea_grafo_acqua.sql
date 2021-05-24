DO $$
DECLARE
    arco_fid integer := 0;
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
DROP SEQUENCE if exists grafo.archi_arco_id_seq_acqua;
CREATE SEQUENCE grafo.archi_arco_id_seq_acqua INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.archi_acqua cascade;
CREATE TABLE grafo.archi_acqua AS
SELECT 
   nextval('grafo.archi_arco_id_seq_acqua'::regclass)::integer as id_arco,
   fid AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom
FROM acquedotto.fcl_w_dn_section
WHERE id_stato=3 and id_gestore in (15,999) and id_tiporete in (1,2,999) and id_tipologia in (1,2,3,999)
AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM acquedotto.fcl_w_dn_section l, 
   (SELECT * from 
      ( -- Punti iniziali tratta
       SELECT ST_StartPoint(geom) AS the_geom
	FROM acquedotto.fcl_w_dn_section
	WHERE id_stato=3 and id_gestore in (15,999) and id_tiporete in (1,2,999) and id_tipologia in (1,2,3,999)
       UNION ALL 
        -- Punti finali tratta
       SELECT ST_EndPoint(geom) AS the_geom 
	FROM acquedotto.fcl_w_dn_section
	WHERE id_stato=3 and id_gestore in (15,999) and id_tiporete in (1,2,999) and id_tipologia in (1,2,3,999)
       UNION ALL 
        -- Valvole
       SELECT geom AS the_geom 
       FROM acquedotto.fcl_w_isolation_device
       WHERE id_stato=3 and id_gestore in (15,999) and gtype_id in (10,20)
       /*UNION ALL 
        -- Valvole drenaggio
       SELECT geom AS the_geom 
       FROM teleriscaldamento.fcl_h_isolation_device
       WHERE id_tipologia=5 and id_stato=3*/
      ) AS foo GROUP BY the_geom
   ) AS x
   WHERE l.id_stato=3 and l.id_gestore in (15,999) and l.id_tiporete in (1,2,999) and l.id_tipologia in (1,2,3,999)
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.01)
);

OPEN crs_split FOR 
   SELECT DISTINCT l.fid,l.geom as the_geom,x.the_geom as the_geom_node 
   FROM acquedotto.fcl_w_dn_section l, 
   (SELECT * FROM 
      ( -- Punti iniziali tratta
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM acquedotto.fcl_w_dn_section  
       WHERE id_stato=3 and id_gestore in (15,999) and id_tiporete in (1,2,999) and id_tipologia in (1,2,3,999)
       UNION ALL 
        -- Punti finali tratta
       SELECT ST_EndPoint(geom) AS the_geom
       FROM acquedotto.fcl_w_dn_section  
       WHERE id_stato=3 and id_gestore in (15,999) and id_tiporete in (1,2,999) and id_tipologia in (1,2,3,999)
       UNION ALL 
        -- Valvole 
       SELECT geom AS the_geom 
       FROM acquedotto.fcl_w_isolation_device
       WHERE id_stato=3 and id_gestore in (15,999) and gtype_id in (10,20)
       /*UNION ALL 
        -- Valvole drenaggio
       SELECT geom AS the_geom 
       FROM teleriscaldamento.fcl_h_isolation_device
       WHERE id_tipologia=5 and id_stato=3*/
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.id_gestore in (15,999) and l.id_tiporete in (1,2,999) and l.id_tipologia in (1,2,3,999)
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.01)
   ORDER BY l.fid;
LOOP
   FETCH crs_split INTO rcd;
   IF (arco_fid <> rcd.fid AND arco_fid <> 0) OR NOT FOUND THEN
      num_splits := 1;
      LOOP
	 IF ST_GeometryN(geometry_arc,num_splits) IS NULL THEN
            EXIT;
         END IF;
         INSERT INTO grafo.archi_acqua (id_arco,id_elemento,da_nodo,a_nodo,da_tipo,a_tipo,the_geom)
         VALUES (
            nextval('grafo.archi_arco_id_seq_acqua'::regclass)::integer,
            arco_fid,
            NULL,
            NULL,
            NULL,
            NULL,
            ST_GeometryN(geometry_arc,num_splits));
            num_splits := num_splits+1;
      END LOOP;
   END IF;
   EXIT WHEN NOT FOUND;
   IF arco_fid <> rcd.fid THEN
      arco_fid := rcd.fid;
      geometry_arc := rcd.the_geom;
   END IF;
   num_splits := 1;
   geometry_tmp_coll := NULL;
   LOOP
      geometry_tmp := ST_GeometryN(geometry_arc,num_splits);
      geometry_tmp := ST_Split(geometry_tmp, rcd.the_geom_node);
      geometry_tmp_coll = ST_CollectionHomogenize(ST_Collect(geometry_tmp_coll, geometry_tmp));
      num_splits := num_splits+1;
      IF num_splits > ST_NumGeometries(geometry_arc) THEN
         EXIT;
      END IF;
   END LOOP;
   geometry_arc := geometry_tmp_coll;
END LOOP;

ALTER TABLE grafo.archi_acqua ADD CONSTRAINT archi_acqua_pkey PRIMARY KEY(id_arco); 


-- TABELLA DEI NODI RAGGRUPPATI PER GEOMETRIA E ASSEGNAZIONE DI ID UNIVOCO
DROP SEQUENCE if exists grafo.nodi_nodo_id_seq_acqua;
CREATE SEQUENCE grafo.nodi_nodo_id_seq_acqua INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.nodi_acqua cascade;
CREATE TABLE grafo.nodi_acqua AS
SELECT 
	nextval('grafo.nodi_nodo_id_seq_acqua'::regclass)::integer as id_nodo,
	array_accum(arco_entrante) AS arco_entrante,
	array_accum(arco_uscente) AS arco_uscente,
	the_geom
FROM (
  SELECT 
    ST_StartPoint(the_geom) AS the_geom, 
    id_arco AS arco_uscente, -- fid anzichè gs_id
    NULL::integer AS arco_entrante
  FROM grafo.archi_acqua
  UNION ALL
  SELECT 
    ST_EndPoint(the_geom) AS the_geom, 
    NULL::integer AS arco_uscente,
    id_arco AS arco_entrante -- fid anzichè gs_id
  FROM grafo.archi_acqua
) AS foo
GROUP BY the_geom;
ALTER TABLE grafo.nodi_acqua ADD PRIMARY KEY (id_nodo);


--ESPANDO LA TABELLA DEI NODI PER POTER FARE LE QUERY DI JOIN E AGGIORNARE LA TABELLA DEGLI ARCHI
UPDATE grafo.archi_acqua a SET da_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_acqua
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


UPDATE grafo.archi_acqua a SET a_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_acqua
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
ALTER TABLE grafo.nodi_acqua ADD COLUMN tipo_nodo character varying;
ALTER TABLE grafo.nodi_acqua ADD COLUMN id_elemento integer;

-- VALVOLE
update grafo.nodi_acqua set tipo_nodo='valvola', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_acqua n, acquedotto.fcl_w_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and e.id_gestore in (15,999) and e.gtype_id in (10,20)) as foo where nodi_acqua.id_nodo=foo.id_nodo;

--
-- TERMINALI
update grafo.nodi_acqua set tipo_nodo='utenza', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_acqua n, acquedotto.fcl_w_service e where
ST_DWithin(n.the_geom,e.geom,0.01) and gtype_id in (40,50)) as foo where nodi_acqua.id_nodo=foo.id_nodo;

update grafo.nodi_acqua set tipo_nodo='scarico', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_acqua n, acquedotto.fcl_w_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=20) as foo where nodi_acqua.id_nodo=foo.id_nodo;

update grafo.nodi_acqua set tipo_nodo='idrante', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_acqua n, acquedotto.fcl_w_hydrant e where
ST_DWithin(n.the_geom,e.geom,0.01)) as foo where nodi_acqua.id_nodo=foo.id_nodo;

-- PUNTI DI IMMISSIONE
update grafo.nodi_acqua set tipo_nodo='sorgente', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_acqua n, acquedotto.fcl_w_installation e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and e.id_gestore in (15,999) and e.gtype_id=10) as foo where nodi_acqua.id_nodo=foo.id_nodo;


update grafo.nodi_acqua set tipo_nodo='serbatoio', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_acqua n, acquedotto.fcl_w_installation e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and e.id_gestore in (15,999) and e.gtype_id=20) as foo where nodi_acqua.id_nodo=foo.id_nodo;


update grafo.nodi_acqua set tipo_nodo='sollevamento', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_acqua n, acquedotto.fcl_w_installation e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and e.id_gestore in (15,999) and e.gtype_id=40) as foo where nodi_acqua.id_nodo=foo.id_nodo;

update grafo.nodi_acqua set tipo_nodo='altro' where tipo_nodo is null;

CREATE INDEX nodi_acqua_tipo_idx ON grafo.nodi_acqua (tipo_nodo);

-- AGGIORNO LA TABELLA ARCHI CON I TIPI E INDICI
UPDATE grafo.archi_acqua set da_tipo = nodi_acqua.tipo_nodo FROM grafo.nodi_acqua WHERE da_nodo=nodi_acqua.id_nodo;
UPDATE grafo.archi_acqua set a_tipo = nodi_acqua.tipo_nodo FROM grafo.nodi_acqua WHERE a_nodo=nodi_acqua.id_nodo;
CREATE INDEX archi_acqua_da_nodo_idx ON grafo.archi_acqua (da_nodo);
CREATE INDEX archi_acqua_a_nodo_idx ON grafo.archi_acqua (a_nodo);
CREATE INDEX archi_acqua_the_geom_gist ON grafo.archi_acqua USING gist (the_geom);


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
