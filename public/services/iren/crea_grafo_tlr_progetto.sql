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


create schema if not exists grafo;
DROP SEQUENCE if exists grafo.archi_arco_id_seq_TLR_PRJ;
CREATE SEQUENCE grafo.archi_arco_id_seq_TLR_PRJ INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.archi_TLR_PRJ cascade;
CREATE TABLE grafo.archi_TLR_PRJ AS
(SELECT 
   nextval('grafo.archi_arco_id_seq_TLR_PRJ'::regclass)::integer as id_arco,
   fid AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom
FROM
   teleriscaldamento.fcl_h_ww_section_ptz
WHERE id_stato = 1
AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM teleriscaldamento.fcl_h_ww_section_ptz l, 
   (SELECT * from 
      ( -- Punti iniziali tratta
       -- introduzione caso di tratte potenziali che intersecano tratte
       select ST_StartPoint(geom) as the_geom
       from teleriscaldamento.fcl_h_ww_section_ptz
       where id_stato=1
       union all
       select ST_EndPoint(geom) as the_geom
       from teleriscaldamento.fcl_h_ww_section_ptz
       where id_stato=1
     ) AS foo GROUP BY the_geom
   ) AS x
   WHERE id_stato = 1
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.03)
));

OPEN crs_split FOR 
   (SELECT DISTINCT l.fid,l.geom as the_geom,x.the_geom as the_geom_node
   FROM teleriscaldamento.fcl_h_ww_section_ptz l, 
   (SELECT * FROM (
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 1 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato = 1
       -- fine MZ
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=1 
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.fid
   );
LOOP
   FETCH crs_split INTO rcd;
   IF ((arco_fid <> rcd.fid) AND arco_fid <> 0) OR NOT FOUND THEN
      num_splits := 1;
      LOOP
	 IF ST_GeometryN(geometry_arc,num_splits) IS NULL THEN
            EXIT;
         END IF;
         INSERT INTO grafo.archi_TLR_PRJ (id_arco,id_elemento,da_nodo,a_nodo,da_tipo,a_tipo,the_geom)
         VALUES (
            nextval('grafo.archi_arco_id_seq_TLR_PRJ'::regclass)::integer,
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
   IF (arco_fid <> rcd.fid) THEN
      arco_fid := rcd.fid;
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

ALTER TABLE grafo.archi_TLR_PRJ ADD CONSTRAINT archi_TLR_PRJ_pkey PRIMARY KEY(id_arco); 


-- TABELLA DEI NODI RAGGRUPPATI PER GEOMETRIA E ASSEGNAZIONE DI ID UNIVOCO
DROP SEQUENCE if exists grafo.nodi_nodo_id_seq_TLR_PRJ;
CREATE SEQUENCE grafo.nodi_nodo_id_seq_TLR_PRJ INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.nodi_TLR_PRJ cascade;
CREATE TABLE grafo.nodi_TLR_PRJ AS
SELECT 
	nextval('grafo.nodi_nodo_id_seq_TLR_PRJ'::regclass)::integer as id_nodo,
	array_remove(array_accum(arco_entrante),NULL) AS arco_entrante,
	array_remove(array_accum(arco_uscente),NULL) AS arco_uscente,
	the_geom
FROM (
  SELECT 
    ST_StartPoint(the_geom) AS the_geom, 
    id_arco AS arco_uscente, -- fid anzichè gs_id
    NULL::integer AS arco_entrante
  FROM grafo.archi_TLR_PRJ
  UNION ALL
  SELECT 
    ST_EndPoint(the_geom) AS the_geom, 
    NULL::integer AS arco_uscente,
    id_arco AS arco_entrante -- fid anzichè gs_id 
  FROM grafo.archi_TLR_PRJ
) AS foo
GROUP BY the_geom;
ALTER TABLE grafo.nodi_TLR_PRJ ADD PRIMARY KEY (id_nodo);
CREATE INDEX nodi_TLR_PRJ_the_geom_gist ON grafo.nodi_TLR_PRJ USING gist (the_geom);

--MG, 22/11/2024
--CERCO NODI ENTRO LA TOLLERANZA E LI UNISCO
CREATE TABLE grafo.nodi_tlr_prj_cluster AS (
	SELECT x.*, ST_ClusterDBSCAN(x.the_geom, eps => 0.01, minpoints => 2) over() AS cluster FROM grafo.nodi_TLR_prj x, grafo.nodi_TLR_prj y WHERE ST_DWithin(x.the_geom,y.the_geom,0.01) AND x.id_nodo<>y.id_nodo
);
UPDATE grafo.nodi_tlr_prj
SET arco_entrante=subquery.arco_entrante,
    arco_uscente=subquery.arco_uscente
FROM (
	SELECT MIN(id_nodo) AS id_nodo, array_remove(array_agg(arco_entrante),null) AS arco_entrante, array_remove(array_agg(arco_uscente),null) AS arco_uscente FROM (SELECT id_nodo, unnest(arco_entrante) AS arco_entrante, unnest(arco_uscente) AS arco_uscente, cluster FROM grafo.nodi_tlr_prj_cluster) dummy GROUP BY cluster
) AS subquery
WHERE grafo.nodi_tlr_prj.id_nodo=subquery.id_nodo;
DELETE FROM grafo.nodi_tlr_prj WHERE id_nodo IN (SELECT MAX(id_nodo) FROM grafo.nodi_tlr_prj_cluster GROUP BY cluster);
DROP TABLE grafo.nodi_tlr_prj_cluster;

--ESPANDO LA TABELLA DEI NODI PER POTER FARE LE QUERY DI JOIN E AGGIORNARE LA TABELLA DEGLI ARCHI
UPDATE grafo.archi_TLR_prj a SET da_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_TLR_prj
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


UPDATE grafo.archi_TLR_prj a SET a_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_TLR_prj
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
ALTER TABLE grafo.nodi_TLR_prj ADD COLUMN tipo_nodo character varying;
ALTER TABLE grafo.nodi_TLR_prj ADD COLUMN id_elemento integer;

-- UTENZA POTENZIALE
update grafo.nodi_TLR_PRJ set tipo_nodo='utenza potenziale', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_TLR_prj n, teleriscaldamento.fcl_h_service_ptz e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=1) as foo where nodi_TLR_prj.id_nodo=foo.id_nodo;

update grafo.nodi_TLR_PRJ set tipo_nodo='raccordo potenziale' from
(select id_nodo from grafo.nodi_TLR_prj n, (SELECT geom 
       FROM teleriscaldamento.fcl_h_ww_section 
       WHERE id_tipo_verso in (1,3) and id_stato = 3 
) e where
ST_DWithin(n.the_geom,e.geom,0.1)) as foo where nodi_TLR_prj.id_nodo=foo.id_nodo;

update grafo.nodi_TLR_PRJ set tipo_nodo='raccordo potenziale' from
(select id_nodo from grafo.nodi_TLR_prj n, (SELECT geom 
       FROM teleriscaldamento.fcl_h_ww_section_ptz
       WHERE id_stato in (2,3)
) e where
ST_DWithin(n.the_geom,e.geom,0.1)) as foo where nodi_TLR_prj.id_nodo=foo.id_nodo;

update grafo.nodi_TLR_prj set tipo_nodo='altro' where tipo_nodo is null;

CREATE INDEX nodi_TLR_prj_tipo_idx ON grafo.nodi_TLR_prj (tipo_nodo);

-- AGGIORNO LA TABELLA ARCHI CON I TIPI E INDICI
UPDATE grafo.archi_TLR_prj set da_tipo = nodi_TLR_prj.tipo_nodo FROM grafo.nodi_TLR_prj WHERE da_nodo=nodi_TLR_prj.id_nodo;
UPDATE grafo.archi_TLR_prj set a_tipo = nodi_TLR_prj.tipo_nodo FROM grafo.nodi_TLR_prj WHERE a_nodo=nodi_TLR_prj.id_nodo;
CREATE INDEX archi_TLR_prj_da_nodo_idx ON grafo.archi_TLR_prj (da_nodo);
CREATE INDEX archi_TLR_prj_a_nodo_idx ON grafo.archi_TLR_prj (a_nodo);
CREATE INDEX archi_TLR_prj_the_geom_gist ON grafo.archi_TLR_prj USING gist (the_geom);

DROP TABLE if exists grafo.sentiero_TLR_PRJ cascade;

create table grafo.sentiero_tlr_prj(
idorigine int,
lunghezza float not null,
primary key(idorigine)
);
END$$;