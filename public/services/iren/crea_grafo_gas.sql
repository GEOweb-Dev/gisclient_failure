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
DROP SEQUENCE if exists grafo.archi_arco_id_seq_Gas;
CREATE SEQUENCE grafo.archi_arco_id_seq_Gas INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.archi_Gas cascade;
CREATE TABLE grafo.archi_Gas AS
SELECT 
   nextval('grafo.archi_arco_id_seq_Gas'::regclass)::integer as id_arco,
   fid AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom
FROM gas.fcl_g_dn_section
WHERE id_stato=3 and id_gestore in (15,999) and id_tipo_gas in (1,2,999) and id_tiporete in (1,2,999)
AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM gas.fcl_g_dn_section l, 
   (SELECT * from 
      ( -- Punti iniziali tratta
       SELECT ST_StartPoint(geom) AS the_geom
	FROM gas.fcl_g_dn_section
	WHERE id_stato=3 and id_gestore in (15,999) and id_tipo_gas in (1,2,999) and id_tiporete in (1,2,999)
       UNION ALL 
        -- Punti finali tratta
       SELECT ST_EndPoint(geom) AS the_geom 
	FROM gas.fcl_g_dn_section
	WHERE id_stato=3 and id_gestore in (15,999) and id_tipo_gas in (1,2,999) and id_tiporete in (1,2,999)
	UNION ALL
	-- Raccordi e riduttori
	SELECT geom as the_geom
	from gas.fcl_g_component
	where gtype_id in (10,20) and id_stato=3 and id_gestore in (15,999) and id_tipo_gas in (1,2,999) and id_tiporete in (1,2,999)
      ) AS foo GROUP BY the_geom
   ) AS x
   WHERE l.id_stato=3 and l.id_gestore in (15,999) and l.id_tipo_gas in (1,2,999) and l.id_tiporete in (1,2,999)
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.01)
);

OPEN crs_split FOR 
   SELECT DISTINCT l.fid,l.geom as the_geom,x.the_geom as the_geom_node 
   FROM gas.fcl_g_dn_section l, 
   (SELECT * FROM 
      ( -- Punti iniziali tratta
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM gas.fcl_g_dn_section  
       WHERE id_stato=3 and id_gestore in (15,999) and id_tipo_gas in (1,2,999) and id_tiporete in (1,2,999)
       UNION ALL 
        -- Punti finali tratta
       SELECT ST_EndPoint(geom) AS the_geom
       FROM gas.fcl_g_dn_section  
       WHERE id_stato=3 and id_gestore in (15,999) and id_tipo_gas in (1,2,999) and id_tiporete in (1,2,999)
       UNION ALL
       -- Raccordi e riduttori
	SELECT geom as the_geom
	from gas.fcl_g_component
	where gtype_id in (10,20) and id_stato=3 and id_gestore in (15,999) and id_tipo_gas in (1,2,999) and id_tiporete in (1,2,999)
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.id_gestore in (15,999) and l.id_tipo_gas in (1,2,999) and l.id_tiporete in (1,2,999)
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
         INSERT INTO grafo.archi_Gas (id_arco,id_elemento,da_nodo,a_nodo,da_tipo,a_tipo,the_geom)
         VALUES (
            nextval('grafo.archi_arco_id_seq_Gas'::regclass)::integer,
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

ALTER TABLE grafo.archi_Gas ADD CONSTRAINT archi_Gas_pkey PRIMARY KEY(id_arco); 


-- TABELLA DEI NODI RAGGRUPPATI PER GEOMETRIA E ASSEGNAZIONE DI ID UNIVOCO
DROP SEQUENCE if exists grafo.nodi_nodo_id_seq_Gas;
CREATE SEQUENCE grafo.nodi_nodo_id_seq_Gas INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.nodi_Gas cascade;
CREATE TABLE grafo.nodi_Gas AS
SELECT 
	nextval('grafo.nodi_nodo_id_seq_Gas'::regclass)::integer as id_nodo,
	array_accum(arco_entrante) AS arco_entrante,
	array_accum(arco_uscente) AS arco_uscente,
	the_geom
FROM (
  SELECT 
    ST_StartPoint(the_geom) AS the_geom, 
    id_arco AS arco_uscente, -- fid anzichè gs_id
    NULL::integer AS arco_entrante
  FROM grafo.archi_Gas
  UNION ALL
  SELECT 
    ST_EndPoint(the_geom) AS the_geom, 
    NULL::integer AS arco_uscente,
    id_arco AS arco_entrante -- fid anzichè gs_id
  FROM grafo.archi_Gas
) AS foo
GROUP BY the_geom;
ALTER TABLE grafo.nodi_Gas ADD PRIMARY KEY (id_nodo);


--ESPANDO LA TABELLA DEI NODI PER POTER FARE LE QUERY DI JOIN E AGGIORNARE LA TABELLA DEGLI ARCHI
UPDATE grafo.archi_Gas a SET da_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_Gas
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


UPDATE grafo.archi_Gas a SET a_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_Gas
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
ALTER TABLE grafo.nodi_Gas ADD COLUMN tipo_nodo character varying;
ALTER TABLE grafo.nodi_Gas ADD COLUMN id_elemento integer;

-- VALVOLE
update grafo.nodi_Gas set tipo_nodo='valvola', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_Gas n, gas.fcl_g_isolation_device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and e.id_gestore in (15,999) and e.id_tipo_gas in (1,2,999) and e.id_tiporete in (1,2,999)) as foo where nodi_Gas.id_nodo=foo.id_nodo;

--punti misura (terminali)
--update grafo.nodi_Gas set tipo_nodo='punti misura', id_elemento = fid from
--(select fid, id_nodo from grafo.nodi_Gas n, gas.fcl_g_punti_misura e where
--ST_DWithin(n.the_geom,e.geom,0.01) e.id_stato=3 and e.id_gestore in (15,999) and e.id_tipo_gas in (1,2,999)) as foo where nodi_Gas.id_nodo=foo.id_nodo;

--raccordi (quadrati)
update grafo.nodi_Gas set tipo_nodo='raccordo', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_Gas n, gas.fcl_g_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and gtype_id=10 and e.id_stato=3 and e.id_gestore in (15,999) 
 and e.id_tipo_gas in (1,2,999) and e.id_tiporete in (1,2,999) 
 and e.id_tipologia not in (3,7,8,9,10,11,12,18,22,27,28,33,39,40,999)) as foo where nodi_Gas.id_nodo=foo.id_nodo;

--montanti
update grafo.nodi_Gas set tipo_nodo='montante', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_Gas n, gas.fcl_g_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=40 and e.id_stato=3 and e.id_gestore in (15,999) and e.id_tipo_gas in (1,2,999) and e.id_tiporete in (1,2,999)) as foo where nodi_Gas.id_nodo=foo.id_nodo;

-- UTENZA
update grafo.nodi_Gas set tipo_nodo='utenza', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_Gas n, gas.fcl_g_service e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_Stato=3 and e.id_Gestore in (15,999) and e.id_Tipo_gas in (1,2,999)) as foo where nodi_Gas.id_nodo=foo.id_nodo;
-- RIDUTTORI UTENZA
update grafo.nodi_Gas set tipo_nodo='riduttore utenza', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_Gas n, gas.fcl_g_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.gtype_id=20 and e.id_Stato=3 and e.id_Gestore in (15,999) and e.id_Tipo_gas in (1,2,999) and e.id_Tiporete in (1,2,999)) as foo where nodi_Gas.id_nodo=foo.id_nodo;

-- CABINA EROGAZIONE
update grafo.nodi_Gas set tipo_nodo='cabina erogazione', id_elemento = fid from
(select fid, id_nodo from grafo.nodi_Gas n, gas.fcl_g_installation e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and e.id_gestore in (15,999) and e.gtype_id in (10,20) and e.id_tipologia in (1,2,3,4,5,6,999)) as foo where nodi_Gas.id_nodo=foo.id_nodo;

update grafo.nodi_Gas set tipo_nodo='altro' where tipo_nodo is null;

CREATE INDEX nodi_Gas_tipo_idx ON grafo.nodi_Gas (tipo_nodo);

-- AGGIORNO LA TABELLA ARCHI CON I TIPI E INDICI
UPDATE grafo.archi_Gas set da_tipo = nodi_Gas.tipo_nodo FROM grafo.nodi_Gas WHERE da_nodo=nodi_Gas.id_nodo;
UPDATE grafo.archi_Gas set a_tipo = nodi_Gas.tipo_nodo FROM grafo.nodi_Gas WHERE a_nodo=nodi_Gas.id_nodo;
CREATE INDEX archi_Gas_da_nodo_idx ON grafo.archi_Gas (da_nodo);
CREATE INDEX archi_Gas_a_nodo_idx ON grafo.archi_Gas (a_nodo);
CREATE INDEX archi_Gas_the_geom_gist ON grafo.archi_Gas USING gist (the_geom);

END$$;
