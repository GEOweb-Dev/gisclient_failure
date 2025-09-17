DO $$
DECLARE
    arco_fid integer := 0;
	arco_objId varchar(22) := '';
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
    rcd RECORD;
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
DROP SEQUENCE if exists grafo.archi_arco_id_seq_ele;
CREATE SEQUENCE grafo.archi_arco_id_seq_ele INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.archi_ele cascade;
CREATE TABLE grafo.archi_ele AS
(SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   1 as tipo
FROM
   elettricita.fcl_e_bt_section
where id_stato = 3 AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM elettricita.fcl_e_bt_section l, 
   (SELECT * from 
      ( -- Punti iniziali tratta
       SELECT ST_StartPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_bt_section 
       WHERE id_stato = 3 
       UNION ALL 
        -- Punti finali tratta
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_bt_section 
       WHERE id_stato = 3
      ) AS foo GROUP BY the_geom
   ) AS x
   WHERE id_stato = 3
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.03)
)) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   2 as tipo
FROM
   elettricita.fcl_e_mt_section
where id_stato = 3 AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM elettricita.fcl_e_mt_section l, 
   (SELECT * from 
      (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_mt_section 
       where id_stato=3
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_mt_section 
       where id_stato=3
      ) AS foo GROUP BY the_geom
   ) AS x
   WHERE id_stato = 3
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.03)
)) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   3 as tipo
FROM
   elettricita.fcl_e_at_section
where id_stato = 3 AND fid NOT IN (
   SELECT DISTINCT l.fid
   FROM elettricita.fcl_e_at_section l, 
   (SELECT * from 
	   (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_at_section 
       where id_stato=3
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_at_section 
       where id_stato=3
      ) AS foo GROUP BY the_geom
   ) AS x
   WHERE id_stato = 3
   AND NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   and NOT st_equals(x.the_geom,ST_EndPoint(l.geom))
   AND ST_DWithin(l.geom,x.the_geom,0.03)
)) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   4 as tipo --archi bt
FROM
   elettricita.fcl_e_plant_schemasection
where id_stato = 3 and gtype_id=300) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   7 as tipo --archi bt
FROM
   elettricita.fcl_e_plant_busbar
where id_stato = 3 and gtype_id=200) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   5 as tipo --archi mt
FROM
   elettricita.fcl_e_plant_schemasection
where id_stato = 3 and gtype_id=200) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   8 as tipo --archi mt
FROM
   elettricita.fcl_e_plant_busbar
where id_stato = 3 and gtype_id=100 ) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   6 as tipo --archi at
FROM
   elettricita.fcl_e_plant_schemasection
where id_stato = 3 and gtype_id=100 ) union all (
SELECT 
   nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer as id_arco,
   obj_id AS id_elemento, -- fid è la chiave anzichè gs_id
   NULL::integer as da_nodo,
   NULL::integer as a_nodo,
   NULL::character varying as da_tipo,
   NULL::character varying as a_tipo,  
   geom as the_geom,
   9 as tipo --archi at
FROM
   elettricita.fcl_e_plant_busbar
where id_stato = 3 and gtype_id=300);

OPEN crs_split FOR 
   (SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 1 as tipo 
   FROM elettricita.fcl_e_bt_section l, 
   (SELECT * FROM 
      (SELECT ST_StartPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_bt_section 
       WHERE id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_bt_section 
       WHERE id_stato = 3
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_Stato=3 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id) union all (
   SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 2 as tipo
   FROM elettricita.fcl_e_mt_section l, 
   (SELECT * FROM 
      (SELECT ST_StartPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_mt_section 
       WHERE id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_mt_section 
       WHERE id_stato = 3
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id) union all (
      SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 3 as tipo
   FROM elettricita.fcl_e_at_section l, 
   (SELECT * FROM 
      (SELECT ST_StartPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_at_section 
       WHERE id_stato = 3 
       UNION ALL 
       SELECT ST_EndPoint(geom) AS the_geom 
       FROM elettricita.fcl_e_at_section 
       WHERE id_stato = 3
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id
   )/* union all (
   SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 4 as tipo
   FROM elettricita.fcl_e_plant_schemasection l, 
   (SELECT * FROM 
      (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_plant_busbar
       where id_stato=3 and gtype_id in (200)
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_plant_busbar
       where id_stato=3 and gtype_id in (200)
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.gtype_id=300 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id
   ) */union all (
   SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 7 as tipo
   FROM elettricita.fcl_e_plant_busbar l, 
   (SELECT * FROM 
      (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_plant_schemasection
       where id_stato=3 and gtype_id in (300)
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_plant_schemasection
       where id_stato=3 and gtype_id in (300)
	   ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.gtype_id=200 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id
   
   ) /*union all (
   SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 5 as tipo
   FROM elettricita.fcl_e_plant_schemasection l, 
   (SELECT * FROM 
      (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_plant_busbar
       where id_stato=3 and gtype_id in (100)
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_plant_busbar
       where id_stato=3 and gtype_id in (100)
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.gtype_id=200 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id
   ) */union all (
   
   SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 8 as tipo
   FROM elettricita.fcl_e_plant_busbar l, 
   (SELECT * FROM 
      (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_plant_schemasection
       where id_stato=3 and gtype_id in (200)
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_plant_schemasection
       where id_stato=3 and gtype_id in (200)
	   ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.gtype_id=100 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id
   
   ) /*union all (
   SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 6 as tipo
   FROM elettricita.fcl_e_plant_schemasection l, 
   (SELECT * FROM 
      (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_plant_busbar
       where id_stato=3 and gtype_id in (300)
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_plant_busbar
       where id_stato=3 and gtype_id in (300)
      ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.gtype_id=100 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id
   ) */union all(
   
   SELECT DISTINCT l.obj_id,l.geom as the_geom,x.the_geom as the_geom_node, 9 as tipo
   FROM elettricita.fcl_e_plant_busbar l, 
   (SELECT * FROM 
      (select ST_StartPoint(geom) as the_geom
       from elettricita.fcl_e_plant_schemasection
       where id_stato=3 and gtype_id in (100)
       union all
       select ST_EndPoint(geom) as the_geom
       from elettricita.fcl_e_plant_schemasection
       where id_stato=3 and gtype_id in (100)
	   ) AS foo GROUP BY the_geom
   ) AS x 
   WHERE l.id_stato=3 and l.gtype_id=300 and NOT st_equals(x.the_geom,ST_StartPoint(l.geom)) 
   AND not st_equals(x.the_geom,ST_EndPoint(l.geom)) 
   AND ST_DWithin(l.geom,x.the_geom,0.03)
   ORDER BY l.obj_id
   );
   
LOOP
	FETCH crs_split INTO rcd;
	IF ((arco_objId <> rcd.obj_id OR arco_tipo<>rcd.tipo) AND arco_objId <> '' and arco_tipo<>0) OR NOT FOUND THEN
		num_splits := 1;
		LOOP
			IF ST_GeometryN(geometry_arc,num_splits) IS NULL THEN
				EXIT;
			END IF;
			INSERT INTO grafo.archi_ele(id_arco,id_elemento,da_nodo,a_nodo,da_tipo,a_tipo,the_geom,tipo)
				VALUES (nextval('grafo.archi_arco_id_seq_ele'::regclass)::integer,
				arco_objId,
				NULL,
				NULL,
				NULL,
				NULL,
				ST_GeometryN(geometry_arc,num_splits), arco_tipo);
				num_splits := num_splits+1;
		END LOOP;
	END IF;
	EXIT WHEN NOT FOUND;
	IF (arco_objId <> rcd.obj_id or arco_tipo<>rcd.tipo) THEN
		arco_objId := rcd.obj_id;
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

ALTER TABLE grafo.archi_ele ADD CONSTRAINT archi_ele_pkey PRIMARY KEY(id_arco); 


-- TABELLA DEI NODI RAGGRUPPATI PER GEOMETRIA E ASSEGNAZIONE DI ID UNIVOCO
DROP SEQUENCE if exists grafo.nodi_nodo_id_seq_ele;
CREATE SEQUENCE grafo.nodi_nodo_id_seq_ele INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

DROP TABLE if exists grafo.nodi_ele cascade;
CREATE TABLE grafo.nodi_ele AS
SELECT 
	nextval('grafo.nodi_nodo_id_seq_ele'::regclass)::integer as id_nodo,
	array_remove(array_accum(arco_entrante),NULL) AS arco_entrante,
	array_remove(array_accum(arco_uscente),NULL) AS arco_uscente,
	the_geom
FROM (
  SELECT 
    ST_StartPoint(the_geom) AS the_geom, 
    id_arco AS arco_uscente, -- fid anzichè gs_id
    NULL::integer AS arco_entrante
  FROM grafo.archi_ele
  UNION ALL
  SELECT 
    ST_EndPoint(the_geom) AS the_geom, 
    NULL::integer AS arco_uscente,
    id_arco AS arco_entrante -- fid anzichè gs_id
  FROM grafo.archi_ele
) AS foo
GROUP BY the_geom;

ALTER TABLE grafo.nodi_ele ADD PRIMARY KEY (id_nodo);


--ESPANDO LA TABELLA DEI NODI PER POTER FARE LE QUERY DI JOIN E AGGIORNARE LA TABELLA DEGLI ARCHI
UPDATE grafo.archi_ele a SET da_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_ele
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


UPDATE grafo.archi_ele a SET a_nodo = b.id_nodo FROM
	(WITH 
	nodi_serie AS (
		  SELECT 
		    id_nodo, 
		    arco_uscente, 
		    generate_series(1, array_upper(arco_uscente, 1)) AS uscente_upper,
		    arco_entrante, 
		    generate_series(1, array_upper(arco_entrante, 1)) AS entrante_upper
		  FROM grafo.nodi_ele
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
ALTER TABLE grafo.nodi_ele ADD COLUMN tipo_nodo character varying;
ALTER TABLE grafo.nodi_ele ADD COLUMN id_elemento character varying;

--utenze -> fcl_e_Xt_service
update grafo.nodi_ele set tipo_nodo='utenze BT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_bt_service e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='utenze MT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_service e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='utenze AT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_service e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3) as foo where nodi_ele.id_nodo=foo.id_nodo;

--cass derivazione -> fcl_e_bt_component, gtypeId=112 / fcl_e_mt_component, gtypeId=204
update grafo.nodi_ele set tipo_nodo='cassetta derivazione BT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_bt_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=112) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='cassetta derivazione MT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=204) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='cassetta derivazione AT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_at_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=303) as foo where nodi_ele.id_nodo=foo.id_nodo;

--raccordi ->
update grafo.nodi_ele set tipo_nodo='raccordi BT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_bt_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=111) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='raccordi MT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=200) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='raccordi AT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_at_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=300) as foo where nodi_ele.id_nodo=foo.id_nodo;

--nodo confine
update grafo.nodi_ele set tipo_nodo='nodo confine BT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_bt_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=100) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='nodo confine MT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=203) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='nodo confine AT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_at_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=302) as foo where nodi_ele.id_nodo=foo.id_nodo;

--sezionatori
update grafo.nodi_ele set tipo_nodo='sezionatori BT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_bt_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=203) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='sezionatori MT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=204) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='sezionatori AT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_at_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=205) as foo where nodi_ele.id_nodo=foo.id_nodo;

--nodi origine
update grafo.nodi_ele set tipo_nodo='origine BT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_bt_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=303) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='origine MT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=304) as foo where nodi_ele.id_nodo=foo.id_nodo;
update grafo.nodi_ele set tipo_nodo='origine AT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_at_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=305) as foo where nodi_ele.id_nodo=foo.id_nodo;

--nodi linea
update grafo.nodi_ele set tipo_nodo='nodo linea MT', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_mt_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=900) as foo where nodi_ele.id_nodo=foo.id_nodo;


--armadio bt -> fcl_installation gtype=119
update grafo.nodi_ele set tipo_nodo='armadio', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_installation e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=119) as foo where nodi_ele.id_nodo=foo.id_nodo;

--interruttore origine bt -> fcl_E_bt_isolation_Device gtype=950
update grafo.nodi_ele set tipo_nodo='interruttore', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_bt_isolation_Device e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id=950) as foo where nodi_ele.id_nodo=foo.id_nodo;

--trasformatore
update grafo.nodi_ele set tipo_nodo='trasformatore', id_elemento = obj_id from
(select obj_id, id_nodo from grafo.nodi_ele n, elettricita.fcl_e_plant_component e where
ST_DWithin(n.the_geom,e.geom,0.01) and e.id_stato=3 and gtype_id in (150,200,100,190)) as foo where nodi_ele.id_nodo=foo.id_nodo;

--ELEMENTO GENERICO
update grafo.nodi_ele set tipo_nodo='altro' where tipo_nodo is null;

CREATE INDEX nodi_ele_tipo_idx ON grafo.nodi_ele (tipo_nodo);

-- AGGIORNO LA TABELLA ARCHI CON I TIPI E INDICI
UPDATE grafo.archi_ele set da_tipo = nodi_ele.tipo_nodo FROM grafo.nodi_ele WHERE da_nodo= nodi_ele.id_nodo;
UPDATE grafo.archi_ele set a_tipo = nodi_ele.tipo_nodo FROM grafo.nodi_ele WHERE a_nodo= nodi_ele.id_nodo;

CREATE INDEX archi_ele_da_nodo_idx ON grafo.archi_ele (da_nodo);
CREATE INDEX archi_ele_a_nodo_idx ON grafo.archi_ele (a_nodo);
CREATE INDEX archi_ele_the_geom_gist ON grafo.archi_ele USING gist (the_geom);

drop sequence if exists grafo.sentiero_seq_ele;
CREATE SEQUENCE grafo.sentiero_seq_ele INCREMENT 1 MINVALUE 1 MAXVALUE 9223372036854775807 START 1 CACHE 1;

drop table if exists grafo.sentiero_ele cascade;
create table grafo.sentiero_ele AS (
select 
nextval('grafo.sentiero_seq_ele'::regclass)::integer as id,
NULL::character varying as idOrigine,	
NULL::integer as progressivo,
NULL::character varying as idArco,
NULL::character varying as sezione, 
NULL::character varying as sorgente,
NULL::character varying as destinazione,
NULL::character varying as codiceSapCabinaSorgente,
NULL::character varying as tipologiaSorgente,
NULL::character varying as codiceSapCabinaDestinazione,
NULL::character varying as tipologiaDestinazione,
NULL::boolean as parallelo,
NULL::character varying as tipologia
); 

ALTER TABLE grafo.sentiero_ele ADD CONSTRAINT sentiero_ele_pkey PRIMARY KEY(id); 
ALTER TABLE grafo.sentiero_ele ADD CONSTRAINT sentiero_ele_unique UNIQUE(idOrigine,progressivo, idArco);

END$$;