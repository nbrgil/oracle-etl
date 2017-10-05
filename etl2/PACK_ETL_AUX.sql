CREATE OR REPLACE PACKAGE      PACK_ETL_AUX AS 
  
  c_ano_sc constant number := 2010;

   FUNCTION FN_GET_COLUNA_ENRIQUECIMENTO (
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_nome_coluna VARCHAR2
   ) RETURN VARCHAR2;
   
   PROCEDURE SP_REFRESH_SOLR_INDEX(p_projeto_info etl2.pack_etl.rec_projeto_info);
   
   PROCEDURE SP_ENRIQUEC_GEOCODER_ORACLE(p_interface_info etl2.pack_etl.rec_interface_info);
   
   PROCEDURE SP_ENRIQUEC_RENDA_PRED_ORACLE(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);
   
   PROCEDURE SP_ENRIQ_RENDA_PRED_ORACLE_FX(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);
   
   PROCEDURE SP_ENRIQUEC_BAIRRO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);   
   
   PROCEDURE SP_ENRIQUEC_FAVELA(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);   
   
   PROCEDURE SP_ENRIQUEC_SUBDISTRITO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);   
   
   PROCEDURE SP_ENRIQUEC_DISTRITO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);   
   
   PROCEDURE SP_ENRIQUEC_MICRORREGIAO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);   

   PROCEDURE SP_ENRIQUEC_ISOCOTA(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL);   
   
   PROCEDURE SP_ENRIQUEC_SC(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL, p_ano_sc NUMBER := c_ano_sc);

END PACK_ETL_AUX;

/


CREATE OR REPLACE PACKAGE BODY           PACK_ETL_AUX AS

   TYPE tp_ref_cursor IS REF CURSOR;
   
   TYPE tp_endereco_geocoder IS RECORD (
      geom           SDO_GEOMETRY,
      logradouro     VARCHAR2(200), 
      numero         NUMBER,
      complemento    VARCHAR2(200),
      bairro         VARCHAR2(100),
      cep            VARCHAR2(20),
      ufmun_cod      NUMBER,
      geocodificacao NUMBER,
      match_value    NUMBER,
      error_message  VARCHAR2(20),
      altera_local   VARCHAR2(10),
      match_mode     VARCHAR2(50)
   );
   
   FUNCTION FN_GET_COLUNA_ENRIQUECIMENTO(
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_nome_coluna VARCHAR2
   ) RETURN VARCHAR2 AS
      v_alias_coluna VARCHAR2(30);
      
      CURSOR cur_busca_alias IS
         SELECT alias_coluna
         FROM etl2.vw_intf_enriq_coluna
         WHERE cod_interface = p_cod_interface AND nome_coluna = p_nome_coluna;
   BEGIN
      OPEN cur_busca_alias;
      FETCH cur_busca_alias INTO v_alias_coluna;
      IF cur_busca_alias%NOTFOUND THEN 
         v_alias_coluna := p_nome_coluna;
      END IF;
      CLOSE cur_busca_alias;
      
      RETURN v_alias_coluna;
   END;
   
   PROCEDURE SP_REFRESH_SOLR_INDEX(p_projeto_info etl2.pack_etl.rec_projeto_info) AS
      v_existe_camada BOOLEAN;
      v_solr_array    tools.pack_manipula_str.v_string_array;
      v_cod_empresa   admin.empresa.cod_empresa%TYPE;
   BEGIN
      v_existe_camada := FALSE;
      
      SELECT cod_empresa INTO v_cod_empresa
      FROM etl2.vw_projeto
      WHERE cod_projeto = p_projeto_info.cod_projeto;
      
      FOR x IN (
         SELECT DISTINCT a.cod_camada, a.solr_node, b.full_refresh
         FROM admin.vw_perfil_camada a 
            JOIN admin.objeto_fonte b ON (a.cod_objeto = b.cod_objeto)
         WHERE b.cod_projeto  = p_projeto_info.cod_projeto
         AND A.solr_node IS NOT NULL
      )
      LOOP
         v_existe_camada := TRUE;
         
         tools.pack_manipula_str.sp_quebra_str(';', x.solr_node, v_solr_array);
         FOR y IN v_solr_array.FIRST..v_solr_array.LAST 
         LOOP
            tools.sp_atualiza_indice(p_projeto_info.data_inicio, p_projeto_info.data_atual, v_cod_empresa, v_solr_array(y), x.cod_camada, x.full_refresh); 
         END LOOP;
      END LOOP;
               
      IF NOT v_existe_camada THEN
         tools.sp_atualiza_indice(p_projeto_info.data_inicio, p_projeto_info.data_atual, v_cod_empresa, 'pi');
      END IF;
   END SP_REFRESH_SOLR_INDEX;
  
   PROCEDURE SP_ENRIQUEC_GEOCODER_ORACLE(p_interface_info etl2.pack_etl.rec_interface_info) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      v_table_name VARCHAR2(100);
      
      v_registro_rowid        ROWID;
      v_coluna_geom           VARCHAR2(30);
      v_coluna_logradouro     VARCHAR2(30);
      v_coluna_numero         VARCHAR2(30);
      v_coluna_complemento    VARCHAR2(30);
      v_coluna_bairro         VARCHAR2(30);
      v_coluna_cep            VARCHAR2(30);
      v_coluna_ufmun_cod      VARCHAR2(30);
      v_coluna_geocodificacao VARCHAR2(30);
      v_coluna_match_value    VARCHAR2(30);
      v_coluna_error_message  VARCHAR2(30);
      v_coluna_altera_local   VARCHAR2(30);
      v_coluna_match_mode     VARCHAR2(30);
      
      v_old_geom           SDO_GEOMETRY;
      v_old_logradouro     VARCHAR2(200);
      v_old_numero         NUMBER;
      v_old_complemento    VARCHAR2(200);
      v_old_bairro         VARCHAR2(100);
      v_old_cep            VARCHAR2(20);
      v_old_ufmun_cod      NUMBER;
      v_old_geocodificacao NUMBER;
      v_old_match_value    NUMBER;
      v_old_error_message  VARCHAR2(20);
      v_old_altera_local   VARCHAR2(10);
      v_old_match_mode     VARCHAR2(50);
      
      v_new_geom           SDO_GEOMETRY;
      v_new_logradouro     VARCHAR2(200);
      v_new_numero         NUMBER;
      v_new_complemento    VARCHAR2(200);
      v_new_bairro         VARCHAR2(100);
      v_new_cep            VARCHAR2(20);
      v_new_ufmun_cod      NUMBER;
      v_new_geocodificacao NUMBER;
      v_new_match_value    NUMBER;
      v_new_error_message  VARCHAR2(20);
      v_new_altera_local   VARCHAR2(10);
      v_new_match_mode     VARCHAR2(50);
      
      v_updating CHAR(1);
   BEGIN 
      v_coluna_geom           := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_logradouro     := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'LOGRADOURO');
      v_coluna_numero         := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'NUMERO');
      v_coluna_complemento    := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COMPLEMENTO');
      v_coluna_bairro         := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'BAIRRO');
      v_coluna_cep            := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'CEP');
      v_coluna_ufmun_cod      := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'UFMUN_COD');
      v_coluna_geocodificacao := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOCODIFICACAO');
      v_coluna_match_value    := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'MATCH_VALUE');
      v_coluna_error_message  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'ERROR_MESSAGE');
      v_coluna_altera_local   := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'ALTERACAO_LOCALIZACAO');
      v_coluna_match_mode     := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'MATCH_MODE');
      
      v_sql_query := '';
      v_sql_query := v_sql_query || 'SELECT ';
      v_sql_query := v_sql_query || '   b.'||v_coluna_geom||' as old_geom,';
      v_sql_query := v_sql_query || '   b.'||v_coluna_logradouro||' as old_logradouro, ';
      v_sql_query := v_sql_query || '   b.'||v_coluna_numero||' as old_numero, ';
      v_sql_query := v_sql_query || '   b.'||v_coluna_complemento||' as old_complemento, ';
      v_sql_query := v_sql_query || '   b.'||v_coluna_bairro||' as old_bairro, ';
      v_sql_query := v_sql_query || '   b.'||v_coluna_cep||' as old_cep, ';
      v_sql_query := v_sql_query || '   b.'||v_coluna_ufmun_cod||' as old_ufmun_cod,';
      v_sql_query := v_sql_query || '   b.'||v_coluna_geocodificacao||' as old_geocodificacao,';
      v_sql_query := v_sql_query || '   b.'||v_coluna_match_value||' as old_match_value,';
      v_sql_query := v_sql_query || '   b.'||v_coluna_error_message||' as old_error_message,';
      v_sql_query := v_sql_query || '   b.'||v_coluna_altera_local||' as old_altera_local,';
      v_sql_query := v_sql_query || '   b.'||v_coluna_match_mode||' as old_match_mode,';
      v_sql_query := v_sql_query || '   CASE WHEN b.rowid IS NOT NULL THEN ''S'' ELSE ''N'' END as updating,';
      v_sql_query := v_sql_query || '   a.'||v_coluna_geom||' as new_geom, ';
      v_sql_query := v_sql_query || '   a.'||v_coluna_logradouro||' as new_logradouro, ';
      v_sql_query := v_sql_query || '   a.'||v_coluna_numero||' as new_numero, ';
      v_sql_query := v_sql_query || '   a.'||v_coluna_complemento||' as new_complemento, ';
      v_sql_query := v_sql_query || '   a.'||v_coluna_bairro||' as new_bairro, ';
      v_sql_query := v_sql_query || '   a.'||v_coluna_cep||' as new_cep, ';
      v_sql_query := v_sql_query || '   a.'||v_coluna_ufmun_cod||' as new_ufmun_cod,';
      v_sql_query := v_sql_query || '   a.'||v_coluna_geocodificacao||' as new_geocodificacao,';
      v_sql_query := v_sql_query || '   a.'||v_coluna_match_mode||' as new_match_mode ';
      v_sql_query := v_sql_query || ',  a.rowid as registro_rowid ';      
      v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
      v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'backup')||' b ON (a.cod_fluxo = b.cod_fluxo) ';
      v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
      v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL ';
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_geom||' = :1, ';
      v_sql_update := v_sql_update || '   '||v_coluna_geocodificacao||' = :2, ';
      v_sql_update := v_sql_update || '   '||v_coluna_match_value||' = :3, ';
      v_sql_update := v_sql_update || '   '||v_coluna_error_message||' = :4, ';
      v_sql_update := v_sql_update || '   '||v_coluna_altera_local||' = :5, ';
      v_sql_update := v_sql_update || '   '||v_coluna_match_mode||'  = :6 ';
      --v_sql_update := v_sql_update || '   editado  = ''N'' ';
      v_sql_update := v_sql_update || 'WHERE rowid = :7 ';
      
      DBMS_OUTPUT.PUT_LINE('v_sql_query: '||v_sql_query);
      DBMS_OUTPUT.PUT_LINE('v_sql_update: '||v_sql_update);
      
      OPEN v_cursor FOR v_sql_query;
      
      LOOP
         FETCH v_cursor 
         INTO 
            v_old_geom, 
            v_old_logradouro, 
            v_old_numero, 
            v_old_complemento, 
            v_old_bairro, 
            v_old_cep, 
            v_old_ufmun_cod, 
            v_old_geocodificacao, 
            v_old_match_value,
            v_old_error_message, 
            v_old_altera_local, 
            v_old_match_mode, 
            v_updating,
            v_new_geom,
            v_new_logradouro, 
            v_new_numero,
            v_new_complemento, 
            v_new_bairro, 
            v_new_cep, 
            v_new_ufmun_cod,
            v_new_geocodificacao,
            v_new_match_mode,
            v_registro_rowid;
            
         EXIT WHEN v_cursor%NOTFOUND;
         
         v_new_geom := 
            TOOLS.FN_GEOCODIFICACAO ( 
              p_endereco_antigo              => TOOLS.TP_ENDERECO(v_old_logradouro, v_old_numero, v_old_complemento, v_old_bairro, v_old_cep, v_old_ufmun_cod, v_old_geom)
            , p_endereco                     => TOOLS.TP_ENDERECO(v_new_logradouro, v_new_numero, v_new_complemento, v_new_bairro, v_new_cep, v_new_ufmun_cod, v_new_geom) 
            , p_geocodificacao_antigo        => v_old_geocodificacao
            , p_geocodificacao               => v_new_geocodificacao             
            , p_match_value_antigo           => v_old_match_value
            , p_match_value                  => v_new_match_value
            , p_error_message_antigo         => v_old_error_message 
            , p_error_message                => v_new_error_message 
            , p_alteracao_localizacao_antigo => v_old_altera_local
            , p_alteracao_localizacao        => v_new_altera_local
            , p_updating                     => CASE WHEN v_updating = 'S' THEN TRUE ELSE FALSE END
            , p_updating_geom                => TRUE
            , p_updating_geocodif            => TRUE
            , p_match_mode_antigo            => v_old_match_mode
            , p_match_mode                   => v_new_match_mode
            , p_validar_endereco_antigo      => 'S'
         ); 
         
         
         EXECUTE IMMEDIATE v_sql_update USING v_new_geom, v_new_geocodificacao, v_new_match_value, v_new_error_message, v_new_altera_local, v_new_match_mode, v_registro_rowid;
         v_new_geom := NULL;
      END LOOP;
      CLOSE v_cursor;
   END;
   
   PROCEDURE SP_ENRIQUEC_RENDA_PRED_ORACLE(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid             ROWID;
      v_coluna_geom                VARCHAR2(30);
      v_coluna_renda_predominante     VARCHAR2(30);
      v_coluna_classe_predominante    VARCHAR2(30);
      v_coluna_tipo_enriquecimento    VARCHAR2(30);
      
      v_geom                 SDO_GEOMETRY;
      v_renda_predominante   FLOAT;
      v_classe_predominante  VARCHAR2(3);
      v_tipo_enriquecimento  NUMBER(1);
      v_cod_enriquecimento   NUMBER;
      
      v_update_table VARCHAR2(100);
      v_erro CLOB;
          
   BEGIN 
      v_coluna_geom                 := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_renda_predominante   := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'RENDA_PREDOMINANTE');
      v_coluna_classe_predominante  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'CLASSE_PREDOMINANTE');
      v_coluna_tipo_enriquecimento  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'TIPO_ENRIQUECIMENTO');
      
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   a.'||v_coluna_geom||' as geom,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
         v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   a.'||v_coluna_geom||' as geom,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_renda_predominante||' = :1, ';
      v_sql_update := v_sql_update || '   '||v_coluna_classe_predominante||' = :2, ';
      v_sql_update := v_sql_update || '   '||v_coluna_tipo_enriquecimento||' = :3 ';
      v_sql_update := v_sql_update || 'WHERE rowid = :4 ';
      
      BEGIN
        SELECT cod_enriquecimento INTO v_cod_enriquecimento
        FROM admin.enriquecimento a
        JOIN etl2.vw_projeto b ON (b.cod_empresa = a.cod_empresa and b.nome = a.nome )
        JOIN etl2.vw_passo c ON (b.cod_projeto = c.cod_projeto)
        WHERE c.cod_interface = p_interface_info.cod_interface;
      EXCEPTION
         WHEN no_data_found THEN
            RAISE_APPLICATION_ERROR(-20000, 'Configuração de enriquecimento não encontrada.');
      END;
      
      OPEN v_cursor FOR v_sql_query;
      LOOP
         FETCH v_cursor 
         INTO 
            v_geom,
            v_registro_rowid;
         
         EXIT WHEN v_cursor%NOTFOUND;
               
         BEGIN
             TOOLS.SP_ENRIQUECER_REGISTRO (pGeom => v_geom, 
                                           pCodEnriquecimento => v_cod_enriquecimento,
                                           pRenda => v_renda_predominante, 
                                           pClasse => v_classe_predominante,
                                           pTipoEnriquecimento => v_tipo_enriquecimento);
         EXCEPTION
            WHEN OTHERS THEN
               v_erro := sqlerrm;
               RAISE_APPLICATION_ERROR(-20000, 'Erro de enriquecimento (SP_ENRIQUEC_RENDA_PRED_ORACLE) '||v_registro_rowid || '--' || v_erro);
         END;
                                        
         EXECUTE IMMEDIATE v_sql_update USING v_renda_predominante, v_classe_predominante, v_tipo_enriquecimento, v_registro_rowid;
         IF p_gt_table IS NULL THEN 
            COMMIT;
         END IF;
      END LOOP;
      CLOSE v_cursor;
   END;
   
   PROCEDURE SP_ENRIQ_RENDA_PRED_ORACLE_FX(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid             ROWID;
      v_coluna_geom                VARCHAR2(30);
      v_coluna_renda_predominante  VARCHAR2(30);
      v_coluna_classe_predominante VARCHAR2(30);
      v_coluna_tipo_enriquecimento VARCHAR2(30);
      v_coluna_faixa_A1            VARCHAR2(30);
      v_coluna_faixa_A2            VARCHAR2(30);
      v_coluna_faixa_B1            VARCHAR2(30);
      v_coluna_faixa_B2            VARCHAR2(30);
      v_coluna_faixa_C1            VARCHAR2(30);
      v_coluna_faixa_C2            VARCHAR2(30);
      v_coluna_faixa_D             VARCHAR2(30);
      v_coluna_faixa_E             VARCHAR2(30);
      v_coluna_total               VARCHAR2(30);
      
      v_geom                SDO_GEOMETRY;
      v_renda_predominante  FLOAT;
      v_classe_predominante CHAR(3);
      v_tipo_enriquecimento NUMBER(1);
      v_cod_enriquecimento  NUMBER;
      v_faixa_A1            NUMBER;
      v_faixa_A2            NUMBER;
      v_faixa_B1            NUMBER;
      v_faixa_B2            NUMBER;
      v_faixa_C1            NUMBER;
      v_faixa_C2            NUMBER;
      v_faixa_D             NUMBER;
      v_faixa_E             NUMBER;
      v_total               NUMBER;
      
      v_update_table VARCHAR2(100);
      v_erro CLOB;
          
   BEGIN 
      v_coluna_geom                := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_renda_predominante  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'RENDA_PREDOMINANTE');
      v_coluna_classe_predominante := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'CLASSE_PREDOMINANTE');
      v_coluna_tipo_enriquecimento := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'TIPO_ENRIQUECIMENTO');
      v_coluna_faixa_A1            := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA1');
      v_coluna_faixa_A2            := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA2');
      v_coluna_faixa_B1            := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA3');
      v_coluna_faixa_B2            := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA4');
      v_coluna_faixa_C1            := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA5');
      v_coluna_faixa_C2            := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA6');
      v_coluna_faixa_D             := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA7');
      v_coluna_faixa_E             := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'FAIXA8');
      v_coluna_total               := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'TOTAL');
      
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   a.'||v_coluna_geom||' as geom,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
         v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   a.'||v_coluna_geom||' as geom,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_renda_predominante||' = :1, ';
      v_sql_update := v_sql_update || '   '||v_coluna_classe_predominante||' = :2, ';
      v_sql_update := v_sql_update || '   '||v_coluna_tipo_enriquecimento||' = :3, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_A1||' = :4, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_A2||' = :5, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_B1||' = :6, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_B2||' = :7, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_C1||' = :8, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_C2||' = :9, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_D||' = :10, ';
      v_sql_update := v_sql_update || '   '||v_coluna_faixa_E||' = :11, ';
      v_sql_update := v_sql_update || '   '||v_coluna_total||' = :12 ';
      v_sql_update := v_sql_update || 'WHERE rowid = :13 ';
      
      SELECT cod_enriquecimento INTO v_cod_enriquecimento
      FROM admin.enriquecimento a
      JOIN etl2.vw_projeto b ON (b.cod_empresa = a.cod_empresa and b.nome = a.nome )
      JOIN etl2.vw_passo c ON (b.cod_projeto = c.cod_projeto)
      WHERE c.cod_interface = p_interface_info.cod_interface;
      
      OPEN v_cursor FOR v_sql_query;
      LOOP
         FETCH v_cursor 
         INTO 
            v_geom,
            v_registro_rowid;
         
         EXIT WHEN v_cursor%NOTFOUND;
               
         BEGIN
             TOOLS.SP_ENRIQUECER_REGISTRO_FX (pGeom => v_geom, 
                                              pCodEnriquecimento => v_cod_enriquecimento,
                                              pRenda => v_renda_predominante, 
                                              pClasse => v_classe_predominante,
                                              pTipoEnriquecimento => v_tipo_enriquecimento,
                                              pFaixaRendaA1 => v_faixa_A1,
                                              pFaixaRendaA2 => v_faixa_A2,
                                              pFaixaRendaB1 => v_faixa_B1,
                                              pFaixaRendaB2 => v_faixa_B2,
                                              pFaixaRendaC1 => v_faixa_C1,
                                              pFaixaRendaC2 => v_faixa_C2,
                                              pFaixaRendaD => v_faixa_D,
                                              pFaixaRendaE => v_faixa_E,
                                              pFaixaTotal => v_total);
         EXCEPTION
            WHEN OTHERS THEN
               v_erro := sqlerrm;
               RAISE_APPLICATION_ERROR(-20000, 'Erro de enriquecimento (SP_ENRIQUEC_RENDA_PRED_ORACLE_FX) '||v_registro_rowid || '--' || v_erro);
         END;
                                        
         EXECUTE IMMEDIATE v_sql_update USING v_renda_predominante, v_classe_predominante, v_tipo_enriquecimento, v_faixa_A1, v_faixa_A2, v_faixa_B1, v_faixa_B2, v_faixa_C1, v_faixa_C2, v_faixa_D, v_faixa_E, v_total, v_registro_rowid;
         IF p_gt_table IS NULL THEN 
            COMMIT;
         END IF;
      END LOOP;
      CLOSE v_cursor;
   END SP_ENRIQ_RENDA_PRED_ORACLE_FX;
   
   
   PROCEDURE SP_ENRIQUEC_BAIRRO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid             ROWID;
      v_coluna_geom                VARCHAR2(30);
      v_coluna_cod                 VARCHAR2(30);
            
      v_cod                  NUMBER;
      v_cod_enriquecimento   NUMBER;
      
      v_update_table VARCHAR2(100);
          
   BEGIN 
      v_coluna_geom := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_cod  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COD_BAIRRO');
            
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_BAIRRO(a.'||v_coluna_geom||') as cod_bairro,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
         v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_BAIRRO(a.'||v_coluna_geom||') as cod_bairro,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_cod||' = :1 ';            
      v_sql_update := v_sql_update || 'WHERE rowid = :2 ';
            
      OPEN v_cursor FOR v_sql_query;
      LOOP
       FETCH v_cursor 
       INTO 
          v_cod,
          v_registro_rowid;
      
       EXIT WHEN v_cursor%NOTFOUND;
                                     
       EXECUTE IMMEDIATE v_sql_update USING v_cod, v_registro_rowid;
      END LOOP;
      CLOSE v_cursor;
      
   END;
   
   
   PROCEDURE SP_ENRIQUEC_FAVELA(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid      ROWID;
      v_coluna_geom         VARCHAR2(30);
      v_coluna_cod          VARCHAR2(30);
            
      v_cod          NUMBER;
      v_cod_enriquecimento  NUMBER;
      
      v_update_table VARCHAR2(100);
          
   BEGIN 
      v_coluna_geom := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_cod  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COD_FAVELA');
            
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_FAVELA(a.'||v_coluna_geom||') as cod_favela,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
         v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_FAVELA(a.'||v_coluna_geom||') as cod_favela,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_cod||' = :1 ';            
      v_sql_update := v_sql_update || 'WHERE rowid = :2 ';
            
      OPEN v_cursor FOR v_sql_query;
      LOOP
       FETCH v_cursor 
       INTO 
          v_cod,
          v_registro_rowid;
      
       EXIT WHEN v_cursor%NOTFOUND;
                                     
       EXECUTE IMMEDIATE v_sql_update USING v_cod, v_registro_rowid;
      END LOOP;
      CLOSE v_cursor;
      
   END;
   

   PROCEDURE SP_ENRIQUEC_SUBDISTRITO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid      ROWID;
      v_coluna_geom         VARCHAR2(30);
      v_coluna_cod          VARCHAR2(30);
            
      v_cod          NUMBER;
      v_cod_enriquecimento  NUMBER;
      
      v_update_table VARCHAR2(100);
          
   BEGIN 
      v_coluna_geom := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_cod  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COD_SUBDISTRITO');
            
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_SUBDISTRITO(a.'||v_coluna_geom||') as cod_subdistrito,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
         v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_SUBDISTRITO(a.'||v_coluna_geom||') as cod_subdistrito,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_cod||' = :1 ';            
      v_sql_update := v_sql_update || 'WHERE rowid = :2 ';
            
      OPEN v_cursor FOR v_sql_query;
      LOOP
       FETCH v_cursor 
       INTO 
          v_cod,
          v_registro_rowid;
      
       EXIT WHEN v_cursor%NOTFOUND;
                                     
       EXECUTE IMMEDIATE v_sql_update USING v_cod, v_registro_rowid;
      END LOOP;
      CLOSE v_cursor;
      
   END;
   
   
      PROCEDURE SP_ENRIQUEC_DISTRITO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid      ROWID;
      v_coluna_geom         VARCHAR2(30);
      v_coluna_cod          VARCHAR2(30);
            
      v_cod          NUMBER;
      v_cod_enriquecimento  NUMBER;
      
      v_update_table VARCHAR2(100);
          
   BEGIN 
      v_coluna_geom := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_cod  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COD_DISTRITO');
            
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_DISTRITO(a.'||v_coluna_geom||') as cod_distrito,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
         v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_DISTRITO(a.'||v_coluna_geom||') as cod_distrito,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_cod||' = :1 ';            
      v_sql_update := v_sql_update || 'WHERE rowid = :2 ';
            
      OPEN v_cursor FOR v_sql_query;
      LOOP
       FETCH v_cursor 
       INTO 
          v_cod,
          v_registro_rowid;
      
       EXIT WHEN v_cursor%NOTFOUND;
                                     
       EXECUTE IMMEDIATE v_sql_update USING v_cod, v_registro_rowid;
      END LOOP;
      CLOSE v_cursor;
      
   END;
   
   /*
   PROCEDURE SP_ENRIQUEC_MICRORREGIAO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid      ROWID;
      v_coluna_geom         VARCHAR2(30);
      v_coluna_cod          VARCHAR2(30);
            
      v_cod          NUMBER;
      v_cod_enriquecimento  NUMBER;
      
      v_update_table VARCHAR2(100);
          
   BEGIN 
      v_coluna_geom := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_cod  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COD_MICRORREGIAO');
            
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_MR(a.'||v_coluna_geom||') as cod_microrregiao,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
         v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_MR(a.'||v_coluna_geom||') as cod_microrregiao,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
      
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_cod||' = :1 ';            
      v_sql_update := v_sql_update || 'WHERE rowid = :2 ';
            
      OPEN v_cursor FOR v_sql_query;
      LOOP
       FETCH v_cursor 
       INTO 
          v_cod,
          v_registro_rowid;
      
       EXIT WHEN v_cursor%NOTFOUND;
                                     
       EXECUTE IMMEDIATE v_sql_update USING v_cod, v_registro_rowid;
      END LOOP;
      CLOSE v_cursor;
      
   END;
   */
   
   PROCEDURE SP_ENRIQUEC_MICRORREGIAO(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
      v_sql_query CLOB;
      v_sql_update CLOB;
      v_cursor tp_ref_cursor;
      
      v_registro_rowid      ROWID;
      v_coluna_geom         VARCHAR2(30);
      v_coluna_cod          VARCHAR2(30);
      v_coluna_ano          VARCHAR2(30);
          
      v_cod          NUMBER;
      v_cod_enriquecimento  NUMBER;
      
      v_update_table VARCHAR2(100);
      
      TYPE rec_mr IS RECORD (
         cod_microrregiao NUMBER,
         registro_rowid UROWID
      );
      TYPE tb_mr IS TABLE OF rec_mr INDEX BY PLS_INTEGER;
      l_mr tb_mr;
           
   BEGIN 
      v_coluna_geom := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
      v_coluna_cod  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COD_MICRORREGIAO');
             
      IF p_gt_table IS NULL THEN         
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT a.cod_microrregiao, b.rowid ';
         v_sql_query := v_sql_query || 'FROM mapa_urbano.microrregiao a ';
         v_sql_query := v_sql_query || '   CROSS JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' b ';
         v_sql_query := v_sql_query || '   LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (b.cod_fluxo = c.cod_fluxo) ';
         v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND b.'||v_coluna_geom||' is not null ';
         v_sql_query := v_sql_query || 'AND SDO_RELATE(a.geom, b.geom, ''mask=ANYINTERACT'') = ''TRUE''  ';
         v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
      ELSE
         v_sql_query := '';
         v_sql_query := v_sql_query || 'SELECT ';
         v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_MR(a.'||v_coluna_geom||', ''ANYINTERACT'') as cod_microrregiao,';
         v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
         v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
         v_update_table := p_gt_table;
      END IF;
       
      v_sql_update := '';
      v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
      v_sql_update := v_sql_update || 'SET ';
      v_sql_update := v_sql_update || '   '||v_coluna_cod||' = :1 ';
      v_sql_update := v_sql_update || 'WHERE rowid = :2 ';
      
      DBMS_OUTPUT.PUT_LINE('SQL QUERY (mr): '||v_sql_query);
      DBMS_OUTPUT.PUT_LINE('SQL UPDATE (mr): '||v_sql_update);
             
      OPEN v_cursor FOR v_sql_query;
      LOOP
         FETCH v_cursor
         BULK COLLECT INTO l_mr
         LIMIT 1000;
         
         FORALL v_indx IN 1 .. l_mr.COUNT     
            EXECUTE IMMEDIATE v_sql_update 
            USING 
               l_mr(v_indx).cod_microrregiao,  
               l_mr(v_indx).registro_rowid;
         IF p_gt_table IS NULL THEN         
            COMMIT;
         END IF;
         EXIT WHEN v_cursor%NOTFOUND;
      END LOOP;
      IF p_gt_table IS NULL THEN         
         COMMIT;
      END IF;
      CLOSE v_cursor;
    END;

PROCEDURE SP_ENRIQUEC_ISOCOTA (p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL) AS
   v_coluna_isocota VARCHAR2(30);
   v_coluna_geom    VARCHAR2(30);
   
   TYPE tp_ref_cursor IS REF CURSOR;
   
   v_registro_rowid ROWID;
   v_isocota SDO_GEOMETRY;
   
   v_sql_query CLOB;
   v_sql_update CLOB;
   v_cursor tp_ref_cursor;
   v_table_name VARCHAR2(100);
   
   v_update_table VARCHAR2(100);
   
BEGIN
   v_coluna_geom    := etl2.pack_etl_aux.fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
   v_coluna_isocota := etl2.pack_etl_aux.fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'ISOCOTA');
   
   IF p_gt_table IS NULL THEN         
      v_sql_query := '';
      v_sql_query := v_sql_query || 'SELECT ';
      v_sql_query := v_sql_query || '   SDO_GEOM.SDO_BUFFER(a.'||v_coluna_geom||', 1, 0.05, ''UNIT=KM'') as isocota, ';
      v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
      v_sql_query := v_sql_query || 'FROM etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' a ';
      v_sql_query := v_sql_query || 'LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
      v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND a.'||v_coluna_geom||' is not null';       
      v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
   ELSE
      v_sql_query := '';
      v_sql_query := v_sql_query || 'SELECT ';
      v_sql_query := v_sql_query || '   SDO_GEOM.SDO_BUFFER(a.'||v_coluna_geom||', 1, 0.05, ''UNIT=KM'') as isocota, ';
      v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
      v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
      v_update_table := p_gt_table;
   END IF;
   
   v_sql_update := '';
   v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
   v_sql_update := v_sql_update || 'SET ';
   v_sql_update := v_sql_update || '   '||v_coluna_isocota||' = :1 ';            
   v_sql_update := v_sql_update || 'WHERE rowid = :2 ';
         
   OPEN v_cursor FOR v_sql_query;
   LOOP
    FETCH v_cursor 
    INTO 
       v_isocota,
       v_registro_rowid;
   
    EXIT WHEN v_cursor%NOTFOUND;
                                  
    EXECUTE IMMEDIATE v_sql_update USING v_isocota, v_registro_rowid;
   END LOOP;
   CLOSE v_cursor;

END;

PROCEDURE SP_ENRIQUEC_SC(p_interface_info etl2.pack_etl.rec_interface_info, p_gt_table VARCHAR2 := NULL, p_ano_sc NUMBER := c_ano_sc) AS
   v_sql_query CLOB;
   v_sql_update CLOB;
   v_cursor tp_ref_cursor;
   
   v_registro_rowid      ROWID;
   v_coluna_geom         VARCHAR2(30);
   v_coluna_cod          VARCHAR2(30);
   v_coluna_ano          VARCHAR2(30);
       
   v_cod          NUMBER;
   v_cod_enriquecimento  NUMBER;
   
   v_update_table VARCHAR2(100);
   
   TYPE rec_sc IS RECORD (
      cod_sc NUMBER,
      ano_sc NUMBER,
      registro_rowid UROWID
   );
   TYPE tb_sc IS TABLE OF rec_sc INDEX BY PLS_INTEGER;
   l_sc tb_sc;
        
BEGIN 
   v_coluna_geom := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'GEOM');
   v_coluna_cod  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'COD_SC');
   v_coluna_ano  := fn_get_coluna_enriquecimento(p_interface_info.cod_interface, 'ANO_SC');
          
   IF p_gt_table IS NULL THEN         
      v_sql_query := '';
      v_sql_query := v_sql_query || 'SELECT a.cod_sc, a.ano_sc, b.rowid ';
      v_sql_query := v_sql_query || 'FROM dados_estat.sd_sc a ';
      v_sql_query := v_sql_query || '   JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo')||' b ON (a.ano_sc = '||p_ano_sc||') ';
      v_sql_query := v_sql_query || '   LEFT JOIN etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'erro')||' c ON (b.cod_fluxo = c.cod_fluxo) ';
      v_sql_query := v_sql_query || 'WHERE c.cod_fluxo IS NULL AND b.'||v_coluna_geom||' is not null ';
      v_sql_query := v_sql_query || 'AND SDO_RELATE(a.geom, b.geom, ''mask=ANYINTERACT'') = ''TRUE''  ';
      v_update_table := 'etl2.'||etl2.pack_etl.fn_get_nome_tabela(p_interface_info.cod_interface, p_interface_info.cod_historico, 'fluxo');
   ELSE
      v_sql_query := '';
      v_sql_query := v_sql_query || 'SELECT ';
      v_sql_query := v_sql_query || '   TOOLS.FN_RECUPERA_COD_SC(a.'||v_coluna_geom||', a.'||v_coluna_ano||', ''ANYINTERACT'') as cod_sc,';
      v_sql_query := v_sql_query || '   a.ano_sc, ';
      v_sql_query := v_sql_query || '   a.rowid as registro_rowid ';
      v_sql_query := v_sql_query || 'FROM '||p_gt_table||' a ';
      v_update_table := p_gt_table;
   END IF;
    
   v_sql_update := '';
   v_sql_update := v_sql_update || 'UPDATE '||v_update_table||' ';
   v_sql_update := v_sql_update || 'SET ';
   v_sql_update := v_sql_update || '   '||v_coluna_cod||' = :1, ';
   v_sql_update := v_sql_update || '   '||v_coluna_ano||' = case when :2 is null then null else :3 end ';
   v_sql_update := v_sql_update || 'WHERE rowid = :4 ';
   
   --DBMS_OUTPUT.PUT_LINE('SQL QUERY (SC): '||v_sql_query);
   DBMS_OUTPUT.PUT_LINE('SQL UPDATE (SC): '||v_sql_update);
          
   OPEN v_cursor FOR v_sql_query;
   LOOP
      FETCH v_cursor
      BULK COLLECT INTO l_sc
      LIMIT 1000;
      
      FORALL v_indx IN 1 .. l_sc.COUNT     
         EXECUTE IMMEDIATE v_sql_update 
         USING 
            l_sc(v_indx).cod_sc, 
            l_sc(v_indx).cod_sc,
            l_sc(v_indx).ano_sc, 
            l_sc(v_indx).registro_rowid;
      IF p_gt_table IS NULL THEN         
         COMMIT;
      END IF;
      EXIT WHEN v_cursor%NOTFOUND;
   END LOOP;
   IF p_gt_table IS NULL THEN         
      COMMIT;
   END IF;
   CLOSE v_cursor;
 END;


END PACK_ETL_AUX;
/
