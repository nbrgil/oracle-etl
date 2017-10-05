CREATE OR REPLACE PACKAGE      PACK_ETL IS 
    
   PRAGMA SERIALLY_REUSABLE; -- Diretiva de compilação (The pragma SERIALLY_REUSABLE indicates that the package state is needed only for the duration of one call to the server)

   e_estrat_insercao_invalida     EXCEPTION;
   e_estrat_atualizacao_invalida  EXCEPTION;
   e_tabela_nao_existe_projeto    EXCEPTION;
   e_tabela_nao_existe_banco      EXCEPTION;
   e_coluna_nao_existe_dicionario EXCEPTION;
   e_coluna_nao_existe_intf       EXCEPTION;
   e_ja_existe_join_tabela        EXCEPTION;
   e_multiplas_tabelas_alvo       EXCEPTION;
   e_param_nao_suport_espera      EXCEPTION;
   e_projeto_inexistente          EXCEPTION;
   e_agendamento                  EXCEPTION;
   e_historico_finalizado         EXCEPTION;
   e_cabecalho_invalido           EXCEPTION; 
   e_arquivo_invalido             EXCEPTION;
   e_enriquecimento_nao_existe    EXCEPTION;
   e_enriquecimento               EXCEPTION;
   e_passo_inexistente            EXCEPTION;
   e_tabelas_log_invalido         EXCEPTION;
   e_erro_execucao                EXCEPTION;
   e_erro_importar_shp_file       EXCEPTION;
   
   c_sqlcode_date_format_picture  CONSTANT NUMBER := 01830;
   c_sqlcode_check_cnstr_violate  CONSTANT NUMBER := 02293;
   c_sqlcode_value_too_large      CONSTANT NUMBER := 12899;
   c_sqlcode_invalid_geometry     CONSTANT NUMBER := 29984;
   c_sqlcode_invalid_shape_file   CONSTANT NUMBER := 29985;
	c_sqlcode_error_import_shp     CONSTANT NUMBER := 20985;
   c_sqlcode_cabecalho_invalido   CONSTANT NUMBER := 20986;
   c_sqlcode_arquivo_invalido     CONSTANT NUMBER := 20987;
   c_sqlcode_agendamento          CONSTANT NUMBER := 20988;
   c_sqlcode_param_nao_sup_esp    CONSTANT NUMBER := 20989;
   c_sqlcode_projeto_inexistente  CONSTANT NUMBER := 20990;
   c_sqlcode_historico_finalizado CONSTANT NUMBER := 20991;
   c_sqlcode_coluna_n_exist_dic   CONSTANT NUMBER := 20992;
   c_sqlcode_tabela_n_exist_bnc   CONSTANT NUMBER := 20993;
   c_sqlcode_ja_existe_join_tab   CONSTANT NUMBER := 20994;
   c_sqlcode_coluna_n_exist_int   CONSTANT NUMBER := 20995;
   c_sqlcode_enriquec_n_exist     CONSTANT NUMBER := 20996;
   c_sqlcode_enriquecimento       CONSTANT NUMBER := 20997;
   c_sqlcode_passo_nao_existe     CONSTANT NUMBER := 20998;
   c_sqlcode_interface_invalida   CONSTANT NUMBER := 20999;
   c_sqlcode_acesso_dir_invalido  CONSTANT NUMBER := 29289;   
	

  
   c_tp_msg_pk                   CONSTANT VARCHAR2(100) := 'CHAVE_PRIMARIA_NAO_EXCLUSIVA'; 
   c_tp_msg_fk                   CONSTANT VARCHAR2(100) := 'VALOR_INVALIDO_CHAVE_ESTRANGEIRA'; 
   c_tp_msg_nn                   CONSTANT VARCHAR2(100) := 'VALOR_OBRIGATORIO_NAO_PREENCHIDO'; 
   c_tp_msg_ck                   CONSTANT VARCHAR2(100) := 'RESTRICAO_CHECAGEM_VIOLADA'; 
   c_tp_msg_un                   CONSTANT VARCHAR2(100) := 'CHAVE_UNICA_NAO_EXCLUSIVA'; 
   c_tp_msg_dado_incorreto       CONSTANT VARCHAR2(100) := 'TIPO_DADO_INCORRETO';
   c_tp_msg_ora_value_too_large  CONSTANT VARCHAR2(100) := 'VALOR_MUITO_GRANDE';
   c_tp_msg_date_format_picture  CONSTANT VARCHAR2(100) := 'FORMATO_DATA_INVALIDO';
   c_tp_msg_unexpected_error     CONSTANT VARCHAR2(100) := 'ERRO_INESPERADO';
   c_tp_msg_invalid_header       CONSTANT VARCHAR2(100) := 'CABECALHO_INVALIDO';
   c_tp_msg_scheduled            CONSTANT VARCHAR2(100) := 'AGENDAMENTO';
   c_tp_msg_shp                  CONSTANT VARCHAR2(100) := 'SHAPEFILE_INVALIDO';
   
   TYPE rec_projeto_info IS RECORD (
      cod_projeto          etl2.projeto.cod_projeto%TYPE,
      data_inicio          DATE,
      data_atual           DATE,
      nome_arquivo_entrada etl2.historico.nome_arquivo%TYPE,
      characterset         VARCHAR2(400),
      cod_historico etl2.historico.cod_historico%TYPE,
      cod_interface etl2.interface.cod_interface%TYPE
   );
     
   TYPE rec_interface_info IS RECORD (
      cod_interface etl2.interface.cod_interface%TYPE,
      cod_historico etl2.historico.cod_historico%TYPE
   );
   
   /**
   * Calcula o nome das tabelas temporárias criadas
   * @param p_cod_interface  Código da interface
   * @param p_cod_historico  Identificador de execução do histórico (etl.historico)
   * @param p_tipo           Tipo da tabela. Valores: 'erro','erro_geral','fluxo', 'fluxo_erro', 'origem_erro', 'origem', 'externa', 'resumo', 'externa_materializada', 'backup'
   * @return Nome da tabela (sem o esquema)
   */
   FUNCTION FN_GET_NOME_TABELA(
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_cod_historico etl2.historico.cod_historico%TYPE,
      p_tipo          VARCHAR2
   ) RETURN VARCHAR2;
   
   PROCEDURE SP_ADD_PROJETO (
      p_nome_projeto       etl2.projeto.nome%TYPE, 
      p_descricao          etl2.projeto.descricao%TYPE,
      p_sigla_empresa      admin.empresa.sigla%TYPE,
      p_qtde_linhas_maximo etl2.projeto.qtde_linhas_maximo%TYPE,
      p_diretorio          etl2.projeto.diretorio%TYPE,
      p_sql_reverso        etl2.projeto.sql_reverso%TYPE
   );
   
   /** 
   * Adiciona a configuração de um projeto
   * @param p_nome_projeto   Nome do projeto
   * @param p_sigla_empresa  Sigla da empresa no módulo administrativo (admin.empresa)
   * @return PK do projeto criado
   */
   FUNCTION FN_ADD_PROJETO(
      p_nome_projeto       etl2.projeto.nome%TYPE, 
      p_sigla_empresa      admin.empresa.sigla%TYPE,
      p_qtde_linhas_maximo etl2.projeto.qtde_linhas_maximo%TYPE,
      p_diretorio          etl2.projeto.diretorio%TYPE
   ) RETURN etl2.projeto.cod_projeto%TYPE;
   
   PROCEDURE SP_EXCLUIR_TABELAS_INTERNAS (
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_cod_historico etl2.historico.cod_historico%TYPE
   );   
   
   -- Objetivo: Adicionar uma tabela do dicionário e suas colunas nos metadados do ETL.
   -- Parâmetros:
   --    p_esquema_tabela => Esquema (owner) da tabela;
   --    p_nome_tabela    => Nome da tabela.
   PROCEDURE SP_ADD_TABELA(
      p_esquema_tabela etl2.tabela.esquema%TYPE, 
      p_nome_tabela    etl2.tabela.nome%TYPE
   );
   
   -- Objetivo: Adicionar uma tabela externa nos metadados do ETL.
   -- Parâmetros:
   --    p_esquema_tabela   => Esquema (owner) da tabela;
   --    p_nome_tabela      => Nome da tabela;
   --    p_diretorio_tabela => Diretório em que o arquivo se encontra;
   --    p_lista_colunas    => Lista de colunas, separadas por vírgula, representando o cabeçalho desse arquivo.
   -- Retorno: Chave da tabela criada.
   FUNCTION FN_ADD_TABELA_EXTERNA(
      p_esquema_tabela   etl2.tabela.esquema%TYPE, 
      p_nome_tabela      etl2.tabela.nome%TYPE, 
      p_lista_colunas    CLOB,
      p_formato_arquivo  VARCHAR2 := 'CSV',
      p_limitador_coluna VARCHAR2 := ';'
   ) RETURN etl2.tabela.cod_tabela%TYPE;
   
   PROCEDURE SP_ADD_TABELA_EXTERNA (
      p_esquema_tabela   etl2.tabela.esquema%TYPE, 
      p_nome_tabela      etl2.tabela.nome%TYPE, 
      p_lista_colunas    CLOB
   );
   
   
   PROCEDURE SP_ADD_COLUNA_CALCULADA (
      p_cod_interface    etl2.interface.cod_interface%TYPE,
      p_esquema_tabela   etl2.tabela.esquema%TYPE, 
      p_nome_tabela      etl2.tabela.nome%TYPE, 
      p_coluna_original  etl2.coluna.nome%TYPE,
      p_alias_coluna     etl2.interface_tabela_coluna.alias_coluna%TYPE,
      p_transformacao    etl2.transformacao_texto.nome%TYPE,
      p_lista_parametros CLOB := NULL
   );
   
   -- Objetivo: Adicionar uma interface nos metadados do ETL.
   -- Parâmetros:
   --    p_nome_interface      => Nome (único) da interface;
   --    p_esquema_tabela_alvo => Esquema (owner) da tabela alvo;
   --    p_nome_tabela_alvo    => Nome da tabela alvo;
   --    p_commit              => Indica por S/N se deve efetuar o commit ao final o processamento;
   --    p_delete_all          => Indica por S/N se deve excluir (DELETE) todos os registros antes de começar a processar;
   --    p_truncate            => Indica por S/N se deve excluir (TRUNCATE) todos os registros antes de começar a processar;
   --    p_distinct            => Indica por S/N se deve inserir todas as linhas do arquivo de entrada (N) ou se somente os valores mapeados distintos (S);
   --    p_estrat_deteccao     => Estratégia de detecção (de registros existentes). Pode ser M para usar o Minus,  E para usar o Exists e N para nenhuma estratégia;
   --    p_estrat_atualizacao  => Estratégia de atualização dos registros. Pode ser M para MERGE (INSERT + UPDATE), U para somente UPDATE e I para somente INSERT;
   --    p_cod_nivel_log       => Nível de log padrão. Será nesse nível que o cliente receberá o email.
   -- Retorno: Chave da interface criada.
   FUNCTION FN_ADD_INTERFACE(
      p_nome_interface      etl2.interface.nome%TYPE,
      p_esquema_tabela_alvo etl2.tabela.esquema%TYPE,
      p_nome_tabela_alvo    etl2.tabela.nome%TYPE,
      p_commit              etl2.interface.confirmar_alteracoes%TYPE,
      p_delete_all          etl2.interface_importacao.excluir_tudo_dml%TYPE,
      p_truncate            etl2.interface_importacao.excluir_tudo_ddl%TYPE,
      p_distinct            etl2.interface_importacao.somente_distintos%TYPE,
      p_estrat_deteccao     etl2.interface_importacao.estrategia_deteccao%TYPE,
      p_estrat_atualizacao  etl2.interface_importacao.estrategia_atualizacao%TYPE,
      p_cod_nivel_log       etl2.interface.cod_nivel_log%TYPE
   ) RETURN etl2.interface.cod_interface%TYPE;
   
   PROCEDURE SP_ADD_INTERFACE (
      p_nome_interface      etl2.interface.nome%TYPE, 
      p_esquema_tabela_alvo etl2.tabela.esquema%TYPE, 
      p_nome_tabela_alvo    etl2.tabela.nome%TYPE, 
      p_commit              etl2.interface.confirmar_alteracoes%TYPE, 
      p_delete_all          etl2.interface_importacao.excluir_tudo_dml%TYPE,
      p_truncate            etl2.interface_importacao.excluir_tudo_ddl%TYPE, 
      p_distinct            etl2.interface_importacao.somente_distintos%TYPE,  
      p_estrat_deteccao     etl2.interface_importacao.estrategia_deteccao%TYPE,
      p_estrat_atualizacao  etl2.interface_importacao.estrategia_atualizacao%TYPE,
      p_cod_nivel_log       etl2.interface.cod_nivel_log%TYPE
   );
   
   PROCEDURE SP_ADD_INTERFACE_PROCESSAMENTO (
      p_nome_interface      etl2.interface.nome%TYPE, 
      p_commit              etl2.interface.confirmar_alteracoes%TYPE, 
      p_esquema             etl2.interface_processamento.esquema%TYPE,
      p_nome_procedimento   etl2.interface_processamento.nome_procedimento%TYPE,
      p_cod_nivel_log       etl2.interface.cod_nivel_log%TYPE
   );
   
   
   -- Objetivo: Inserir/Atualizar a tabela de textos para transformações.
   -- Parâmetros: 
   --    p_nome  => Nome da transformação. Único e será usado como referência nas criações de transformações para cada interface;
   --    p_texto => Texto da transformação. Pode conter variáveis representando colunas ou outros parâmetros
   -- Exemplos:
   --    1) Deixando a coluna com UPPER => UPPER(#COLUNA1)
   --    2) Sequência => #PARAM1.NEXTVAL
   -- Retorno: Código da transformação criada.
   FUNCTION FN_ADD_TRANSFORMACAO_TEXTO(
      p_nome  etl2.transformacao_texto.nome%TYPE,
      p_texto etl2.transformacao_texto.texto%TYPE
   ) RETURN etl2.transformacao_texto.cod_transformacao_texto%TYPE;
   
   -- Objetivo: Idêntico a função FN_ADD_TRANSFORMACAO_TEXTO, mas sem retorno
   PROCEDURE SP_ADD_TRANSFORMACAO_TEXTO(
      p_nome  etl2.transformacao_texto.nome%TYPE,
      p_texto etl2.transformacao_texto.texto%TYPE
   );
   
   -- Objetivo: Adicionar relacionamento entre interface e tabela
   -- Parâmetros: 
   --    p_cod_interface      Código da interface
   --    p_esquema            Esquema (owner) da tabela
   --    p_tabela             Nome da tabela (table_name)
   --    p_apelido_tabela     Apelido da tabela (usado para referenciar tabela na cláusula FROM)
   --    p_posicao            Posição (usado para definir ordem na cláusula FROM)
   --    p_tipo_tabela        Tipo da tabela. Pode ser 'F' para fonte ou 'A' para alvo
   --    p_join_transformacao Transformação usada para efetuar o join entre tabelas (usado na cláusula ON do JOIN)
   --    p_lista_colunas      Lista de colunas da transformação informada
   -- Retorno: Código da interface tabela criada
   FUNCTION FN_ADD_INTERFACE_TABELA(
      p_cod_interface      etl2.interface.cod_interface%TYPE,
      p_esquema            etl2.tabela.esquema%TYPE, 
      p_tabela             etl2.tabela.nome%TYPE,
      p_apelido_tabela     etl2.interface_tabela.alias_tabela%TYPE,
      p_posicao            etl2.interface_tabela.posicao%TYPE,      
      p_tipo_tabela        etl2.interface_tabela.tipo_tabela%TYPE,
		p_prefixo_join       etl2.interface_tabela.prefixo_join%TYPE := 'LEFT'
   ) RETURN etl2.interface_tabela.cod_interface_tabela%TYPE;
   
   -- Objetivo: Idêntico a função FN_ADD_INTERFACE_TABELA, mas sem retorno
   PROCEDURE SP_ADD_INTERFACE_TABELA(
      p_cod_interface      etl2.interface.cod_interface%TYPE,
      p_esquema            etl2.tabela.esquema%TYPE, 
      p_tabela             etl2.tabela.nome%TYPE,
      p_apelido_tabela     etl2.interface_tabela.alias_tabela%TYPE,
      p_posicao            etl2.interface_tabela.posicao%TYPE,      
      p_tipo_tabela        etl2.interface_tabela.tipo_tabela%TYPE,
		p_prefixo_join       etl2.interface_tabela.prefixo_join%TYPE := 'LEFT'
   );
   
   -- Objetivo: Relaciona tabelas (cláusula ON do JOIN) criando uma transformação
   -- Parâmetro: 
   --    p_cod_interface    => Código da interface
   --    p_tabela           => Código da tabela na cláusula FROM 
   --    p_transformacao    => Transformação que realiza o JOIN
   --    p_lista_colunas    => Lista de colunas separadas por vírgula no seguinte padrão [ALIAS_TABELA].[NOME_TABELA];
   --    p_lista_parametros => Lista de parâmetros separados por vírgula
   --    p_excluir_antigo   => Define se deve excluir joins antigos nessa tabela
   PROCEDURE SP_ADD_JOIN(
      p_cod_interface    etl2.interface.cod_interface%TYPE,
      p_tabela           etl2.tabela.nome%TYPE,
      p_transformacao    etl2.transformacao_texto.nome%TYPE,
      p_lista_colunas    VARCHAR2 := NULL,
      p_lista_parametros CLOB := NULL,
      p_excluir_antigo   CHAR := 'N'
   );
   
   
   
   PROCEDURE SP_ADD_MAPEAMENTO(
      p_cod_interface       etl2.interface.cod_interface%TYPE,
      p_coluna_alvo         etl2.interface_tabela_coluna.alias_coluna%TYPE,
      p_transformacao       etl2.transformacao_texto.nome%TYPE,
      p_esquema_execucao    etl2.mapeamento.esquema_execucao%TYPE,
      p_posicao             etl2.mapeamento.posicao%TYPE,
      p_chave_atualizacao   etl2.mapeamento.chave_atualizacao%TYPE,
      p_insert              etl2.mapeamento.participa_insercao%TYPE,
      p_update              etl2.mapeamento.participa_atualizacao%TYPE,
      p_lista_colunas       CLOB := NULL,
      p_lista_parametros    CLOB := NULL
   );
   
   -- Adiciona uma transformação e um mapeamento
   -- Parâmetros: 
   --    p_cod_interface       Pk da interface
   --    p_tabela_alvo         Nome da tabela alvo
   --    p_coluna_alvo         Nome da coluna alvo
   --    p_transformacao       Nome da transformação
   --    p_esquema_execucao    Esquema de execução (T = Trabalho / A = Alvo)
   --    p_posicao             Posição/Ordem do mapeamento
   --    p_chave_atualizacao   Coluna alvo é chave de atualização? (S/N)
   --    p_insert              Participa da inserção? (S/N)
   --    p_update              Participa da atualização? (S/N)
   --    p_lista_colunas       Lista de colunas (bind #COLUNAX) do mapeamento separadas por vírgula
   --    p_lista_parametros    Lista de parametros (substituição #PARAMX) do mapeamento separadas por vírgula
   -- Retorno: Código do mapeamento criado
   FUNCTION FN_ADD_MAPEAMENTO(
      p_cod_interface       etl2.interface.cod_interface%TYPE,
      p_coluna_alvo         etl2.coluna.nome%TYPE,
      p_transformacao       etl2.transformacao_texto.nome%TYPE,
      p_esquema_execucao    etl2.mapeamento.esquema_execucao%TYPE,
      p_posicao             etl2.mapeamento.posicao%TYPE,
      p_chave_atualizacao   etl2.mapeamento.chave_atualizacao%TYPE,
      p_insert              etl2.mapeamento.participa_insercao%TYPE,
      p_update              etl2.mapeamento.participa_atualizacao%TYPE,
      p_lista_colunas       CLOB := NULL,
      p_lista_parametros    CLOB := NULL
   ) RETURN etl2.mapeamento.cod_mapeamento%TYPE;
   
   -- Remover um mapeamento 
   PROCEDURE SP_DROP_MAPEAMENTO(
      p_cod_interface etl2.interface.cod_interface%TYPE, 
      p_coluna_alvo   etl2.coluna.nome%TYPE
   );
   PROCEDURE SP_ADD_TRANSFORMACAO_PARAMETRO(
      p_cod_transformacao etl2.transformacao.cod_transformacao%TYPE,
      p_texto_parametro   etl2.transformacao_parametro.texto_parametro%TYPE,
      p_posicao           etl2.transformacao_coluna.posicao%TYPE
   );
   
   /** -------------------------------------------------------------------------
      Objetivo: validar o arquivo da tabela externa
      Parâmetros:
         p_cod_interface  => Código da interface que possui a tabela externa
         p_diretorio      => Diretório Oracle onde está o arquivo
         p_nome_arquivo   => Nome do arquivo analisado
   ---------------------------------------------------------------------------*/
   PROCEDURE SP_VALIDAR_ARQUIVO (
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_diretorio      etl2.projeto.diretorio%TYPE,
      p_nome_arquivo   VARCHAR2
   );
   
   PROCEDURE SP_VALIDAR_AGENDAMENTO (
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_cod_historico etl2.historico.cod_historico%TYPE
   );
   
   FUNCTION FN_ADD_PROJETO_PASSO (
      p_cod_projeto    etl2.projeto.cod_projeto%TYPE,
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_nome           etl2.passo.nome%TYPE,
      p_primeiro_passo etl2.passo.primeiro_passo%TYPE
   ) RETURN etl2.passo.cod_passo%TYPE;
   
   PROCEDURE SP_SET_CICLO_PROJETO_PASSO (
      p_cod_passo    etl2.passo.cod_passo%TYPE,
      p_cod_passo_ok etl2.passo.cod_passo%TYPE,
      p_cod_passo_ko etl2.passo.cod_passo%TYPE
   );
   
   PROCEDURE SP_DELETE_PROJETO_PASSO (
      p_cod_projeto    etl2.projeto.cod_projeto%TYPE,
      p_tipo_execucao  CHAR := 'A'
   );
   
   PROCEDURE SP_SET_PROJETO_PASSO (
      p_cod_projeto    etl2.projeto.cod_projeto%TYPE,
      p_lista_passos   VARCHAR2,
      p_tipo_execucao  CHAR := 'A'
   );
   
   PROCEDURE SP_GERAR_INTERFACE_IMPORTACAO(
      p_cod_historico     etl2.historico.cod_historico%TYPE,
      p_cod_interface     etl2.interface.cod_interface%TYPE,
      p_diretorio         etl2.projeto.diretorio%TYPE,
      p_nome_arquivo      VARCHAR2,
      p_gerar_log         CHAR := 'S',
      p_remover_tabelas_$ CHAR := 'S',
      p_validar_agendamento CHAR := 'S'
   );
   
   PROCEDURE SP_GERAR_INTERFACE_PROCESS(
      p_cod_historico  etl2.historico.cod_historico%TYPE,
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_gerar_log      CHAR := 'S',
      p_diretorio           etl2.projeto.diretorio%TYPE,
      p_nome_arquivo        VARCHAR2,
      p_remover_tabelas_$   CHAR := 'S'
   );
   
   PROCEDURE SP_EXCLUIR_PROJETO (
      p_cod_projeto etl2.projeto.cod_projeto%TYPE
   );
   
   PROCEDURE SP_EXPORTAR_PROJETO (
      p_cod_projeto etl2.projeto.cod_projeto%TYPE
   );
   
   PROCEDURE SP_EXCLUIR_INTERFACE (
      p_cod_interface etl2.interface.cod_interface%TYPE
   );
   
   PROCEDURE SP_EXCLUIR_INTERFACE (
      p_nome_interface etl2.interface.nome%TYPE
   );  
   
   PROCEDURE SP_EXCLUIR_TABELA_DICIONARIO (
      p_esquema etl2.tabela.esquema%TYPE,
      p_nome    etl2.tabela.nome%TYPE
   );
   
   PROCEDURE SP_ADD_INTF_ENRIQUECIMENTO (
      p_interface            etl2.interface.nome%TYPE,
      p_nome_enriquecimento  etl2.enriquecimento.nome%TYPE,
      p_nome                 etl2.interface_enriquecimento.nome%TYPE,
      p_ordem                etl2.interface_enriquecimento.ordem%TYPE,      
      p_lista_coluna_relacao CLOB := NULL, -- formato 'COLUNAENRIQUECIMENTO=COLUNATABELAALVO,COLUNA2=COLUNA2'
		p_qtde_reg_map			  VARCHAR2 := NULL
	);
   
   PROCEDURE SP_DROP_INTF_ENRIQUECIMENTO (
      p_interface etl2.interface.nome%TYPE,
      p_ordem     etl2.interface_enriquecimento.ordem%TYPE := NULL
   );
    
   PROCEDURE SP_EXECUTAR_PROJETO (
      p_cod_projeto         etl2.projeto.cod_projeto%TYPE,
      p_cod_usuario_perfil  etl2.historico.cod_usuario_perfil%TYPE,
      p_nome_arquivo        etl2.historico.nome_arquivo%TYPE,
      p_cod_passo_inicial   etl2.passo.cod_passo%TYPE := NULL,
      p_cod_passo_final     etl2.passo.cod_passo%TYPE := NULL,
      p_saida_dbms          CHAR := 'N',
      p_characterset        VARCHAR2 := NULL,
      p_cod_historico       etl2.historico.cod_historico%TYPE := NULL,
      p_enviar_email        CHAR := 'S',
      p_email_adicional     VARCHAR2 := NULL,
      p_enviar_anexos       CHAR := 'S',
      p_remover_tabelas_$   CHAR := 'S'
   );
   
   PROCEDURE SP_CHECAR_PROJETO (
      p_cod_projeto etl2.projeto.cod_projeto%TYPE
   );
   
   PROCEDURE SP_EXECUTAR_PASSOS (
      p_cod_projeto         etl2.projeto.cod_projeto%TYPE,
      p_cod_usuario_perfil  etl2.historico.cod_usuario_perfil%TYPE,
      p_nome_arquivo        etl2.historico.nome_arquivo%TYPE,
      p_cod_passo_inicial   etl2.passo.cod_passo%TYPE := NULL,
      p_cod_passo_final     etl2.passo.cod_passo%TYPE := NULL,
      p_saida_dbms          CHAR := 'N',
      p_cod_historico       etl2.historico.cod_historico%TYPE := NULL,
      p_enviar_email        CHAR := 'S',
      p_email_adicional     VARCHAR2 := NULL,
      p_enviar_anexos       CHAR := 'S',
      p_tipo_execucao       CHAR := 'A',
      p_characterset        VARCHAR2 := NULL,
      p_remover_tabelas_$   CHAR := 'S'
   );
   
   PROCEDURE SP_ENRIQUECER_INTERFACE (
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_cod_historico  etl2.historico.cod_historico%TYPE
   );
   
   PROCEDURE SP_CHECAR_TIPO_INVALIDO (
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_cod_historico  etl2.historico.cod_historico%TYPE
   );
   
   PROCEDURE SP_GERAR_LOG (
      p_cod_interface    etl2.interface.cod_interface%TYPE,
      p_cod_historico    etl2.historico.cod_historico%TYPE,
      p_mensagem         VARCHAR2 := NULL,
      p_tipo_mensagem    etl2.log_tipo_mensagem.nome%TYPE := NULL,
      p_cod_nivel_filtro etl2.log_nivel.cod_nivel%TYPE := NULL
   );
   
   
   PROCEDURE SP_ENVIAR_EMAIL (
      p_cod_historico    etl2.historico.cod_historico%TYPE,
      p_enviar_anexos    CHAR := 'S',
      p_anexo_adicional  VARCHAR2 := NULL
   );
   
  PROCEDURE SP_EXECUTAR_INTERFACE_PROCESS(
      p_cod_historico  etl2.historico.cod_historico%TYPE,
      p_cod_interface  etl2.interface.cod_interface%TYPE
   );
   
   PROCEDURE SP_EXCLUIR_DADOS_PROJETO(
      p_cod_projeto         etl2.projeto.cod_projeto%TYPE,
      p_cod_usuario_perfil  etl2.historico.cod_usuario_perfil%TYPE,
      p_cod_passo_inicial   etl2.passo.cod_passo%TYPE := NULL,
      p_cod_passo_final     etl2.passo.cod_passo%TYPE := NULL,
      p_saida_dbms          CHAR := 'N',
      p_remover_tabelas_$   CHAR := 'S'
   );
   
   PROCEDURE SP_EXECUTAR_AGENDA_EMPRESA(
      p_cod_empresa admin.empresa.cod_empresa%TYPE
   );
   
   PROCEDURE SP_CRIAR_JOB_AGENDAMENTO;
   
END PACK_ETL;
/


CREATE OR REPLACE PACKAGE BODY      PACK_ETL IS
   
   PRAGMA SERIALLY_REUSABLE; -- Diretiva de compilação (The pragma SERIALLY_REUSABLE indicates that the package state is needed only for the duration of one call to the server)
   
   -- Variáveis de pacote
   vp_ddl_linha_atual NUMBER;         -- Número da linha atual do script. Veja documentação da procedure "SP_ADD_INTERFACE_DDL";
   vp_ddl_bloco_atual NUMBER;         -- Número do bloco atual do script. Veja documentação da procedure "SP_ADD_INTERFACE_DDL";
   vp_cod_interface   NUMBER;         -- Código da interface. É definido somente no método que gera o script da interface e é utilizado em quase todos os outros métodos;
   vp_cod_historico   NUMBER;         -- Código do histórico. É definido somente no método de execução do projeto e é usando em quase todos os outros métodos;
   vp_characterset    VARCHAR2(400);  -- Definição de charset para tabelas externas.
   vp_variavel_erro   VARCHAR2(4000); -- Guarda lista de variáveis usando para a geração da mensagem de erro.

   
   -- Definição de tipos
   TYPE t_ref_cursor IS REF CURSOR; 
    
   -- Constantes que definem o esquema de execução de um mapeamento
   c_esquema_execucao_area_trab CONSTANT CHAR(1) := 'T';
   c_esquema_execucao_alvo      CONSTANT CHAR(1) := 'A';
   
   -- Constantes que definem o tipo de tabela
   c_tipo_tabela_alvo  CONSTANT VARCHAR2(1) := 'A';
   c_tipo_tabela_fonte CONSTANT VARCHAR2(1) := 'F';
   
   -- Constantes de status de execução de um projeto
   c_st_execucao   CONSTANT VARCHAR2(12) := 'EX';
   c_st_espera     CONSTANT VARCHAR2(12) := 'ES';
   c_st_finalizado CONSTANT VARCHAR2(12) := 'FI';
   c_st_erro       CONSTANT VARCHAR2(12) := 'ER';
   c_st_agendado   CONSTANT VARCHAR2(12) := 'AG';
   
   -- Tipo de interface
   c_interface_importacao   CONSTANT CHAR(1) := 'I';
   c_interface_procedimento CONSTANT CHAR(1) := 'P';
   
   c_tipo_exec_atualizacao CONSTANT CHAR(1) := 'A';
   c_tipo_exec_exclusao    CONSTANT CHAR(1) := 'E';
   
   c_max_precisao_numerica CONSTANT NUMBER := 38;

   c_estrat_det_minus   CONSTANT CHAR(1) := 'M';
   c_estrat_det_exists  CONSTANT CHAR(1) := 'E';
   c_estrat_det_nenhuma CONSTANT CHAR(1) := 'N';
   
   c_formato_arquivo_csv CONSTANT VARCHAR2(12) := 'CSV';
   c_formato_arquivo_shp CONSTANT VARCHAR2(12) := 'SHP';
   
   c_estrat_atulz_merge  CONSTANT CHAR(1) := 'M';
   c_estrat_atulz_insert CONSTANT CHAR(1) := 'I';
   c_estrat_atulz_update CONSTANT CHAR(1) := 'U';
   
   -- Constantes de mensagens de erro
   c_tabela_nao_existe               CONSTANT VARCHAR2(100) := 'Tabela #TABELA não existe no banco ou usuário ETL2 não tem acesso';
   c_estrategia_insercao_invalida    CONSTANT VARCHAR2(100) := 'Estratégia de atualização inválida. Nenhum mapeamento marcado para inserção';
   c_estrategia_atualiz_invalida     CONSTANT VARCHAR2(100) := 'Estratégia de atualização inválida. Nenhum mapeamento marcado para atualização';
   c_coluna_nao_existe               CONSTANT VARCHAR2(100) := 'Coluna #COLUNA não existe no dicionário';
   c_ja_existe_join                  CONSTANT VARCHAR2(100) := 'Tabela "#TABELA" já possui JOIN. Use o parâmetro correto para excluir o mesmo, se necessário'; 
   c_param_nao_suport_espera         CONSTANT VARCHAR2(100) := 'Parâmetros opcionais (Filtro de passos e sáida dbms) não são suportados para um projeto em execução'; 
   c_projeto_inexistente             CONSTANT VARCHAR2(100) := 'Projeto inexistente'; 
   c_historico_finalizado            CONSTANT VARCHAR2(100) := 'O histórico informado já foi finalizado.'; 
   c_cabecalho_invalido              CONSTANT VARCHAR2(100) := 'Coluna número #NUMERO do cabeçalho está  inválida: Informado: #INFORMADO, Esperado: #ESPERADO'; 
   c_qtde_cabecalho_incorreta        CONSTANT VARCHAR2(4000) := 'Quantidade de colunas inválida: Informado: #INFORMADO, Esperado: #ESPERADO. Em anexo um arquivo modelo.';
   c_arquivo_invalido                CONSTANT VARCHAR2(100) := 'Arquivo inválido.'; 
   c_agendamento                     CONSTANT VARCHAR2(100) := 'Processo agendado. Tamanho do arquivo de entrada é muito grande.';
   c_coluna_nao_existe_intf          CONSTANT VARCHAR2(100) := 'Coluna #COLUNA não existe nessa interface';
   c_enriquecimento_nao_existe       CONSTANT VARCHAR2(100) := 'Enriquecimento "#ENRIQ" não existe';
   c_enriquecimento                  CONSTANT VARCHAR2(100) := 'Enriquecimento "#ENRIQ" com erros (#ERRO)';
   c_passo_atualizacao_nao_existe    CONSTANT VARCHAR2(100) := 'Passo inicial para atualização desse projeto não existe';
   c_passo_exclusao_nao_existe       CONSTANT VARCHAR2(100) := 'Passo inicial para exclusão desse projeto não existe';
   c_sequencia_passo_invalida        CONSTANT VARCHAR2(100) := 'Sequência de passos incorreta. Verifique se os passos são do mesmo tipo de execução.';
   c_interface_invalida              CONSTANT VARCHAR2(100) := 'Interface inválida.';
   c_historico_nao_existe_log        CONSTANT VARCHAR2(100) := 'Tabelas de histórico para essa interface não existem. Log não foi gerado';
   c_mail_foot_note                  CONSTANT VARCHAR2(150) := 'Obs: Para mais detalhes sobre a importação (listagem e explicação dos erros), veja os arquivos em anexo.';
   c_attachment_foot_note            CONSTANT VARCHAR2(150) := 'Obs: O arquivo em anexo não foi enviado por problemas de configuração nesse importador. Entre em contato com o suporte.';
   c_invalid_shape_file              CONSTANT VARCHAR2(150) := 'Não foi possível importar o arquivo .SHP .';
   c_invalid_geometry_shp            CONSTANT VARCHAR2(150) := 'Geometrias inválidas encontradas no arquivo .SHP .';
   c_error_import_shp                CONSTANT VARCHAR2(150) := 'Ocorreu um erro ao importar .SHP';
   
   c_registro_agendamento_valido     CONSTANT VARCHAR2(400) := 'Agendamento validado. Executando...';
   c_registro_inicio_execucao        CONSTANT VARCHAR2(400) := 'Início da execução do projeto';
   c_registro_inicio_passo           CONSTANT VARCHAR2(400) := 'Início da execução do passo';
   c_registro_ja_existe_exec         CONSTANT VARCHAR2(400) := 'Já existe outra execução desse projeto em andamento. A importação atual está em espera...';
   
-- Erros de log
   c_source_file_error               CONSTANT VARCHAR2(150) := 'Não foi possível ler o arquivo de entrada. Entre em contato com o suporte.';-- Qualquer erro contendo ODCIEXTTABLEOPEN
   c_unexpected_error                CONSTANT VARCHAR2(150) := 'Erro de execução. Entre em contato com o suporte.'; -- Erro não tratado. Nesse caso, melhorar SP_GERAR_LOG
   
   procedure SP_REGISTRAR_EXECUCAO (
      p_cod_historico etl2.historico.cod_historico%TYPE,
      p_mensagem      VARCHAR2,
      p_status        VARCHAR2
   ) as
      PRAGMA AUTONOMOUS_TRANSACTION;
      
      v_nome_arquivo_nao_carreg etl2.historico_execucao.nome_arquivo_nao_carreg%TYPE;
      v_nome_arquivo_log        etl2.historico_execucao.nome_arquivo_log%TYPE;
      v_log_execucao            VARCHAR2(4000);
      
   begin
      v_log_execucao := SUBSTR(TO_CHAR(SYSDATE, 'DD/MM/YYYY HH24:MI:SS')||' => '||p_mensagem || chr(13), 1, 4000);
      
      UPDATE etl2.historico
      SET status          = p_status,
          log_execucao    = log_execucao || v_log_execucao,
          mensagem_status = v_log_execucao
      WHERE cod_historico = p_cod_historico;
      
      IF p_status = c_st_finalizado or p_status = c_st_erro THEN
         UPDATE etl2.historico
         SET data_termino = SYSDATE
         WHERE cod_historico = p_cod_historico;
      END IF;
      

      COMMIT;
   end;
   
   /**
   * Calcula o nome das tabelas temporárias criadas
   * @param p_cod_interface  Código da interface
   * @param p_cod_historico  Identificador de execução do histórico (etl.historico)
   * @param p_tipo           Tipo da tabela. Valores: 'erro','erro_geral','fluxo', 'fluxo_erro', 'origem_erro', 'origem', 'externa', 'resumo', 'externa_materializada', 'backup'
   * @return Nome da tabela (sem o esquema)
   */
   FUNCTION FN_GET_NOME_TABELA(
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_cod_historico etl2.historico.cod_historico%TYPE,
      p_tipo          VARCHAR2
   ) RETURN VARCHAR2 AS
      v_historico VARCHAR2(5);
   BEGIN
      v_historico := lpad(p_cod_historico, 5, '0');
   
      RETURN 
         CASE p_tipo 
            WHEN 'erro'                  THEN 'etl_e$_'|| p_cod_interface|| '_' ||v_historico
            --WHEN 'erro_geral'            THEN 'etl_eg$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'fluxo'                 THEN 'etl_i$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'fluxo_erro'            THEN 'err$_etl_i$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'origem_erro'           THEN 'err$_etl_s$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'origem'                THEN 'etl_s$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'externa'               THEN 'etl_c$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'resumo'                THEN 'etl_ck$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'externa_materializada' THEN 'etl_m$_'|| p_cod_interface|| '_' ||v_historico
            --WHEN 'externa_materializada' THEN 'etl_m$'
            WHEN 'backup'                THEN 'etl_b$_'|| p_cod_interface|| '_' ||v_historico
            WHEN 'exception'             THEN 'etl_exc$_'|| p_cod_interface|| '_' ||v_historico
         END;
   END FN_GET_NOME_TABELA;
   
   FUNCTION FN_GET_NOME_ARQUIVO (
      p_cod_historico      etl2.historico.cod_historico%TYPE,
      p_tipo               VARCHAR2,
      p_data_gerar_arquivo DATE,
      p_cod_nivel          etl2.log_nivel.cod_nivel%TYPE := NULL
   ) RETURN VARCHAR2 AS
      v_nome_arquivo etl2.historico.nome_arquivo%TYPE;
      v_nome_arquivo_gerado VARCHAR2(4000);
   BEGIN
      SELECT nome_arquivo_sem_ext 
      INTO v_nome_arquivo
      FROM etl2.vw_historico_execucao
      WHERE cod_historico = p_cod_historico;
      
      CASE p_tipo
         WHEN 'logusuario' THEN 
            v_nome_arquivo_gerado := v_nome_arquivo||'_'||TO_CHAR(p_data_gerar_arquivo, 'YYYYMMDDHH24MISS')||'-'||p_cod_historico||'_logusuario.html';
            
            UPDATE etl2.historico_execucao
            SET nome_arquivo_log = v_nome_arquivo_gerado
            WHERE cod_historico = p_cod_historico;
            
         WHEN 'log' THEN 
            v_nome_arquivo_gerado := v_nome_arquivo||'_'||TO_CHAR(p_data_gerar_arquivo, 'YYYYMMDDHH24MISS')||'-'||p_cod_historico||'_lognvl'||p_cod_nivel||'.html';            
            
         WHEN 'nc' THEN 
            v_nome_arquivo_gerado := v_nome_arquivo||'_'||TO_CHAR(p_data_gerar_arquivo, 'YYYYMMDDHH24MISS')||'-'||p_cod_historico||'_naocarregados.csv';
            
            UPDATE etl2.historico_execucao
            SET nome_arquivo_nao_carreg = v_nome_arquivo_gerado
            WHERE cod_historico = p_cod_historico;
            
      END CASE;
      
      RETURN v_nome_arquivo_gerado;
      
   END FN_GET_NOME_ARQUIVO;   
   
   -- Objetivo: Inserir registros na tabela temporária etl2.interface_ddl, que servirá como base para 
   --           a execução da interface ou para exportar o script gerado;
   -- Parâmetros: 
   --    p_cmd_sql     => Comando SQL
   --    p_final_bloco => Se for verdadeiro, indica que esse comando é o final de um bloco a ser executado   
   PROCEDURE SP_ADD_INTERFACE_DDL(
      p_tipo_comando VARCHAR2,
      p_cmd_sql      CLOB,
      p_final_bloco  BOOLEAN := FALSE
   ) AS
   BEGIN
      INSERT INTO etl2.interface_ddl
      (
         cod_interface, 
         ordem_comando, 
         tipo_comando,
         linha, 
         cmd_sql
      )
      VALUES
      (
         vp_cod_interface, 
         vp_ddl_bloco_atual, 
         p_tipo_comando,
         vp_ddl_linha_atual, 
         p_cmd_sql
      );
      
      vp_ddl_linha_atual := vp_ddl_linha_atual + 1;
      IF p_final_bloco THEN
         vp_ddl_bloco_atual := vp_ddl_bloco_atual + 1;
      END IF;      
   END SP_ADD_INTERFACE_DDL;
   
   -- Objetivo: Inserir no DDL o bloco que exclui as tabelas internas   
   PROCEDURE SP_EXCLUIR_TABELAS AS 
   BEGIN
      sp_add_interface_ddl('EXCL_TAB_INTERNA', '');
      sp_add_interface_ddl('EXCL_TAB_INTERNA', 'BEGIN');
      sp_add_interface_ddl('EXCL_TAB_INTERNA', '   ETL2.PACK_ETL.SP_EXCLUIR_TABELAS_INTERNAS('||vp_cod_interface||', '||vp_cod_historico||');');
      sp_add_interface_ddl('EXCL_TAB_INTERNA', 'END;');
      sp_add_interface_ddl('EXCL_TAB_INTERNA', '/', TRUE);
   END SP_EXCLUIR_TABELAS;
   
   /*PROCEDURE SP_CRIAR_TAB_ERRO_GERAL AS 
   BEGIN
      sp_add_interface_ddl('CREATE_TAB_ERRO_GERAL', '');
      sp_add_interface_ddl('CREATE_TAB_ERRO_GERAL', 'CREATE TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'erro_geral'));
      sp_add_interface_ddl('CREATE_TAB_ERRO_GERAL', '(');
      sp_add_interface_ddl('CREATE_TAB_ERRO_GERAL', '   erro      VARCHAR2(100), ');
      sp_add_interface_ddl('CREATE_TAB_ERRO_GERAL', '   variaveis CLOB ');   
      sp_add_interface_ddl('CREATE_TAB_ERRO_GERAL', ')', TRUE);
      
   END SP_CRIAR_TAB_ERRO_GERAL;*/
   
   PROCEDURE SP_GERAR_VALIDACAO_ARQUIVO ( 
      p_nome_arquivo   VARCHAR2 := NULL,
      p_diretorio      etl2.projeto.diretorio%TYPE
   ) AS
   BEGIN
      sp_add_interface_ddl('VALIDAR_ARQUIVO','');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','BEGIN ');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','   ETL2.PACK_ETL.SP_VALIDAR_ARQUIVO ( ');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','      p_cod_interface     => '||vp_cod_interface||', ');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','      p_diretorio         => '''||p_diretorio||''', ');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','      p_nome_arquivo      => '''||p_nome_arquivo||''' ');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','   ); ');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','END; ');
      sp_add_interface_ddl('VALIDAR_ARQUIVO','/', TRUE);
   END;
   
   -- Objetivo: Cria uma nova transformação para uma interface, e seus parâmetros e colunas;
   -- Parâmetros: 
   --    p_cod_interface    => Chave da interface;
   --    p_transformacao    => Nome do texto que será usado;
   --    p_lista_colunas    => Lista de colunas separadas por vírgula no seguinte padrão [ALIAS_TABELA].[NOME_TABELA];
   --    p_lista_parametros => Lista de parâmetros separados por vírgula.
   -- Retorno: Código da transformação criada.
   FUNCTION FN_ADD_TRANSFORMACAO (
      p_cod_interface    etl2.interface.cod_interface%TYPE,
      p_transformacao    etl2.transformacao_texto.nome%TYPE,
      p_lista_colunas    VARCHAR2 := NULL,
      p_lista_parametros CLOB := NULL
   ) RETURN etl2.transformacao.cod_transformacao%TYPE AS
      
      v_posicao           PLS_INTEGER := 0;
      v_coluna            VARCHAR2(50);
      v_cod_intf_tab_col  etl2.interface_tabela_coluna.cod_interface_tabela_coluna%TYPE;
      v_cod_intf_tabela   etl2.interface_tabela.cod_interface_tabela%TYPE;
      v_cod_transformacao etl2.transformacao.cod_transformacao%TYPE;
      v_cod_coluna        etl2.coluna.cod_coluna%TYPE;
      v_nome_coluna       etl2.coluna.nome%TYPE;     
      
      CURSOR cur_busca_interface_tab_coluna(p_cod_interface NUMBER, p_coluna VARCHAR) IS
         SELECT a.cod_interface_tabela_coluna, a.cod_interface_tabela
         FROM etl2.vw_interface_coluna a
         WHERE a.alias_tabela || '.' || a.alias_coluna = p_coluna
           AND a.cod_interface = p_cod_interface;
           
      CURSOR cur_busca_coluna(p_cod_interface NUMBER, p_coluna VARCHAR) IS
         SELECT a.cod_coluna, a.nome, a.cod_interface_tabela
         FROM etl2.vw_interface_coluna_sem_uso a
         WHERE a.alias_tabela || '.' || a.nome = p_coluna
           AND a.cod_interface = p_cod_interface;
      
   BEGIN
      INSERT INTO etl2.transformacao (
         cod_transformacao, 
         cod_transformacao_texto
      )
      VALUES (
         etl2.sq_transformacao_pk.nextval, 
         (
            SELECT cod_transformacao_texto 
            FROM etl2.vw_transformacao_texto 
            WHERE nome = p_transformacao
         )
      )
      RETURNING cod_transformacao 
      INTO v_cod_transformacao;
      
      IF p_lista_parametros IS NOT NULL THEN
         v_posicao := 1;
         LOOP
            v_coluna := REGEXP_SUBSTR(p_lista_parametros, '[^,]+', 1, v_posicao);
            EXIT WHEN v_coluna IS NULL;
            
            INSERT INTO etl2.transformacao_parametro
            (cod_transformacao_parametro, cod_transformacao, texto_parametro, posicao)
            VALUES
            (etl2.sq_transformacao_parametro_pk.nextval, v_cod_transformacao, v_coluna, v_posicao);
            
            v_posicao := v_posicao + 1;
        END LOOP;
      END IF;
      
      IF p_lista_colunas IS NOT NULL THEN 
      
         v_posicao := 1; 
         -- Varrer lista de colunas separadas por vírgula
         LOOP
            v_coluna := REGEXP_SUBSTR(p_lista_colunas, '[^,]+', 1, v_posicao);
            EXIT WHEN v_coluna IS NULL;
            
            v_cod_intf_tab_col := NULL;
            v_cod_intf_tabela  := NULL;
            
            -- Verifica se essa coluna já está definida nessa "interface tabela"
            OPEN cur_busca_interface_tab_coluna(p_cod_interface, v_coluna);
            FETCH cur_busca_interface_tab_coluna INTO v_cod_intf_tab_col, v_cod_intf_tabela;
            CLOSE cur_busca_interface_tab_coluna;
            
            --DBMS_OUTPUT.PUT_LINE('v_cod_intf_tabela='||v_cod_intf_tabela||' COLUNA='||v_coluna);
            
            IF v_cod_intf_tab_col IS NULL THEN
               OPEN cur_busca_coluna(p_cod_interface, v_coluna);
                  FETCH cur_busca_coluna INTO v_cod_coluna, v_nome_coluna, v_cod_intf_tabela;
               CLOSE cur_busca_coluna;
               
               IF v_cod_intf_tabela IS NULL THEN
                  vp_variavel_erro := '#COLUNA='||v_coluna;
                  RAISE e_coluna_nao_existe_dicionario;
               END IF;
               
               INSERT INTO etl2.interface_tabela_coluna (
                  cod_interface_tabela_coluna, 
                  cod_interface_tabela, 
                  cod_coluna, 
                  alias_coluna
               )
               VALUES (
                  etl2.sq_interface_tabela_coluna_pk.nextval, 
                  v_cod_intf_tabela,
                  v_cod_coluna,
                  v_nome_coluna
               )
               RETURNING cod_interface_tabela_coluna INTO v_cod_intf_tab_col;
            END IF;
            
            INSERT INTO etl2.transformacao_coluna (
               cod_transformacao_coluna,
               cod_transformacao,
               posicao,
               cod_interface_tabela_coluna
            )
            VALUES (
               etl2.sq_transformacao_coluna_pk.nextval, 
               v_cod_transformacao, 
               v_posicao, 
               v_cod_intf_tab_col
            );
            v_posicao := v_posicao + 1;
         END LOOP;
      END IF;
      
      RETURN v_cod_transformacao;
      
   EXCEPTION WHEN e_coluna_nao_existe_dicionario THEN 
      RAISE_APPLICATION_ERROR(-c_sqlcode_coluna_n_exist_dic,tools.FN_MULTIPLE_REPLACE(c_coluna_nao_existe, vp_variavel_erro));
   END FN_ADD_TRANSFORMACAO;
   
   --**************************************************************************************************************************************************************************
   -- MÉTODOS PÚBLICOS (declarados no SPEC)
   --**************************************************************************************************************************************************************************
   
   FUNCTION FN_ADD_PROJETO(
      p_nome_projeto       etl2.projeto.nome%TYPE, 
      p_sigla_empresa      admin.empresa.sigla%TYPE,
      p_qtde_linhas_maximo etl2.projeto.qtde_linhas_maximo%TYPE,
      p_diretorio          etl2.projeto.diretorio%TYPE
   ) RETURN etl2.projeto.cod_projeto%TYPE AS
      
      v_cod_projeto etl2.projeto.cod_projeto%TYPE;
      v_cod_empresa admin.empresa.cod_empresa%TYPE;
      
      CURSOR cur_busca_projeto IS
         SELECT cod_projeto
         FROM etl2.vw_projeto
         WHERE nome = p_nome_projeto 
           AND sigla_empresa = p_sigla_empresa;
   BEGIN
   
      SELECT COD_EMPRESA INTO v_cod_empresa FROM ADMIN.EMPRESA WHERE SIGLA = p_sigla_empresa;
   
      OPEN cur_busca_projeto;
      FETCH cur_busca_projeto INTO v_cod_projeto;
      IF cur_busca_projeto%NOTFOUND THEN
         INSERT INTO etl2.projeto (
            cod_projeto, 
            nome, 
            cod_empresa, 
            data_cadastro, 
            data_alteracao,
            qtde_linhas_maximo,
            diretorio
         ) 
         VALUES (
            etl2.sq_projeto_pk.nextval, 
            p_nome_projeto, 
            v_cod_empresa, 
            SYSDATE, 
            NULL,
            p_qtde_linhas_maximo,
            p_diretorio 
         )
         RETURNING cod_projeto INTO v_cod_projeto;
      END IF;
      
      RETURN v_cod_projeto;
   END FN_ADD_PROJETO;
   
   PROCEDURE SP_ADD_PROJETO (
      p_nome_projeto       etl2.projeto.nome%TYPE, 
      p_descricao          etl2.projeto.descricao%TYPE,
      p_sigla_empresa      admin.empresa.sigla%TYPE,
      p_qtde_linhas_maximo etl2.projeto.qtde_linhas_maximo%TYPE,
      p_diretorio          etl2.projeto.diretorio%TYPE,
      p_sql_reverso        etl2.projeto.sql_reverso%TYPE
   ) AS
      v_cod_projeto NUMBER;
   BEGIN
      v_cod_projeto := 
         FN_ADD_PROJETO(
            p_nome_projeto       => p_nome_projeto,
            p_sigla_empresa      => p_sigla_empresa,
            p_qtde_linhas_maximo => p_qtde_linhas_maximo,
            p_diretorio          => p_diretorio
         );
   END;
   
   PROCEDURE SP_EXCLUIR_TABELAS_INTERNAS (
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_cod_historico etl2.historico.cod_historico%TYPE
   ) AS 
      v_table_name VARCHAR2(4000);
      
      PROCEDURE excluir(p_nome VARCHAR2, p_truncate BOOLEAN := TRUE) AS
      BEGIN        
         v_table_name := fn_get_nome_tabela(p_cod_interface, p_cod_historico, p_nome);
         
         IF p_truncate THEN
            EXECUTE IMMEDIATE 'TRUNCATE TABLE etl2.'|| v_table_name;
         END IF;
         EXECUTE IMMEDIATE 'DROP TABLE etl2.'|| v_table_name;         
      
      EXCEPTION
         WHEN OTHERS THEN
         IF SQLCODE <> -942 THEN -- Erro de SQL: ORA-00942: a tabela ou view não existe
            RAISE;
         END IF; 
      END;
   BEGIN
      excluir('erro');
      --excluir('erro_geral');
      excluir('origem');
      excluir('fluxo');
      excluir('fluxo_erro');
      excluir('origem_erro');
      excluir('externa', FALSE);
      excluir('externa_materializada'); 
      excluir('backup'); 
      excluir('exception');
   END SP_EXCLUIR_TABELAS_INTERNAS;
   
   PROCEDURE SP_ADD_TABELA(
      p_esquema_tabela etl2.tabela.esquema%TYPE, 
      p_nome_tabela    etl2.tabela.nome%TYPE
   ) AS 
      v_contagem_tabela NUMBER := 0;
   BEGIN
      
      SELECT count(*) INTO v_contagem_tabela
      FROM (
         SELECT table_name, owner
         FROM all_tables
         WHERE owner = p_esquema_tabela AND table_name = p_nome_tabela
         UNION 
         SELECT view_name, owner
         FROM all_views
         WHERE owner = p_esquema_tabela AND view_name = p_nome_tabela
      );
      
      IF NVL(v_contagem_tabela, 0) = 0 THEN 
         vp_variavel_erro := '#TABELA='||p_esquema_tabela||'.'||p_nome_tabela;
         RAISE e_tabela_nao_existe_banco;
      END IF;
      
   
      MERGE INTO etl2.tabela tgt
      USING (SELECT table_name, owner
             FROM all_tables
             WHERE owner = p_esquema_tabela AND table_name = p_nome_tabela
             UNION 
             SELECT view_name, owner
             FROM all_views
             WHERE owner = p_esquema_tabela AND view_name = p_nome_tabela
            ) src ON (tgt.nome = src.table_name AND  tgt.esquema = src.owner)
      WHEN MATCHED THEN
         UPDATE SET tgt.data_alteracao = SYSDATE
      WHEN NOT MATCHED THEN          
         INSERT (cod_tabela, nome, esquema, data_cadastro)
         VALUES (etl2.sq_tabela_pk.nextval, src.table_name, src.owner, sysdate);
         
      MERGE INTO etl2.coluna tgt
      USING (SELECT a.owner, a.table_name, a.column_name, b.cod_tabela, 
                    a.column_id, a.data_type, a.data_length, a.data_precision, a.data_scale,
                    CASE WHEN a.nullable = 'Y' THEN 'N'
                         ELSE 'S' 
                    END AS obrigatorio
             FROM all_tab_columns a
                  JOIN etl2.tabela b ON (a.owner = b.esquema 
                                         AND a.table_name = b.nome)
             WHERE a.owner = p_esquema_tabela 
               AND a.table_name = p_nome_tabela 
            ) src
            ON (tgt.cod_tabela = src.cod_tabela AND tgt.nome = src.column_name)
      WHEN MATCHED THEN
         UPDATE SET 
            tgt.posicao     = src.column_id, 
            tgt.tipo        = src.data_type, 
            tgt.tamanho     = src.data_length, 
            tgt.precisao    = src.data_precision, 
            tgt.obrigatorio = src.obrigatorio,
            tgt.escala      = src.data_scale
      WHEN NOT MATCHED THEN
         INSERT (COD_COLUNA, COD_TABELA, NOME, POSICAO, TIPO, TAMANHO, PRECISAO, OBRIGATORIO, ESCALA)
         VALUES (ETL2.SQ_COLUNA_PK.NEXTVAL, src.cod_tabela, src.column_name, src.column_id, 
                 src.data_type, src.data_length, src.data_precision, src.obrigatorio, src.data_scale);
   EXCEPTION 
      WHEN e_tabela_nao_existe_banco THEN
         RAISE_APPLICATION_ERROR(-c_sqlcode_tabela_n_exist_bnc, TOOLS.FN_MULTIPLE_REPLACE(c_tabela_nao_existe, vp_variavel_erro));
   END;
   
   FUNCTION FN_ADD_TABELA_EXTERNA(
      p_esquema_tabela   etl2.tabela.esquema%TYPE, 
      p_nome_tabela      etl2.tabela.nome%TYPE, 
      p_lista_colunas    CLOB,
      p_formato_arquivo  VARCHAR2 := 'CSV',
      p_limitador_coluna VARCHAR2 := ';'
   ) RETURN etl2.tabela.cod_tabela%TYPE AS
      v_cod_tabela   etl2.tabela.cod_tabela%TYPE;
      v_cod_coluna   etl2.coluna.cod_coluna%TYPE;
      v_coluna       etl2.coluna.nome%TYPE;
      v_posicao      NUMBER;
   BEGIN
      MERGE INTO etl2.tabela tgt
      USING (
         SELECT 
            UPPER(p_nome_tabela) nome, 
            UPPER(p_esquema_tabela) esquema
         FROM DUAL
      ) src
      ON (tgt.nome = src.nome AND tgt.esquema = src.esquema)
      WHEN MATCHED THEN 
         UPDATE SET data_alteracao= SYSDATE            
      WHEN NOT MATCHED THEN
         INSERT (
            cod_tabela,
            nome,
            esquema,
            data_cadastro,
            data_alteracao
         )
         VALUES (
            etl2.sq_tabela_pk.nextval, 
            src.nome, 
            src.esquema, 
            SYSDATE,
            NULL
         )
      ;
      
      SELECT cod_tabela INTO v_cod_tabela
      FROM etl2.tabela
      WHERE esquema = p_esquema_tabela
        AND nome = p_nome_tabela;      
      
      MERGE INTO etl2.tabela_externa tgt
      USING (
         SELECT 
            v_cod_tabela cod_tabela,
            p_formato_arquivo formato_arquivo,
            p_limitador_coluna limitador_coluna,
            chr(13) limitador_linha,
            1 qtde_linha_cabecalho
         FROM DUAL
      ) src
      ON (tgt.cod_tabela = src.cod_tabela)
      WHEN MATCHED THEN
         UPDATE SET
            tgt.formato_arquivo      = src.formato_arquivo, 
            tgt.limitador_coluna     = src.limitador_coluna, 
            tgt.limitador_linha      = src.limitador_linha, 
            tgt.qtde_linha_cabecalho = src.qtde_linha_cabecalho
      WHEN NOT MATCHED THEN
         INSERT 
            (cod_tabela, formato_arquivo, limitador_coluna, limitador_linha, qtde_linha_cabecalho)
         VALUES
            (v_cod_tabela, src.formato_arquivo, src.limitador_coluna, src.limitador_linha, src.qtde_linha_cabecalho);
      
      -- Mesclar colunas novas com antigas.
      v_posicao := 1;
      LOOP
         v_coluna := regexp_substr(p_lista_colunas, '[^,]+', 1, v_posicao);
         EXIT WHEN v_coluna IS NULL;
           
         MERGE INTO etl2.coluna tgt
         USING (
            SELECT 
               v_cod_tabela cod_tabela, 
               upper(v_coluna) nome, 
               v_posicao posicao, 
               'VARCHAR2' tipo, 
               4000 tamanho, 
               NULL as precisao, 
               'N' obrigatorio
            FROM dual) src
         ON (tgt.cod_tabela = src.cod_tabela AND tgt.nome = src.nome)
         WHEN MATCHED THEN 
            UPDATE SET 
               tgt.posicao =src.posicao,
               tgt.tipo=src.tipo,
               tgt.tamanho=src.tamanho,
               tgt.precisao=src.precisao,
               tgt.obrigatorio=src.obrigatorio
         WHEN NOT MATCHED THEN
            INSERT
               (cod_coluna, cod_tabela, nome, posicao, tipo, tamanho, precisao, obrigatorio)
            VALUES
               (etl2.sq_coluna_pk.nextval, v_cod_tabela, v_coluna, v_posicao, 'VARCHAR2', 4000, NULL, 'N');
         
         v_posicao := v_posicao + 1;
      END LOOP;
      
      -- Exclui colunas que não existem mais
      DELETE FROM etl2.coluna 
      WHERE cod_tabela = v_cod_tabela 
         AND regexp_instr(p_lista_colunas, '(^|,)'||nome||'(,|$)') = 0;
      
      RETURN v_cod_tabela;
   END;
   
   PROCEDURE SP_ADD_TABELA_EXTERNA (
      p_esquema_tabela   etl2.tabela.esquema%TYPE, 
      p_nome_tabela      etl2.tabela.nome%TYPE, 
      p_lista_colunas    CLOB
   ) AS
      v_cod_tabela NUMBER;
   BEGIN
      v_cod_tabela := FN_ADD_TABELA_EXTERNA(
      p_esquema_tabela   => p_esquema_tabela, 
      p_nome_tabela      => p_nome_tabela, 
      p_lista_colunas    => p_lista_colunas);
   END;
   
   FUNCTION FN_ADD_INTERFACE(
      p_nome_interface      etl2.interface.nome%TYPE,
      p_esquema_tabela_alvo etl2.tabela.esquema%TYPE,
      p_nome_tabela_alvo    etl2.tabela.nome%TYPE,
      p_commit              etl2.interface.confirmar_alteracoes%TYPE,
      p_delete_all          etl2.interface_importacao.excluir_tudo_dml%TYPE,
      p_truncate            etl2.interface_importacao.excluir_tudo_ddl%TYPE,
      p_distinct            etl2.interface_importacao.somente_distintos%TYPE,
      p_estrat_deteccao     etl2.interface_importacao.estrategia_deteccao%TYPE,
      p_estrat_atualizacao  etl2.interface_importacao.estrategia_atualizacao%TYPE,
      p_cod_nivel_log       etl2.interface.cod_nivel_log%TYPE
   ) RETURN etl2.interface.cod_interface%TYPE AS 
      v_cod_interface NUMBER;
   BEGIN

      INSERT INTO etl2.interface (
         cod_interface,
         nome,
         data_cadastro,
         data_alteracao,
         confirmar_alteracoes,
         cod_nivel_log
      )
      VALUES (
         etl2.sq_interface_pk.nextval,
         p_nome_interface,
         sysdate,
         null,
         p_commit,
         p_cod_nivel_log
      )
      RETURNING cod_interface INTO v_cod_interface;
      
      INSERT INTO etl2.interface_importacao (
         cod_interface,
         excluir_tudo_dml,
         excluir_tudo_ddl,
         somente_distintos,
         estrategia_deteccao,
         estrategia_atualizacao
      )
      VALUES (
         v_cod_interface,
         p_delete_all,
         p_truncate,
         p_distinct,
         p_estrat_deteccao,
         p_estrat_atualizacao
      );
      
      INSERT INTO etl2.interface_tabela (
         cod_interface_tabela, 
         cod_interface, 
         cod_tabela, 
         alias_tabela, 
         tipo_tabela,
         posicao
      )
      VALUES (
         etl2.sq_interface_tabela_pk.nextval,
         v_cod_interface,
         (
            SELECT tab.cod_tabela
            FROM etl2.tabela tab
            WHERE tab.nome = p_nome_tabela_alvo
              AND tab.esquema = p_esquema_tabela_alvo
         ),
         p_nome_tabela_alvo,
         c_tipo_tabela_alvo,
         0
      );
      
      RETURN v_cod_interface;
   END;
   
   PROCEDURE SP_ADD_INTERFACE (
      p_nome_interface      etl2.interface.nome%TYPE, 
      p_esquema_tabela_alvo etl2.tabela.esquema%TYPE, 
      p_nome_tabela_alvo    etl2.tabela.nome%TYPE, 
      p_commit              etl2.interface.confirmar_alteracoes%TYPE, 
      p_delete_all          etl2.interface_importacao.excluir_tudo_dml%TYPE,
      p_truncate            etl2.interface_importacao.excluir_tudo_ddl%TYPE, 
      p_distinct            etl2.interface_importacao.somente_distintos%TYPE,  
      p_estrat_deteccao     etl2.interface_importacao.estrategia_deteccao%TYPE,
      p_estrat_atualizacao  etl2.interface_importacao.estrategia_atualizacao%TYPE,
      p_cod_nivel_log       etl2.interface.cod_nivel_log%TYPE
   ) AS
      v_cod_interface NUMBER;
   BEGIN
      v_cod_interface := FN_ADD_INTERFACE(
      p_nome_interface      => p_nome_interface, 
      p_esquema_tabela_alvo => p_esquema_tabela_alvo, 
      p_nome_tabela_alvo    => p_nome_tabela_alvo, 
      p_commit              => p_commit, 
      p_delete_all          => p_delete_all,
      p_truncate            => p_truncate, 
      p_distinct            => p_distinct,  
      p_estrat_deteccao     => p_estrat_deteccao,
      p_estrat_atualizacao  => p_estrat_atualizacao,
      p_cod_nivel_log       => p_cod_nivel_log);
   END SP_ADD_INTERFACE;
   
   PROCEDURE SP_ADD_INTERFACE_PROCESSAMENTO (
      p_nome_interface      etl2.interface.nome%TYPE, 
      p_commit              etl2.interface.confirmar_alteracoes%TYPE, 
      p_esquema             etl2.interface_processamento.esquema%TYPE,
      p_nome_procedimento   etl2.interface_processamento.nome_procedimento%TYPE,
      p_cod_nivel_log       etl2.interface.cod_nivel_log%TYPE
   ) AS
      v_cod_interface etl2.interface.cod_interface%TYPE;
   BEGIN
      INSERT INTO etl2.interface (
         cod_interface,
         nome,
         data_cadastro,
         data_alteracao,
         confirmar_alteracoes,
         cod_nivel_log
      )
      VALUES (
         etl2.sq_interface_pk.nextval,
         p_nome_interface,
         sysdate,
         null,
         p_commit,
         p_cod_nivel_log
      )
      RETURNING cod_interface INTO v_cod_interface;
      
      INSERT INTO etl2.interface_processamento (
         cod_interface,
         esquema,
         nome_procedimento
      )
      VALUES (
         v_cod_interface,
         p_esquema,
         p_nome_procedimento
      );
   END SP_ADD_INTERFACE_PROCESSAMENTO;
   
   FUNCTION FN_ADD_TRANSFORMACAO_TEXTO(
      p_nome  etl2.transformacao_texto.nome%TYPE,
      p_texto etl2.transformacao_texto.texto%TYPE
   ) RETURN etl2.transformacao_texto.cod_transformacao_texto%TYPE AS
      v_cod_transformacao_texto etl2.transformacao_texto.cod_transformacao_texto%TYPE;
   BEGIN
      MERGE INTO etl2.transformacao_texto tgt
      USING (SELECT p_nome nome, p_texto texto FROM dual) src
            ON (src.nome = tgt.nome)
      WHEN MATCHED THEN
         UPDATE SET 
            tgt.texto = src.texto
      WHEN NOT MATCHED THEN
         INSERT (COD_TRANSFORMACAO_TEXTO, NOME, TEXTO)
         VALUES (etl2.sq_transformacao_texto_pk.nextval, src.nome, src.texto);
      
      SELECT cod_transformacao_texto INTO v_cod_transformacao_texto
      FROM etl2.vw_transformacao_texto
      WHERE nome = p_nome;
      
      RETURN v_cod_transformacao_texto;
   END;
   
   PROCEDURE SP_ADD_TRANSFORMACAO_TEXTO(
      p_nome  etl2.transformacao_texto.nome%TYPE,
      p_texto etl2.transformacao_texto.texto%TYPE
   ) AS
      v_cod_transformacao_texto NUMBER;
   BEGIN
      v_cod_transformacao_texto := FN_ADD_TRANSFORMACAO_TEXTO(p_nome, p_texto);
   END;
   
   PROCEDURE SP_ADD_INTERFACE_TABELA(
      p_cod_interface      etl2.interface.cod_interface%TYPE,
      p_esquema            etl2.tabela.esquema%TYPE, 
      p_tabela             etl2.tabela.nome%TYPE,
      p_apelido_tabela     etl2.interface_tabela.alias_tabela%TYPE,
      p_posicao            etl2.interface_tabela.posicao%TYPE,      
      p_tipo_tabela        etl2.interface_tabela.tipo_tabela%TYPE,
		p_prefixo_join       etl2.interface_tabela.prefixo_join%TYPE := 'LEFT'
   ) AS
      v_cod_intf_tab etl2.interface_tabela.cod_interface_tabela%TYPE;
   BEGIN
      v_cod_intf_tab := FN_ADD_INTERFACE_TABELA(
         p_cod_interface,
         p_esquema,
         p_tabela,
         p_apelido_tabela,
         p_posicao,
         p_tipo_tabela,
			p_prefixo_join
      );      
   END;
   
   FUNCTION FN_ADD_INTERFACE_TABELA(
      p_cod_interface      etl2.interface.cod_interface%TYPE,
      p_esquema            etl2.tabela.esquema%TYPE, 
      p_tabela             etl2.tabela.nome%TYPE,
      p_apelido_tabela     etl2.interface_tabela.alias_tabela%TYPE,
      p_posicao            etl2.interface_tabela.posicao%TYPE,      
      p_tipo_tabela        etl2.interface_tabela.tipo_tabela%TYPE,
		p_prefixo_join       etl2.interface_tabela.prefixo_join%TYPE := 'LEFT'
   ) RETURN etl2.interface_tabela.cod_interface_tabela%TYPE AS

      v_cod_intf_tabela   etl2.interface_tabela.cod_interface_tabela%TYPE;
           
   BEGIN

      INSERT INTO etl2.interface_tabela (
         cod_interface_tabela, 
         cod_interface, 
         cod_tabela, 
         alias_tabela, 
         tipo_tabela,
         posicao,
			prefixo_join
      )
      VALUES (
         etl2.sq_interface_tabela_pk.nextval,
         p_cod_interface,
         (
            SELECT tab.cod_tabela
            FROM etl2.tabela tab
            WHERE tab.nome = p_tabela
              AND tab.esquema = p_esquema
         ),
         p_apelido_tabela,
         p_tipo_tabela,
         p_posicao,
			p_prefixo_join
      )
      RETURNING cod_interface_tabela INTO v_cod_intf_tabela;

      RETURN v_cod_intf_tabela;  
      
   END FN_ADD_INTERFACE_TABELA;   
   
   PROCEDURE SP_ADD_JOIN(
      p_cod_interface    etl2.interface.cod_interface%TYPE,
      p_tabela           etl2.tabela.nome%TYPE,
      p_transformacao    etl2.transformacao_texto.nome%TYPE,
      p_lista_colunas    VARCHAR2 := NULL,
      p_lista_parametros CLOB := NULL,
      p_excluir_antigo   CHAR := 'N'
   ) AS
      v_cod_transformacao etl2.transformacao.cod_transformacao%TYPE;
      v_cod_interface_tabela etl2.interface_tabela.cod_interface_tabela%TYPE;
      
   BEGIN
      -- Excluir transformação antiga
      SELECT cod_transformacao, cod_interface_tabela INTO v_cod_transformacao, v_cod_interface_tabela
      FROM etl2.vw_interface_tabela_join
      WHERE cod_interface = p_cod_interface
        AND alias_tabela = p_tabela;
         
      IF v_cod_transformacao IS NOT NULL THEN
         IF p_excluir_antigo = 'S' THEN
            UPDATE etl2.interface_tabela 
            SET cod_transformacao_join = NULL 
            WHERE cod_interface_tabela = v_cod_interface_tabela;
            
            DELETE FROM etl2.transformacao_coluna WHERE cod_transformacao = v_cod_transformacao;
            DELETE FROM etl2.transformacao_parametro WHERE cod_transformacao = v_cod_transformacao;
            DELETE FROM etl2.transformacao WHERE cod_transformacao = v_cod_transformacao;
         ELSE 
            vp_variavel_erro := '#TABELA='||p_tabela;
            RAISE e_ja_existe_join_tabela;
         END IF;      
      END IF;
      
      v_cod_transformacao := FN_ADD_TRANSFORMACAO(p_cod_interface, p_transformacao, p_lista_colunas, p_lista_parametros);
      
      -- Atualizar "interface tabela" com a transformação
      UPDATE etl2.interface_tabela
      SET cod_transformacao_join = v_cod_transformacao
      WHERE cod_interface_tabela = v_cod_interface_tabela;
   
   EXCEPTION
      WHEN e_ja_existe_join_tabela THEN 
         RAISE_APPLICATION_ERROR(-c_sqlcode_ja_existe_join_tab,c_ja_existe_join);
      
   END SP_ADD_JOIN;
   
   PROCEDURE SP_ADD_MAPEAMENTO(
      p_cod_interface       etl2.interface.cod_interface%TYPE,
      p_coluna_alvo         etl2.interface_tabela_coluna.alias_coluna%TYPE,
      p_transformacao       etl2.transformacao_texto.nome%TYPE,
      p_esquema_execucao    etl2.mapeamento.esquema_execucao%TYPE,
      p_posicao             etl2.mapeamento.posicao%TYPE,
      p_chave_atualizacao   etl2.mapeamento.chave_atualizacao%TYPE,
      p_insert              etl2.mapeamento.participa_insercao%TYPE,
      p_update              etl2.mapeamento.participa_atualizacao%TYPE,
      p_lista_colunas       CLOB := NULL,
      p_lista_parametros    CLOB := NULL
   ) AS 
      v_cod_map etl2.mapeamento.cod_mapeamento%TYPE;
   BEGIN
      v_cod_map := FN_ADD_MAPEAMENTO(
         p_cod_interface,
         p_coluna_alvo,
         p_transformacao,
         p_esquema_execucao,
         p_posicao,
         p_chave_atualizacao,
         p_insert,
         p_update,
         p_lista_colunas,
         p_lista_parametros
      );
   END;
   
   PROCEDURE SP_ADD_COLUNA_CALCULADA (
      p_cod_interface    etl2.interface.cod_interface%TYPE,
      p_esquema_tabela   etl2.tabela.esquema%TYPE, 
      p_nome_tabela      etl2.tabela.nome%TYPE, 
      p_coluna_original  etl2.coluna.nome%TYPE,
      p_alias_coluna     etl2.interface_tabela_coluna.alias_coluna%TYPE,
      p_transformacao    etl2.transformacao_texto.nome%TYPE,
      p_lista_parametros CLOB := NULL
   ) AS 
      v_cod_intf_tabela      etl2.interface_tabela.cod_interface_tabela%TYPE;
      v_cod_transformacao    etl2.transformacao.cod_transformacao%TYPE;
      v_alias_tabela_externa etl2.interface_tabela.alias_tabela%TYPE;
      v_cod_tabela           etl2.coluna.cod_coluna%TYPE;
      v_cod_coluna           etl2.coluna.cod_coluna%TYPE;
   BEGIN
      SELECT cod_interface_tabela, alias_tabela, cod_tabela INTO v_cod_intf_tabela, v_alias_tabela_externa, v_cod_tabela
      FROM etl2.vw_interface_tabela_join 
      WHERE cod_interface = p_cod_interface 
        AND tabela_externa = 'S';
        
      BEGIN
        SELECT cod_coluna INTO v_cod_coluna
        FROM etl2.coluna 
        WHERE cod_tabela = v_cod_tabela
          AND nome = p_coluna_original;
      EXCEPTION 
         WHEN NO_DATA_FOUND THEN 
            vp_variavel_erro := '#COLUNA='||p_coluna_original;
            RAISE_APPLICATION_ERROR(-c_sqlcode_coluna_n_exist_dic,tools.FN_MULTIPLE_REPLACE(c_coluna_nao_existe, vp_variavel_erro));
      END;
      
        
      v_cod_transformacao := FN_ADD_TRANSFORMACAO(p_cod_interface, p_transformacao, v_alias_tabela_externa||'.'||p_coluna_original, p_lista_parametros);
        
      INSERT INTO etl2.interface_tabela_coluna (
         cod_interface_tabela_coluna, 
         cod_interface_tabela, 
         cod_coluna, 
         alias_coluna,
         cod_transformacao_calc
      )
      VALUES (
         etl2.sq_interface_tabela_coluna_pk.nextval,
         v_cod_intf_tabela,
         v_cod_coluna,
         p_alias_coluna,
         v_cod_transformacao
      ); 
   END;

   FUNCTION FN_ADD_MAPEAMENTO(
      p_cod_interface       etl2.interface.cod_interface%TYPE,
      p_coluna_alvo         etl2.coluna.nome%TYPE,
      p_transformacao       etl2.transformacao_texto.nome%TYPE,
      p_esquema_execucao    etl2.mapeamento.esquema_execucao%TYPE,
      p_posicao             etl2.mapeamento.posicao%TYPE,
      p_chave_atualizacao   etl2.mapeamento.chave_atualizacao%TYPE,
      p_insert              etl2.mapeamento.participa_insercao%TYPE,
      p_update              etl2.mapeamento.participa_atualizacao%TYPE,
      p_lista_colunas       CLOB := NULL,
      p_lista_parametros    CLOB := NULL
   ) RETURN etl2.mapeamento.cod_mapeamento%TYPE AS
      
      v_cod_intf_tab_col   etl2.interface_tabela_coluna.cod_interface_tabela_coluna%TYPE;
      v_cod_intf_tabela    etl2.interface_tabela.cod_interface_tabela%TYPE;
      v_cod_transformacao  etl2.transformacao.cod_transformacao%TYPE;
      v_cod_mapeamento     etl2.mapeamento.cod_mapeamento%TYPE;
      v_cod_coluna         etl2.coluna.cod_coluna%TYPE;
      v_nome_coluna        etl2.coluna.nome%TYPE;
      
      CURSOR cur_busca_intf_coluna_alvo IS
         SELECT intftabcol.cod_interface_tabela_coluna, intftab.cod_interface_tabela
         FROM etl2.interface_tabela intftab
              LEFT JOIN (
                 SELECT * 
                 FROM etl2.interface_tabela_coluna a
                      JOIN etl2.coluna b ON (a.cod_coluna = b.cod_coluna)
                 WHERE b.nome = p_coluna_alvo
              ) intftabcol ON (intftab.cod_interface_tabela = intftabcol.cod_interface_tabela)
         WHERE intftab.cod_interface = p_cod_interface
           AND intftab.tipo_tabela = c_tipo_tabela_alvo;
           
      CURSOR cur_busca_coluna IS 
         SELECT col.cod_coluna,
                col.nome
         FROM etl2.coluna col
              JOIN etl2.interface_tabela intftab ON (col.cod_tabela = intftab.cod_tabela)
         WHERE intftab.cod_interface_tabela = v_cod_intf_tabela
           AND col.nome = p_coluna_alvo;
      
   BEGIN
      v_cod_transformacao := FN_ADD_TRANSFORMACAO(p_cod_interface, p_transformacao, p_lista_colunas, p_lista_parametros);
      
      v_cod_intf_tab_col := NULL;
      v_cod_intf_tabela  := NULL;
      
      -- Verifica se já existe o relacionamento entre a interface e essa coluna
      OPEN cur_busca_intf_coluna_alvo;
      FETCH cur_busca_intf_coluna_alvo INTO v_cod_intf_tab_col, v_cod_intf_tabela;
      CLOSE cur_busca_intf_coluna_alvo;
            
      -- Insere o relacionamento entre interface e a coluna, caso não exista.
      IF v_cod_intf_tab_col IS NULL THEN
         v_cod_coluna := NULL;
         
         OPEN cur_busca_coluna;
         FETCH cur_busca_coluna INTO v_cod_coluna, v_nome_coluna;
         CLOSE cur_busca_coluna;
         
         IF v_cod_coluna IS NULL THEN 
            vp_variavel_erro:= '#COLUNA='||p_coluna_alvo||'(alvo)';
            RAISE e_coluna_nao_existe_intf;
            
         END IF;
         
         INSERT INTO etl2.interface_tabela_coluna (
            cod_interface_tabela_coluna, 
            cod_interface_tabela, 
            cod_coluna, 
            alias_coluna
         )
         VALUES (
            etl2.sq_interface_tabela_coluna_pk.nextval,
            v_cod_intf_tabela,
            v_cod_coluna,
            v_nome_coluna
         )         
         RETURNING cod_interface_tabela_coluna INTO v_cod_intf_tab_col;
      END IF;
      
      INSERT INTO etl2.mapeamento (
         cod_mapeamento, 
         cod_interface, 
         cod_interface_tabela_col_alvo, 
         cod_transformacao, 
         esquema_execucao, 
         posicao, 
         chave_atualizacao, 
         participa_insercao, 
         participa_atualizacao
      )
      VALUES (
         etl2.sq_mapeamento_pk.nextval, 
         p_cod_interface, 
         v_cod_intf_tab_col, 
         v_cod_transformacao, 
         p_esquema_execucao,
         p_posicao, 
         p_chave_atualizacao, 
         p_insert, 
         p_update
      )
      RETURNING cod_mapeamento INTO v_cod_mapeamento;
       
      RETURN v_cod_mapeamento;
   EXCEPTION
      WHEN e_coluna_nao_existe_intf THEN
         RAISE_APPLICATION_ERROR(-c_sqlcode_coluna_n_exist_int, tools.FN_MULTIPLE_REPLACE(c_coluna_nao_existe_intf, vp_variavel_erro));
   END;
   
   PROCEDURE SP_DROP_MAPEAMENTO(
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_coluna_alvo   etl2.coluna.nome%TYPE
   ) AS
      v_cod_transformacao NUMBER;
      v_cod_intf_tab_col  NUMBER;
   BEGIN
      SELECT intftabcol.cod_interface_tabela_coluna INTO v_cod_intf_tab_col
      FROM etl2.interface_tabela intftab
           JOIN etl2.interface_tabela_coluna intftabcol ON (intftab.cod_interface_tabela = intftabcol.cod_interface_tabela)
           JOIN etl2.coluna col ON (intftabcol.cod_coluna = col.cod_coluna)
      WHERE intftab.cod_interface = p_cod_interface
        AND intftab.tipo_tabela = c_tipo_tabela_alvo
        AND col.nome = p_coluna_alvo;
   
      DELETE FROM etl2.mapeamento 
      WHERE cod_interface = p_cod_interface 
        AND cod_interface_tabela_col_alvo = v_cod_intf_tab_col
      RETURNING cod_transformacao INTO v_cod_transformacao;
      
      DELETE FROM etl2.transformacao_parametro
      WHERE cod_transformacao = v_cod_transformacao;
      
      DELETE FROM etl2.transformacao_coluna
      WHERE cod_transformacao = v_cod_transformacao;
      
      DELETE FROM etl2.transformacao
      WHERE cod_transformacao = v_cod_transformacao;
      
      DBMS_OUTPUT.PUT_LINE('Mapeamento da coluna '||p_coluna_alvo||' removido!');
   END;
   
   PROCEDURE SP_ADD_TRANSFORMACAO_PARAMETRO(
      p_cod_transformacao etl2.transformacao.cod_transformacao%TYPE,
      p_texto_parametro   etl2.transformacao_parametro.texto_parametro%TYPE,
      p_posicao           etl2.transformacao_coluna.posicao%TYPE
   ) AS 
   BEGIN
      INSERT INTO etl2.transformacao_parametro
      (COD_TRANSFORMACAO_PARAMETRO, COD_TRANSFORMACAO, TEXTO_PARAMETRO, POSICAO)
      VALUES
      (etl2.sq_transformacao_parametro_pk.nextval, p_cod_transformacao, p_texto_parametro, p_posicao);
   END;
   
   PROCEDURE SP_VALIDAR_AGENDAMENTO (
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_cod_historico etl2.historico.cod_historico%TYPE
   ) AS 
      v_origem_cursor    t_ref_cursor;
      v_contagem_entrada NUMBER;
      v_max_permitido    NUMBER;
      v_sigla_status     etl2.vw_historico_execucao.sigla_status%TYPE;
      v_cod_projeto      etl2.projeto.cod_projeto%TYPE;
      
   BEGIN
      OPEN v_origem_cursor FOR 'SELECT count(*) FROM '|| fn_get_nome_tabela(p_cod_interface, p_cod_historico, 'externa');
      FETCH v_origem_cursor INTO v_contagem_entrada;
      CLOSE v_origem_cursor ;
      
      SELECT cod_projeto, sigla_status INTO v_cod_projeto, v_sigla_status
      FROM etl2.vw_historico_execucao
      WHERE cod_historico = p_cod_historico;
      
      SELECT qtde_linhas_maximo INTO v_max_permitido
      FROM etl2.projeto
      WHERE cod_projeto = v_cod_projeto;
      
      IF v_sigla_status <> c_st_agendado AND v_contagem_entrada > v_max_permitido THEN 
         sp_excluir_tabelas_internas(p_cod_interface, p_cod_historico);
         RAISE_APPLICATION_ERROR(-c_sqlcode_agendamento, c_agendamento);
      END IF;
   END;

   /** -------------------------------------------------------------------------
      Objetivo: validar o arquivo da tabela externa
      Parâmetros:
         p_cod_interface  => Código da interface que possui a tabela externa
         p_diretorio      => Diretório Oracle onde está o arquivo
         p_nome_arquivo   => Nome do arquivo analisado
   ---------------------------------------------------------------------------*/
   PROCEDURE SP_VALIDAR_ARQUIVO (
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_diretorio      etl2.projeto.diretorio%TYPE,
      p_nome_arquivo   VARCHAR2
   ) AS 
      CURSOR cur_interface IS
         SELECT cod_tabela_externa, qtde_linha_cabecalho, formato_arquivo
         FROM ETL2.vw_interface a
         WHERE cod_interface = p_cod_interface;
         
      v_arquivo    UTL_FILE.FILE_TYPE;
      v_arq_modelo UTL_FILE.FILE_TYPE;
      
      v_interface cur_interface%ROWTYPE;
      v_cabecalho_configurado VARCHAR(32767);
      v_cabecalho_do_arquivo  VARCHAR(32767);
      
      v_qtde_colunas_arquivo NUMBER;
      v_qtde_colunas_config  NUMBER;
      
      v_posicao NUMBER;
      v_coluna_arquivo  VARCHAR2(30);
      v_coluna_config   VARCHAR2(30);
      v_erro            CLOB;
      
      procedure gerar_modelo as 
      begin
         v_arq_modelo  := SYS.UTL_FILE.FOPEN(p_diretorio, 'modelo.csv', 'w', 32767);
         UTL_FILE.PUT_LINE(v_arq_modelo, REPLACE(REPLACE(TRIM(v_cabecalho_configurado), chr(13), null), chr(10), null));
         UTL_FILE.FCLOSE(v_arq_modelo);
      end;

   BEGIN
      OPEN cur_interface;
      FETCH cur_interface INTO v_interface;
      CLOSE cur_interface;
      
      IF v_interface.formato_arquivo = c_formato_arquivo_csv THEN 
         IF v_interface.cod_tabela_externa IS NOT NULL THEN
            
            -- Agrupa colunas configuradas separadas por ';' -------------------------
            SELECT listagg(nome, ';') within group (order by posicao)
            INTO v_cabecalho_configurado
            FROM ETL2.vw_interface_coluna a
            where cod_interface = p_cod_interface
               AND tabela_externa = 'S'
               AND cod_transformacao_calc IS NULL;
            --------------------------------------------------------------------------
      
            -- Abre arquivo e procura a última linha do cabeçalho --------------------
            v_arquivo := sys.utl_file.fopen(p_diretorio, p_nome_arquivo, 'r', 32767);
            v_posicao := 1;
            LOOP
               BEGIN
                  utl_file.get_line(v_arquivo, v_cabecalho_do_arquivo, 32767);
               EXCEPTION
                  WHEN no_data_found THEN
                     v_cabecalho_do_arquivo := NULL;
                     EXIT;
               END;
               v_posicao := v_posicao + 1;
               
               IF v_posicao > NVL(v_interface.qtde_linha_cabecalho,0) THEN
                  EXIT;
               END IF;
            
            END LOOP;
            utl_file.fclose(v_arquivo);
            --------------------------------------------------------------------------
      
            -- Comparação do cabeçalho configurado em relação ao cabeçalho do arquivo-
            IF v_cabecalho_do_arquivo IS NULL OR REPLACE(REPLACE(v_cabecalho_do_arquivo, chr(13), NULL), chr(10), NULL) <> v_cabecalho_configurado THEN
               
               v_qtde_colunas_arquivo := REGEXP_COUNT(v_cabecalho_do_arquivo, '[^;]+');
               v_qtde_colunas_config  := REGEXP_COUNT(v_cabecalho_configurado, '[^;]+');
               
               IF v_qtde_colunas_arquivo <> v_qtde_colunas_config THEN
                  gerar_modelo;
                  RAISE_APPLICATION_ERROR(-c_sqlcode_cabecalho_invalido, TOOLS.FN_MULTIPLE_REPLACE(c_qtde_cabecalho_incorreta, '#INFORMADO='||v_qtde_colunas_arquivo||'/#ESPERADO='||v_qtde_colunas_config));               
               ELSE
                  v_posicao := 1;
                  LOOP
                     
                     v_coluna_arquivo := REPLACE(REGEXP_SUBSTR(v_cabecalho_do_arquivo, '[^;]*[;]+', 1, v_posicao), ';', NULL);
                     v_coluna_config  := REPLACE(REGEXP_SUBSTR(v_cabecalho_configurado, '[^;]*[;]+', 1, v_posicao), ';', NULL);
                     
                     IF nvl(v_coluna_arquivo, '******') <> nvl(v_coluna_config, '******') THEN 
                        gerar_modelo;
                        RAISE_APPLICATION_ERROR(-c_sqlcode_cabecalho_invalido, TOOLS.FN_MULTIPLE_REPLACE(c_cabecalho_invalido, '#NUMERO='||v_posicao||'/#INFORMADO="'||v_coluna_arquivo||'"/#ESPERADO="'||v_coluna_config||'"'));
                     END IF;
                     
                     v_posicao := v_posicao + 1;
                     EXIT WHEN v_posicao > v_qtde_colunas_arquivo;
                  END LOOP;
               END IF;            
            END IF;
         END IF;
      END IF;
      --------------------------------------------------------------------------
      
   EXCEPTION
      WHEN OTHERS THEN
         IF SQLCODE <> -c_sqlcode_cabecalho_invalido THEN
            UTL_FILE.FCLOSE_ALL;
            v_erro := SQLERRM;            
            RAISE_APPLICATION_ERROR(-c_sqlcode_arquivo_invalido, c_arquivo_invalido|| '----' ||v_erro);            
         ELSE
            RAISE;
         END IF;
   END;

   PROCEDURE SP_CRIAR_TABELA_EXTERNA(
      p_nome_arquivo   VARCHAR2 := NULL,
      p_diretorio      etl2.projeto.diretorio%TYPE
   ) AS
      v_declare_coluna CLOB;
      v_lista_coluna   CLOB;
      v_juncao_coluna  VARCHAR(50);
      v_juncao_coluna_char  VARCHAR(50);
      v_alias_tab_ext  etl2.interface_tabela_coluna.alias_coluna%TYPE;
      v_formato_arquivo VARCHAR2(5);
      v_directory_path VARCHAR2(4000);
      v_coluna_geometria VARCHAR2(30);
      
   BEGIN
   
      SELECT DIRECTORY_PATH INTO v_directory_path
      FROM sys.all_directories
      WHERE directory_name = p_diretorio;
   
      SELECT alias_tabela_externa, formato_arquivo INTO v_alias_tab_ext, v_formato_arquivo
      FROM etl2.vw_interface intf
      WHERE intf.cod_interface = vp_cod_interface;
      
      v_declare_coluna := '';
      v_lista_coluna   := '';
      v_juncao_coluna  := '';
      v_juncao_coluna_char := '';
      v_coluna_geometria := '';
      
      FOR v_coluna IN (
         SELECT *
         FROM etl2.vw_interface_coluna
         WHERE tabela_externa = 'S'
           AND cod_interface = vp_cod_interface
           AND coluna_calculada = 'N'
         ORDER BY posicao
      )
      LOOP
         v_declare_coluna := v_declare_coluna || v_juncao_coluna || '   "' || v_coluna.nome || '" ' || v_coluna.tipo_completo;
         v_lista_coluna   := v_lista_coluna || v_juncao_coluna_char || '         "' || v_coluna.nome || '" ';
         v_juncao_coluna_char  := ' CHAR(4000) TERMINATED BY ";" ,' || chr(13);
         v_juncao_coluna  := ' ,' || chr(13);
         IF v_coluna.tipo = 'SDO_GEOMETRY' THEN
            v_coluna_geometria := v_coluna.nome;
         END IF;
      END LOOP;
       
      IF v_declare_coluna IS NOT NULL THEN 
         IF v_formato_arquivo = c_formato_arquivo_csv THEN
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', 'CREATE TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'externa'));
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '(');
           -- sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   record_number NUMBER, ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA',     v_declare_coluna);
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', ')');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', 'ORGANIZATION EXTERNAL ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '( ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   TYPE ORACLE_LOADER ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   DEFAULT DIRECTORY '||p_diretorio||' ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   ACCESS PARAMETERS ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   ( ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      RECORDS DELIMITED BY NEWLINE ');
            IF vp_characterset IS NOT NULL THEN 
               sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      CHARACTERSET '''||vp_characterset||''' ');
            END IF;
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      STRING SIZES ARE IN CHARACTERS ');
            --sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      PREPROCESSOR "DIR_SCRIPTS": ''clean_etl_file.sh'' ' ); -- em testar
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      SKIP 1 ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      NOLOGFILE  ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      FIELDS  TERMINATED BY '';''  ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      MISSING FIELD VALUES ARE NULL  ');               
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      REJECT ROWS WITH ALL NULL FIELDS ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      ( ');
          --  sp_add_interface_ddl('CREATE_TAB_EXTERNA', '        record_number INTEGER TERMINATED BY ";", ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA',          v_lista_coluna||' CHAR(4000) TERMINATED BY ''\r''');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      ) ');      
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   ) ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   LOCATION ('''||p_nome_arquivo||''') ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', ') '); 
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', 'PARALLEL ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', 'REJECT LIMIT UNLIMITED', TRUE);
         ELSIF v_formato_arquivo = c_formato_arquivo_shp THEN

            sp_add_interface_ddl('CREATE_TAB_EXTERNA', 'DECLARE ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   v_result_shapefile VARCHAR2(4000); ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', 'BEGIN ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   v_result_shapefile := tools.fn_shapefile(UPPER(sys_context(''USERENV'', ''SERVER_HOST'')),'''||v_directory_path||'/'||p_nome_arquivo||''', ''etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'externa')||''', ''geom'');');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   IF INSTR(v_result_shapefile, ''Deu erro'') > 0 THEN ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '      RAISE_APPLICATION_ERROR('||-c_sqlcode_error_import_shp||', '''||c_error_import_shp||''');  ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '   END IF; ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', 'END; ');
            sp_add_interface_ddl('CREATE_TAB_EXTERNA', '/', true);            
         END IF;
         
         sp_add_interface_ddl('CREATE_TAB_EXTERNA_MAT', '');
         sp_add_interface_ddl('CREATE_TAB_EXTERNA_MAT', 'CREATE GLOBAL TEMPORARY TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'externa_materializada')||' ON COMMIT PRESERVE ROWS AS ');
         sp_add_interface_ddl('CREATE_TAB_EXTERNA_MAT', 'SELECT ');
         v_juncao_coluna  := '';
         FOR v_coluna_calculada IN (
            SELECT 
               CASE WHEN a.coluna_calculada ='S' 
               THEN b.texto_usar||' '||a.alias_coluna 
               ELSE v_alias_tab_ext||'.'||a.nome||' as '||a.alias_coluna
               END AS coluna
            FROM etl2.vw_interface_coluna a
               LEFT JOIN etl2.vw_transformacao b ON (a.cod_transformacao_calc = b.cod_transformacao)
            WHERE a.tabela_externa = 'S'
              AND a.cod_interface = vp_cod_interface
            ORDER BY a.cod_coluna
         )
         LOOP
            sp_add_interface_ddl('CREATE_TAB_EXTERNA_MAT', '   '|| v_juncao_coluna || ' ' || v_coluna_calculada.coluna);
            v_juncao_coluna  := chr(13) || '   ,';
         END LOOP;      
         sp_add_interface_ddl('CREATE_TAB_EXTERNA_MAT', 'FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'externa')||' '||v_alias_tab_ext, TRUE);
      END IF;     
   END;
   
   PROCEDURE SP_GERAR_VALIDACAO_AGENDAMENTO(p_validar_agendamento CHAR := 'S') AS 
   BEGIN 
      IF p_validar_agendamento = 'S' THEN 
         sp_add_interface_ddl('VALIDAR_AGENDAMENTO', 'BEGIN');
         sp_add_interface_ddl('VALIDAR_AGENDAMENTO', '   ETL2.PACK_ETL.SP_VALIDAR_AGENDAMENTO('||vp_cod_interface||', '||vp_cod_historico||');');
         sp_add_interface_ddl('VALIDAR_AGENDAMENTO', 'END;');
         sp_add_interface_ddl('VALIDAR_AGENDAMENTO', '/', TRUE);
      END IF;
   END;
   
   PROCEDURE SP_CRIAR_TABELA_FLUXO AS
      v_declare_coluna        CLOB;
      v_declare_coluna_origem CLOB;
      v_juncao_coluna         VARCHAR2(2);
      v_cmd_sql               CLOB;
   BEGIN
      v_declare_coluna := '';
      v_declare_coluna_origem := '';
      
      
      v_juncao_coluna  := '';
      -- Obtém a definição das colunas DESTINO de mapeamento
      FOR v_coluna_destino IN (
         SELECT *
         FROM etl2.vw_interface_mapeamento
         WHERE cod_interface = vp_cod_interface
         ORDER BY posicao
      )
      LOOP
         v_declare_coluna := v_declare_coluna || v_juncao_coluna || '   '||v_coluna_destino.nome || ' ' || v_coluna_destino.tipo_completo;
         v_juncao_coluna  := ',' || chr(13);
      END LOOP;
      
      v_juncao_coluna  := '';
      FOR v_coluna_origem IN (
         SELECT *
         FROM etl2.vw_interface_coluna
         WHERE tabela_externa = 'S'
           AND cod_interface = vp_cod_interface
           AND coluna_calculada = 'N'
         ORDER BY posicao
      ) 
      LOOP
         v_declare_coluna_origem := v_declare_coluna_origem || v_juncao_coluna || '   '||v_coluna_origem.nome || ' ' || v_coluna_origem.tipo_completo;
         v_juncao_coluna  := ',' || chr(13);
      END LOOP;
      
      sp_add_interface_ddl('CREATE_TAB_FLUXO', '');
      sp_add_interface_ddl('CREATE_TAB_FLUXO', 'CREATE GLOBAL TEMPORARY TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'fluxo'));
      sp_add_interface_ddl('CREATE_TAB_FLUXO', '(');
      sp_add_interface_ddl('CREATE_TAB_FLUXO', v_declare_coluna || ',');
      sp_add_interface_ddl('CREATE_TAB_FLUXO', '   COD_FLUXO NUMBER PRIMARY KEY,' );
      sp_add_interface_ddl('CREATE_TAB_FLUXO', '   IND_UPDATE CHAR(1)');
      sp_add_interface_ddl('CREATE_TAB_FLUXO', ') ON COMMIT PRESERVE ROWS ', TRUE);
      --sp_add_interface_ddl('CREATE_TAB_FLUXO', 'NOLOGGING', TRUE);
      
      sp_add_interface_ddl('CREATE_TAB_FLUXO_ERROR', 'BEGIN ');
      sp_add_interface_ddl('CREATE_TAB_FLUXO_ERROR', '  DBMS_ERRLOG.create_error_log (dml_table_name => ''etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'fluxo')||''', skip_unsupported => TRUE);');
      sp_add_interface_ddl('CREATE_TAB_FLUXO_ERROR', 'END; ');
      sp_add_interface_ddl('CREATE_TAB_FLUXO_ERROR', '/', TRUE);
      
      sp_add_interface_ddl('CREATE_TAB_ORIGEM', '');
      sp_add_interface_ddl('CREATE_TAB_ORIGEM', 'CREATE GLOBAL TEMPORARY TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'origem'));
      sp_add_interface_ddl('CREATE_TAB_ORIGEM', '(');
      sp_add_interface_ddl('CREATE_TAB_ORIGEM', v_declare_coluna_origem || ',');
      sp_add_interface_ddl('CREATE_TAB_ORIGEM', '   COD_FLUXO NUMBER, ' );
      sp_add_interface_ddl('CREATE_TAB_ORIGEM', '   COD_ORIGEM NUMBER PRIMARY KEY' );
      sp_add_interface_ddl('CREATE_TAB_ORIGEM', ') ON COMMIT PRESERVE ROWS ', TRUE);
      --sp_add_interface_ddl('CREATE_TAB_ORIGEM', 'NOLOGGING', TRUE);      
      
      sp_add_interface_ddl('CREATE_TAB_ORIGEM_ERROR', 'BEGIN ');
      sp_add_interface_ddl('CREATE_TAB_ORIGEM_ERROR', '  DBMS_ERRLOG.create_error_log (dml_table_name => ''etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'origem')||''', skip_unsupported => TRUE);');
      sp_add_interface_ddl('CREATE_TAB_ORIGEM_ERROR', 'END; ');
      sp_add_interface_ddl('CREATE_TAB_ORIGEM_ERROR', '/', TRUE);
      
      sp_add_interface_ddl('CREATE_TAB_IND_ORIGEM', 'CREATE INDEX etl2.idx_'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'origem')||'_fl ON etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'origem')||' (cod_fluxo)', TRUE);
   END;
   
   PROCEDURE SP_CRIAR_TABELA_ERRO AS 
      v_declare_coluna CLOB;
      v_juncao_coluna  VARCHAR2(2);
      v_cmd_sql        CLOB;
   BEGIN
      v_declare_coluna := '';
      v_juncao_coluna  := '';
      
      -- Obtém a definição das colunas DESTINO de mapeamento
      FOR v_coluna_destino IN (
         SELECT *
         FROM etl2.vw_interface_mapeamento
         WHERE cod_interface = vp_cod_interface
         ORDER BY posicao
      )
      LOOP
         v_declare_coluna := v_declare_coluna || v_juncao_coluna || '   '||v_coluna_destino.nome || ' ' || v_coluna_destino.tipo_completo;
         v_juncao_coluna  := ',' || chr(13);
      END LOOP;
      
      sp_add_interface_ddl('CREATE_TAB_ERRO', '');
      sp_add_interface_ddl('CREATE_TAB_ERRO', 'CREATE GLOBAL TEMPORARY TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
      sp_add_interface_ddl('CREATE_TAB_ERRO', '(' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', v_declare_coluna || ',' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   COD_FLUXO NUMBER, ' );
      --sp_add_interface_ddl('CREATE_TAB_ERRO', '      REFERENCES etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' (COD_FLUXO), ');
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   ETL_CHECK_DATE DATE,' );         
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   ETL_COD_TIPO_MENSAGEM NUMBER, ' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   ETL_VARIAVEIS_LOG VARCHAR2(4000),' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   ERRO_ORACLE NUMBER, ' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   MSG_ORACLE VARCHAR2(4000)' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', ') ON COMMIT PRESERVE ROWS ', TRUE );
      --sp_add_interface_ddl('CREATE_TAB_ERRO', 'NOLOGGING', TRUE);
      
      sp_add_interface_ddl('CREATE_TAB_IND_ERRO', 'CREATE INDEX etl2.idx_'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'erro')||'_fl ON etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'erro')||' (cod_fluxo)', TRUE);      
   END;
   
   PROCEDURE SP_CRIAR_TABELA_EXCEPTION AS 
   BEGIN
      sp_add_interface_ddl('CREATE_TAB_ERRO', '');
      sp_add_interface_ddl('CREATE_TAB_ERRO', 'CREATE GLOBAL TEMPORARY TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'exception'));
      sp_add_interface_ddl('CREATE_TAB_ERRO', '(' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   ROW_ID           UROWID, ' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   OWNER          VARCHAR2(30),' );         
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   TABLE_NAME     VARCHAR2(30), ' );
      sp_add_interface_ddl('CREATE_TAB_ERRO', '   CONSTRAINT_NAME VARCHAR2(30)' );      
      sp_add_interface_ddl('CREATE_TAB_ERRO', ') ON COMMIT PRESERVE ROWS ', TRUE );
   END;
   
   PROCEDURE SP_INSERIR_FLUXO AS
      v_cmd_sql            CLOB := NULL;
      v_clausula_select    CLOB := NULL;
      v_clausula_from      CLOB := NULL;
      v_filtro_exists      CLOB := NULL;
      v_lista_colunas      CLOB := NULL;
      v_lista_colunas_2    CLOB := NULL;
      v_lista_colunas_minus    CLOB := NULL;
      v_lista_colunas_alias CLOB := NULL;
      v_chave_atualizacao  CLOB := NULL;
      
      v_juncao_boolean      VARCHAR2(50) := NULL;
      v_juncao_virgula      VARCHAR2(50) := NULL;
      v_juncao_virgula_2    VARCHAR2(50) := NULL;
      v_juncao_virgula_3    VARCHAR2(50) := NULL;
      v_juncao_virgula_4    VARCHAR2(50) := NULL;
      v_juncao_virgula_5    VARCHAR2(50) := NULL;
      v_tabela_alvo         VARCHAR2(50) := NULL;
      v_nome_tabela_alvo    VARCHAR2(50) := NULL;
      v_estrategia_deteccao VARCHAR2(50) := NULL;
      v_esquema_tabela_alvo VARCHAR2(50) := NULL;
      v_somente_distintos etl2.interface_importacao.somente_distintos%TYPE := NULL;
      
      v_alias_tabela_externa etl2.interface_tabela.alias_tabela%TYPE := NULL;
      
      v_coluna_virgula     CLOB;
      v_coluna_virgula_src CLOB;
      v_coluna_tab_src     CLOB;
      v_count              NUMBER;
      
      v_nome_tabela_real  VARCHAR2(50);

   BEGIN
      -- Define colunas de entrada (tabela externa)
      SELECT listagg(a.nome, ',' || chr(13) || '   ') within group (order by a.cod_coluna),
             listagg('SRC_'||a.alias_coluna, ',' || chr(13) || '   ') within group (order by a.cod_coluna),
             listagg(b.alias_tabela_externa||'.'||a.alias_coluna||' AS SRC_'||a.alias_coluna, ',' || chr(13) || '      ' ) within group (order by a.cod_coluna)
      INTO v_coluna_virgula, v_coluna_virgula_src, v_coluna_tab_src
      FROM etl2.vw_interface_coluna a
           JOIN etl2.vw_interface b ON (a.cod_interface = b.cod_interface) 
      WHERE a.tabela_externa = 'S'
        AND a.coluna_calculada = 'N'
        AND a.cod_interface = vp_cod_interface;

   
      -- Define colunas da cláusula SELECT (ALVO)
      FOR v_coluna IN (
         SELECT 
            map.*
         FROM etl2.vw_interface_mapeamento map
         WHERE map.cod_interface = vp_cod_interface
         ORDER BY map.posicao
      )
      LOOP
         IF v_coluna.esquema_execucao = 'T' THEN
            v_lista_colunas       := v_lista_colunas || v_juncao_virgula_2 || v_coluna.nome;
            v_lista_colunas_alias := v_lista_colunas_alias || v_juncao_virgula_2 || v_coluna.alias_coluna_alvo;
            v_lista_colunas_2     := v_lista_colunas_2 || v_juncao_virgula_3 || v_coluna.alias_coluna_alvo;
            v_clausula_select     := v_clausula_select || v_juncao_virgula || v_coluna.transformacao || ' AS ' || v_coluna.alias_coluna_alvo;
            
            IF v_coluna.define_atualizacao_alvo = 'S' THEN 
               v_filtro_exists       := v_filtro_exists || v_juncao_boolean || '(';
               v_filtro_exists       := v_filtro_exists || v_coluna.comparacao_tgt_src ;
               v_filtro_exists       := v_filtro_exists || ')';
               v_juncao_boolean    := chr(13) || '         AND ';
               v_lista_colunas_minus     := v_lista_colunas_minus || v_juncao_virgula_5 || 'a.'||v_coluna.alias_coluna_alvo;
               v_juncao_virgula_5  := ',';   
            END IF;
            
            IF v_coluna.chave_atualizacao = 'S' THEN
               v_chave_atualizacao := v_chave_atualizacao || v_juncao_virgula_4 || v_coluna.alias_coluna_alvo;
               v_juncao_virgula_4  := ',';   
            END IF;

            v_juncao_virgula    := ',' || chr(13) || '      ';
            v_juncao_virgula_2  := ',' || chr(13) || '   ';
            v_juncao_virgula_3  := ',';            
         END IF;
      END LOOP; 
      
      -- Define tabelas da cláusula FROM e os JOINs
      FOR x IN (
         SELECT a.*
         FROM etl2.vw_interface_tabela_join a
         WHERE a.cod_interface = vp_cod_interface
         ORDER BY a.posicao
      )
      LOOP
         IF x.tabela_externa = 'S' THEN
            v_nome_tabela_real := 'etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'externa_materializada');
         ELSE
            v_nome_tabela_real := x.esquema||'.'||x.nome;
         END IF;
      
         IF x.texto_join IS NOT NULL THEN
            v_clausula_from := v_clausula_from || x.prefixo_join || ' JOIN' || ' ';
         END IF;         
         
         v_clausula_from := v_clausula_from || v_nome_tabela_real || ' ' || x.alias_tabela;
         
         IF x.tabela_externa = 'S' THEN
            v_alias_tabela_externa := x.alias_tabela;
         END IF;

         IF x.texto_join IS NOT NULL THEN
            v_clausula_from := v_clausula_from || ' ON ('|| x.texto_join || ')';
         END IF;
         
         v_clausula_from := v_clausula_from || chr(13) || '        ';        
      END LOOP;
       
      -- Buscar definições da interface
      SELECT somente_distintos, esquema_alvo||'.'||tabela_alvo, esquema_alvo, tabela_alvo, estrategia_deteccao
      INTO v_somente_distintos, v_tabela_alvo, v_esquema_tabela_alvo, v_nome_tabela_alvo, v_estrategia_deteccao
      FROM etl2.vw_interface
      WHERE cod_interface = vp_cod_interface;
      
      -- Criar índice na tabela de fluxo
      IF v_chave_atualizacao IS NOT NULL THEN 
         sp_add_interface_ddl('CREATE_INDEX','');
         sp_add_interface_ddl('CREATE_INDEX','-- Criar índice na tabela de fluxo' );
         sp_add_interface_ddl('CREATE_INDEX','CREATE INDEX etl2.idx_'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' on etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' ('||v_chave_atualizacao||') ', TRUE);
      END IF;
      

       --Coletar estatísticas
      sp_add_interface_ddl('COLETAR_ESTAT','');
      sp_add_interface_ddl('COLETAR_ESTAT','--Coletar estatísticas');
      sp_add_interface_ddl('COLETAR_ESTAT','BEGIN ');
      sp_add_interface_ddl('COLETAR_ESTAT','   dbms_stats.gather_table_stats( ');
      sp_add_interface_ddl('COLETAR_ESTAT','      ownname => '''||v_esquema_tabela_alvo||''', ' );
      sp_add_interface_ddl('COLETAR_ESTAT','      tabname => '''||v_nome_tabela_alvo||''', ' );
      sp_add_interface_ddl('COLETAR_ESTAT','      estimate_percent => dbms_stats.auto_sample_size ' );
      sp_add_interface_ddl('COLETAR_ESTAT','   ); ' );
      sp_add_interface_ddl('COLETAR_ESTAT','END; ' );
      sp_add_interface_ddl('COLETAR_ESTAT','/', TRUE);

      sp_add_interface_ddl('INSERT_FLUXO', '--Inserir registros novos/modificados na tabela de fluxo');
      sp_add_interface_ddl('INSERT_FLUXO', 'INSERT /*+ append */ ALL');
      IF v_somente_distintos = 'S' THEN 
         sp_add_interface_ddl('INSERT_FLUXO', 'WHEN REPETICAO = 1 THEN ');
      ELSE
         sp_add_interface_ddl('INSERT_FLUXO', 'WHEN 1 = 1 THEN ');
      END IF;
      sp_add_interface_ddl('INSERT_FLUXO', 'INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo'));
      sp_add_interface_ddl('INSERT_FLUXO', '( ');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || v_lista_colunas || ',');      
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'COD_FLUXO,');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'IND_UPDATE');
      sp_add_interface_ddl('INSERT_FLUXO', ') ');
      sp_add_interface_ddl('INSERT_FLUXO', 'VALUES ( ');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || v_lista_colunas_alias || ',');      
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'RANKING, ');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'IND_UPDATE');
      sp_add_interface_ddl('INSERT_FLUXO', ') ');
      sp_add_interface_ddl('INSERT_FLUXO', 'LOG ERRORS INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo_erro')||' (''Erro de i$'') REJECT LIMIT UNLIMITED ');
      sp_add_interface_ddl('INSERT_FLUXO', 'WHEN 1 = 1 THEN');
      sp_add_interface_ddl('INSERT_FLUXO', 'INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'origem'));
      sp_add_interface_ddl('INSERT_FLUXO', '( ');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || v_coluna_virgula || ',');      
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'COD_FLUXO,');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'COD_ORIGEM');
      sp_add_interface_ddl('INSERT_FLUXO', ') ');
      sp_add_interface_ddl('INSERT_FLUXO', 'VALUES ( ');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || v_coluna_virgula_src || ',');  
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'RANKING, ');
      sp_add_interface_ddl('INSERT_FLUXO', '   ' || 'ORDEM_SRC');
      sp_add_interface_ddl('INSERT_FLUXO', ') ');
      sp_add_interface_ddl('INSERT_FLUXO', 'LOG ERRORS INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'origem_erro')||' (''Erro de s$'') REJECT LIMIT UNLIMITED ');
      sp_add_interface_ddl('INSERT_FLUXO', 'SELECT S.*, '); 
      sp_add_interface_ddl('INSERT_FLUXO', '       ''I'' IND_UPDATE, ');
      IF v_somente_distintos = 'S' THEN 
         sp_add_interface_ddl('INSERT_FLUXO', '       row_number () over (partition by '||v_lista_colunas_2||' ORDER BY '||v_lista_colunas_2||') AS REPETICAO, ');
         sp_add_interface_ddl('INSERT_FLUXO', '       dense_rank() over (order by '||v_lista_colunas_2||') ranking, ');
      ELSE
         sp_add_interface_ddl('INSERT_FLUXO', '       rownum ranking, ');
      END IF;
      sp_add_interface_ddl('INSERT_FLUXO', '       rownum ordem_src ');
      sp_add_interface_ddl('INSERT_FLUXO', 'FROM (');
      sp_add_interface_ddl('INSERT_FLUXO', '   SELECT ');
      sp_add_interface_ddl('INSERT_FLUXO', '      ' || v_clausula_select||',' );
      sp_add_interface_ddl('INSERT_FLUXO', '      ' || v_coluna_tab_src );
      sp_add_interface_ddl('INSERT_FLUXO', '   FROM '|| v_clausula_from);
      sp_add_interface_ddl('INSERT_FLUXO', ') S ', TRUE);
      /*sp_add_interface_ddl('INSERT_FLUXO', 'WHERE NOT EXISTS (');
      sp_add_interface_ddl('INSERT_FLUXO', '   SELECT 1 FROM ' || v_tabela_alvo || ' T ');
      sp_add_interface_ddl('INSERT_FLUXO', '   WHERE ');
      sp_add_interface_ddl('INSERT_FLUXO', v_filtro_exists);
      sp_add_interface_ddl('INSERT_FLUXO', ')', TRUE);*/
      
      /*sp_add_interface_ddl('CREATE_FK', '');
      sp_add_interface_ddl('CREATE_FK','ALTER TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'origem'));
      sp_add_interface_ddl('CREATE_FK','ADD CONSTRAINT FK_'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'origem')||' FOREIGN KEY (COD_FLUXO) ');
      sp_add_interface_ddl('CREATE_FK','REFERENCES etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' (COD_FLUXO) ', TRUE);*/

      --Coletar estatísticas
      sp_add_interface_ddl('COLETAR_ESTAT','');
      sp_add_interface_ddl('COLETAR_ESTAT','--Coletar estatísticas');
      sp_add_interface_ddl('COLETAR_ESTAT','BEGIN ');
      sp_add_interface_ddl('COLETAR_ESTAT','   dbms_stats.gather_table_stats( ');
      sp_add_interface_ddl('COLETAR_ESTAT','      ownname => ''ETL2'', ' );
      sp_add_interface_ddl('COLETAR_ESTAT','      tabname => '''||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||''', ' );
      sp_add_interface_ddl('COLETAR_ESTAT','      estimate_percent => dbms_stats.auto_sample_size ' );
      sp_add_interface_ddl('COLETAR_ESTAT','   ); ' );
      sp_add_interface_ddl('COLETAR_ESTAT','END; ' );
      sp_add_interface_ddl('COLETAR_ESTAT','/', TRUE);
      
      -- Marcar registros para atualizar
      SELECT count(*) INTO v_count  
      FROM etl2.vw_interface_mapeamento map
      WHERE map.cod_interface = vp_cod_interface
        AND map.participa_atualizacao = 'S';
      
      IF v_estrategia_deteccao = c_estrat_det_exists THEN
         sp_add_interface_ddl('DELETE_IGNORADOS','');
         sp_add_interface_ddl('DELETE_IGNORADOS','-- Exclui registros (configurado para usar EXISTS)' );
         sp_add_interface_ddl('DELETE_IGNORADOS','DELETE FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' s ');
         sp_add_interface_ddl('DELETE_IGNORADOS','WHERE EXISTS (');
         sp_add_interface_ddl('DELETE_IGNORADOS','   SELECT 1 FROM ' || v_tabela_alvo || ' T ');
         sp_add_interface_ddl('DELETE_IGNORADOS','   WHERE ');
         sp_add_interface_ddl('DELETE_IGNORADOS', v_filtro_exists);
         sp_add_interface_ddl('DELETE_IGNORADOS',') ', TRUE);
      ELSIF v_estrategia_deteccao = c_estrat_det_minus THEN 
         sp_add_interface_ddl('DELETE_IGNORADOS','');
         sp_add_interface_ddl('DELETE_IGNORADOS','-- Exclui registros (configurado para usar MINUS / DELETE da intersecção)' );
         sp_add_interface_ddl('DELETE_IGNORADOS','DELETE FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' s ');
         sp_add_interface_ddl('DELETE_IGNORADOS','WHERE ( '||v_chave_atualizacao||' ) IN ( ');
         sp_add_interface_ddl('DELETE_IGNORADOS','   SELECT '||v_chave_atualizacao||' FROM ( ');
         sp_add_interface_ddl('DELETE_IGNORADOS','      SELECT '||v_lista_colunas_minus||' FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' a ');
         sp_add_interface_ddl('DELETE_IGNORADOS','      INTERSECT ');
         sp_add_interface_ddl('DELETE_IGNORADOS','      SELECT '||v_lista_colunas_minus||' FROM '||v_tabela_alvo||' a ');
         sp_add_interface_ddl('DELETE_IGNORADOS','   ) ');
         sp_add_interface_ddl('DELETE_IGNORADOS',') ', TRUE);
      END IF;
      
      IF v_count > 0 AND v_chave_atualizacao IS NOT NULL THEN 
         sp_add_interface_ddl('MARCAR_UPDATE','');
         sp_add_interface_ddl('MARCAR_UPDATE','-- Marcar registros para atualizar' );
         sp_add_interface_ddl('MARCAR_UPDATE','UPDATE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo'));
         sp_add_interface_ddl('MARCAR_UPDATE','SET ind_update = ''U'' ');
         sp_add_interface_ddl('MARCAR_UPDATE','WHERE ( '||v_chave_atualizacao||' )');
         sp_add_interface_ddl('MARCAR_UPDATE','   IN ( ');
         sp_add_interface_ddl('MARCAR_UPDATE','   SELECT '||v_chave_atualizacao||' ');
         sp_add_interface_ddl('MARCAR_UPDATE','   FROM '||v_tabela_alvo||' ');
         sp_add_interface_ddl('MARCAR_UPDATE','   ) ', TRUE);
      END IF;
   END;
   
   PROCEDURE SP_VERIFICAR_RESTRICOES AS

      v_juncao_coluna  VARCHAR2(2);
      v_declare_coluna CLOB;
      v_declare_col_erro CLOB;
      v_error_type VARCHAR2(2);
      v_mensagem clob;
   BEGIN

      v_declare_coluna := '';
      
      FOR v_coluna_destino IN (
         SELECT *
         FROM etl2.vw_interface_mapeamento
         WHERE cod_interface = vp_cod_interface
         ORDER BY posicao
      )
      LOOP
         v_declare_coluna := v_declare_coluna || v_juncao_coluna || '   '||v_coluna_destino.nome;
         v_juncao_coluna  := ',' || chr(13);
      END LOOP;
      
      
      -- Validar chaves
      FOR v_chave IN (
         SELECT 'PK' tipo_chave,
             x.*
         FROM (
            SELECT restricao,
                   listagg(coluna_destino, ',') within group (order by posicao) coluna_destino_msg,
                   listagg('sub.'||coluna_destino, ',') within group (order by posicao) coluna_destino,
                   listagg('sub.'||coluna_destino||' = a.'||coluna_destino, ' AND ') within group (order by posicao) texto_join,
                   NULL as colunas_externas
            FROM etl2.vw_interface_pk 
            WHERE cod_interface = vp_cod_interface
              AND esquema_execucao = 'T'
            GROUP BY restricao
         ) x
            
         UNION ALL
            
            SELECT 'UN' tipo_chave,
                    null restricao,
                   x.*
            FROM (
               SELECT 
                  listagg(a.alias_coluna_alvo, ',') within group (order by a.posicao) coluna_destino_msg,
                  listagg('sub.'||a.alias_coluna_alvo, ',') within group (order by a.posicao) coluna_destino,
                  listagg('((sub.'||a.alias_coluna_alvo||' = a.'||a.alias_coluna_alvo||') '|| case when a.obrigatorio = 'N' then 'OR (sub.'||a.alias_coluna_alvo||' IS NULL AND a.'||a.alias_coluna_alvo||' IS NULL)' end||' )', ' AND ') within group (order by a.posicao) texto_join,
                  max(b.colunas_externas) colunas_externas
               FROM ETL2.vw_interface_mapeamento a
                  JOIN ETL2.vw_transformacao b on (a.cod_transformacao = b.cod_transformacao)
               WHERE a.chave_atualizacao = 'S' 
                 AND a.cod_interface = vp_cod_interface
            ) x
      )
      LOOP
         IF v_chave.coluna_destino IS NOT NULL THEN 
            sp_add_interface_ddl('INSERT_ERRO','-- Validar chave ('||v_chave.tipo_chave||')');
            sp_add_interface_ddl('INSERT_ERRO','INSERT INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
            sp_add_interface_ddl('INSERT_ERRO','(');
            sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna||',');
            sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
            sp_add_interface_ddl('INSERT_ERRO','   ETL_CHECK_DATE,');
            sp_add_interface_ddl('INSERT_ERRO','   ETL_COD_TIPO_MENSAGEM,');
            sp_add_interface_ddl('INSERT_ERRO','   ETL_VARIAVEIS_LOG ');
            sp_add_interface_ddl('INSERT_ERRO',')');
            sp_add_interface_ddl('INSERT_ERRO','SELECT  ');
            sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna||',');
            sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
            sp_add_interface_ddl('INSERT_ERRO','   sysdate,');
            IF v_chave.tipo_chave = 'PK' THEN 
               sp_add_interface_ddl('INSERT_ERRO','   (SELECT cod_tipo_mensagem FROM etl2.log_tipo_mensagem WHERE nome = '''||c_tp_msg_pk||'''),');
               sp_add_interface_ddl('INSERT_ERRO','   ''#RESTRICAO='||v_chave.restricao||'/#EXT='||v_chave.colunas_externas||''' ');
            ELSE
               sp_add_interface_ddl('INSERT_ERRO','   (SELECT cod_tipo_mensagem FROM etl2.log_tipo_mensagem WHERE nome = '''||c_tp_msg_un||'''),');
               sp_add_interface_ddl('INSERT_ERRO','   ''#CHAVE='||v_chave.coluna_destino_msg||'/#EXT='||v_chave.colunas_externas||''' ');
            END IF;
         
            sp_add_interface_ddl('INSERT_ERRO','FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' a ');
            sp_add_interface_ddl('INSERT_ERRO','WHERE  EXISTS  ( ');
            sp_add_interface_ddl('INSERT_ERRO','   SELECT '||v_chave.coluna_destino||' ');
            sp_add_interface_ddl('INSERT_ERRO','   FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' sub ');
            sp_add_interface_ddl('INSERT_ERRO','   WHERE '||v_chave.texto_join||' ');
            sp_add_interface_ddl('INSERT_ERRO','   GROUP BY '||v_chave.coluna_destino||' ');
            sp_add_interface_ddl('INSERT_ERRO','   HAVING COUNT(1) > 1 ');
            sp_add_interface_ddl('INSERT_ERRO',') ', TRUE);
         END IF;
      END LOOP;

      -- Validar chaves estrangeiras
      FOR v_fk IN (
         SELECT * 
         FROM etl2.vw_interface_fk 
         WHERE cod_interface = vp_cod_interface
      )
      LOOP
         sp_add_interface_ddl('INSERT_ERRO','');
         sp_add_interface_ddl('INSERT_ERRO','-- Validar chave estrangeira (FK)' );
         sp_add_interface_ddl('INSERT_ERRO','INSERT INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
         sp_add_interface_ddl('INSERT_ERRO','(');
         sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna||',');
         sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_CHECK_DATE,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_COD_TIPO_MENSAGEM,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_VARIAVEIS_LOG');
         sp_add_interface_ddl('INSERT_ERRO',')');
         sp_add_interface_ddl('INSERT_ERRO','SELECT  ');
         sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna||',');
         sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
         sp_add_interface_ddl('INSERT_ERRO','   sysdate,');         
         sp_add_interface_ddl('INSERT_ERRO','   (SELECT cod_tipo_mensagem FROM etl2.log_tipo_mensagem WHERE nome = '''||c_tp_msg_fk||'''),');
         sp_add_interface_ddl('INSERT_ERRO','   ''#COLUNA='||v_fk.coluna||'/#RESTRICAO='||v_fk.restricao||'/#TABELA_REF='||v_fk.tabela_ref||'/#EXT='||v_fk.colunas_externas||''' ');
         sp_add_interface_ddl('INSERT_ERRO','FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' a ');
         sp_add_interface_ddl('INSERT_ERRO','WHERE  '||v_fk.sql_where , TRUE);
      END LOOP;  

      -- Validar arquivos shape
      FOR v_intf_col IN (
         SELECT b.alias_coluna_alvo, B.COLUNAS_EXTERNAS
         FROM etl2.vw_interface a
         JOIN etl2.vw_interface_mapeamento b ON (a.cod_interface = b.cod_interface)
         WHERE a.cod_interface = vp_cod_interface and FORMATO_ARQUIVO = 'SHP' and tipo_completo = 'SDO_GEOMETRY'
      )
      LOOP
         sp_add_interface_ddl('INSERT_ERRO','');
         sp_add_interface_ddl('INSERT_ERRO','-- Validar arquivos shape' );
         sp_add_interface_ddl('INSERT_ERRO','INSERT INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
         sp_add_interface_ddl('INSERT_ERRO','(');
         sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna||',');
         sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_CHECK_DATE,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_COD_TIPO_MENSAGEM,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_VARIAVEIS_LOG');
         sp_add_interface_ddl('INSERT_ERRO',')');
         sp_add_interface_ddl('INSERT_ERRO','SELECT  ');
         sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna||',');
         sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
         sp_add_interface_ddl('INSERT_ERRO','   sysdate,');         
         sp_add_interface_ddl('INSERT_ERRO','   (SELECT cod_tipo_mensagem FROM etl2.log_tipo_mensagem WHERE nome = '''||c_tp_msg_shp||'''),');
         sp_add_interface_ddl('INSERT_ERRO','   ''#COLUNA='||v_intf_col.alias_coluna_alvo||''' ');
         sp_add_interface_ddl('INSERT_ERRO','FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' a ');
         sp_add_interface_ddl('INSERT_ERRO','WHERE sdo_geom.validate_geometry_with_context('||v_intf_col.alias_coluna_alvo||', 0.05) <> ''TRUE'' ', TRUE);
      END LOOP;
      
      -- Validar constraints de Check 
      FOR v_ck IN (
         SELECT * 
         FROM etl2.vw_interface_ck
         WHERE cod_interface = vp_cod_interface
      )
      LOOP
         sp_add_interface_ddl('INSERT_ERRO','');
         sp_add_interface_ddl('INSERT_ERRO','-- Validar check e not null');
         sp_add_interface_ddl('INSERT_ERRO','INSERT INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
         sp_add_interface_ddl('INSERT_ERRO','(');
       sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna ||',');
         sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_CHECK_DATE,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_COD_TIPO_MENSAGEM,');
         sp_add_interface_ddl('INSERT_ERRO','   ETL_VARIAVEIS_LOG');
         sp_add_interface_ddl('INSERT_ERRO',')');
         sp_add_interface_ddl('INSERT_ERRO','SELECT  ');
         sp_add_interface_ddl('INSERT_ERRO',v_declare_coluna ||',');
         sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
         sp_add_interface_ddl('INSERT_ERRO','   sysdate,');
         
         IF v_ck.condicao = '"'||v_ck.coluna||'" IS NOT NULL' THEN 
            
            sp_add_interface_ddl('INSERT_ERRO','   (SELECT cod_tipo_mensagem FROM etl2.log_tipo_mensagem WHERE nome = '''||c_tp_msg_nn||'''),');
            sp_add_interface_ddl('INSERT_ERRO','   ''#COLUNA='||v_ck.coluna||'/#EXT='||v_ck.colunas_externas||''' ');
         ELSE
            sp_add_interface_ddl('INSERT_ERRO','   (SELECT cod_tipo_mensagem FROM etl2.log_tipo_mensagem WHERE nome = '''||c_tp_msg_ck||'''),');
            sp_add_interface_ddl('INSERT_ERRO','   ''#COLUNA='||v_ck.coluna||'/#COMENTARIO='||NVL(v_ck.comentarios,'**')||'/#EXT='||v_ck.colunas_externas||''' ');
         END IF;
         
         sp_add_interface_ddl('INSERT_ERRO','FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' a ');
         sp_add_interface_ddl('INSERT_ERRO','WHERE  NOT('||regexp_replace(v_ck.condicao, '(")?'||v_ck.coluna||'(")?', 'a.'||v_ck.coluna)||')', TRUE);
         
      END LOOP;
      
      sp_add_interface_ddl('CHECK_ERRO_FLUXO','BEGIN  ');
      sp_add_interface_ddl('CHECK_ERRO_FLUXO','   etl2.pack_etl.sp_checar_tipo_invalido(  ');
      sp_add_interface_ddl('CHECK_ERRO_FLUXO','      p_cod_interface => '||vp_cod_interface||', ');
      sp_add_interface_ddl('CHECK_ERRO_FLUXO','      p_cod_historico => '||vp_cod_historico||' ');
      sp_add_interface_ddl('CHECK_ERRO_FLUXO','   ); ');
      sp_add_interface_ddl('CHECK_ERRO_FLUXO','END; ');
      sp_add_interface_ddl('CHECK_ERRO_FLUXO','/', TRUE);
  
      -- Inserção dos erros da tabela de fluxo (erro ora)  
      sp_add_interface_ddl('INSERT_ERRO','');
      sp_add_interface_ddl('INSERT_ERRO','-- Inserção dos erros da tabela de fluxo (erro ora)');
      sp_add_interface_ddl('INSERT_ERRO','INSERT INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
      sp_add_interface_ddl('INSERT_ERRO','(');
      sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
      sp_add_interface_ddl('INSERT_ERRO','   ETL_CHECK_DATE,');
      sp_add_interface_ddl('INSERT_ERRO','   ETL_COD_TIPO_MENSAGEM,');
      sp_add_interface_ddl('INSERT_ERRO','   ETL_VARIAVEIS_LOG,');
      sp_add_interface_ddl('INSERT_ERRO','   ERRO_ORACLE, ');
      sp_add_interface_ddl('INSERT_ERRO','   MSG_ORACLE ');
      sp_add_interface_ddl('INSERT_ERRO',')');
      sp_add_interface_ddl('INSERT_ERRO','SELECT ');
      sp_add_interface_ddl('INSERT_ERRO','   c.cod_fluxo, ');
      sp_add_interface_ddl('INSERT_ERRO','   sysdate, ');
      sp_add_interface_ddl('INSERT_ERRO','   e.cod_tipo_mensagem, ');
      sp_add_interface_ddl('INSERT_ERRO','   ''#COLUNA=''||d.nome||''/#SQLERRM=''||c.ora_err_mesg$||''/#EXT=''||d.colunas_externas||''/#TAMANHO=''||a.tamanho||''/#PRECISAO=''||a.precisao,  ');
      sp_add_interface_ddl('INSERT_ERRO','   c.ora_err_number$, ');
      sp_add_interface_ddl('INSERT_ERRO','   c.ora_err_mesg$ ');
      sp_add_interface_ddl('INSERT_ERRO','FROM etl2.vw_interface_coluna a ');
      sp_add_interface_ddl('INSERT_ERRO','   join etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'exception')||' b on (a.cod_coluna = regexp_substr(b.constraint_name, ''[0-9]+$'')) ');
      sp_add_interface_ddl('INSERT_ERRO','   join ETL2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo_erro')||' c on (c.rowid = b.row_id) ');
      sp_add_interface_ddl('INSERT_ERRO','   join etl2.vw_interface_mapeamento d on (a.cod_coluna = d.cod_coluna) ');
      sp_add_interface_ddl('INSERT_ERRO','   JOIN etl2.log_tipo_sqlcode e ON (e.ora_sqlcode = c.ora_err_number$)', TRUE);      
      
      -- Inserção dos erros da tabela de fluxo (erro ora não detectado / faltando configuração) 
      sp_add_interface_ddl('INSERT_ERRO','');
      sp_add_interface_ddl('INSERT_ERRO','-- Inserção dos erros da tabela de fluxo (erro ora)');
      sp_add_interface_ddl('INSERT_ERRO','INSERT INTO etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
      sp_add_interface_ddl('INSERT_ERRO','(');
      sp_add_interface_ddl('INSERT_ERRO','   COD_FLUXO,');
      sp_add_interface_ddl('INSERT_ERRO','   ETL_CHECK_DATE,');
      sp_add_interface_ddl('INSERT_ERRO','   ETL_COD_TIPO_MENSAGEM,');
      sp_add_interface_ddl('INSERT_ERRO','   ETL_VARIAVEIS_LOG,');
      sp_add_interface_ddl('INSERT_ERRO','   ERRO_ORACLE, ');
      sp_add_interface_ddl('INSERT_ERRO','   MSG_ORACLE ');
      sp_add_interface_ddl('INSERT_ERRO',')');
      sp_add_interface_ddl('INSERT_ERRO','SELECT ');
      sp_add_interface_ddl('INSERT_ERRO','   a.cod_fluxo, ');
      sp_add_interface_ddl('INSERT_ERRO','   sysdate, ');
      sp_add_interface_ddl('INSERT_ERRO','   (SELECT cod_tipo_mensagem FROM etl2.log_tipo_mensagem WHERE nome = '''||c_tp_msg_dado_incorreto||'''), ');
      sp_add_interface_ddl('INSERT_ERRO','   NULL,  ');
      sp_add_interface_ddl('INSERT_ERRO','   a.ora_err_number$, ');
      sp_add_interface_ddl('INSERT_ERRO','   a.ora_err_mesg$ ');
      sp_add_interface_ddl('INSERT_ERRO','FROM ETL2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo_erro')||' a ');
      sp_add_interface_ddl('INSERT_ERRO','   LEFT JOIN etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'exception')||' b on (a.rowid = b.row_id) ');
      sp_add_interface_ddl('INSERT_ERRO','WHERE b.row_id IS NULL ', TRUE);
   END;
   
   PROCEDURE SP_ENRIQUECER_INTERFACE (
      p_cod_interface etl2.interface.cod_interface%TYPE,
      p_cod_historico  etl2.historico.cod_historico%TYPE
   ) AS
      v_execute_procedure CLOB;
      v_erro CLOB;
      v_interface etl2.pack_etl.rec_interface_info;
   BEGIN
      FOR x IN (
         SELECT * 
         FROM ETL2.vw_interface_enriquecimento
         WHERE cod_interface = p_cod_interface
      )
      LOOP
         v_execute_procedure := '';
         
         v_execute_procedure := v_execute_procedure || 'DECLARE ';
         v_execute_procedure := v_execute_procedure || '   v_interface etl2.pack_etl.rec_interface_info; ';
         v_execute_procedure := v_execute_procedure || 'BEGIN ';
         v_execute_procedure := v_execute_procedure || '   v_interface.cod_interface := :1; ';
         v_execute_procedure := v_execute_procedure || '   v_interface.cod_historico := :2; ';
         v_execute_procedure := v_execute_procedure || ''||x.esquema||'.'||x.nome_procedimento||'(v_interface); ';
         v_execute_procedure := v_execute_procedure || 'END; ';
         
         --dbms_output.put_line('---------'||v_execute_procedure);
         
         BEGIN
            EXECUTE IMMEDIATE v_execute_procedure 
            USING 
               p_cod_interface, 
               p_cod_historico;
         EXCEPTION 
            WHEN OTHERS THEN
               v_erro := SQLERRM;
               RAISE_APPLICATION_ERROR(-c_sqlcode_enriquecimento, TOOLS.FN_MULTIPLE_REPLACE(c_enriquecimento, '#ENRIQ='||x.nome||'/#ERRO='||v_erro));
         END;
      END LOOP;
   END;
   
   PROCEDURE SP_CHECAR_TIPO_INVALIDO (
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_cod_historico  etl2.historico.cod_historico%TYPE
   ) AS
      v_query_log CLOB;
      v_query_error CLOB;
      v_cursor_log t_ref_cursor;
      v_cursor_error t_ref_cursor;
      v_flow_error_table_name VARCHAR2(30);
      v_exception_table_name VARCHAR2(30);
      
      v_filtro_validacao VARCHAR2(400);
      v_expr_regular_validacao VARCHAR2(4000);
      v_cod_tipo_mensagem NUMBER;
      v_exec_sql CLOB;
   BEGIN
      v_flow_error_table_name := fn_get_nome_tabela(p_cod_interface, p_cod_historico,'fluxo_erro');
      v_exception_table_name  := fn_get_nome_tabela(p_cod_interface, p_cod_historico,'exception');
      
      -- Query busca todos os tipos de erro cadastrados (somente com erro ORA_SQLCODE)
      v_query_log := '';
      v_query_log := v_query_log || 'SELECT DISTINCT a.cod_tipo_mensagem, a.expr_regular_validacao, a.filtro_validacao ';
      v_query_log := v_query_log || 'FROM etl2.log_tipo_mensagem a ';
      v_query_log := v_query_log || 'JOIN etl2.log_tipo_sqlcode c on (a.cod_tipo_mensagem = c.cod_tipo_mensagem) ';      
      v_query_log := v_query_log || 'JOIN etl2.'||v_flow_error_table_name||' b ON (b.ora_err_number$ = c.ora_sqlcode) ' ;
      
      
      dbms_output.put_line(v_query_log);
      
      OPEN v_cursor_log FOR v_query_log;
      LOOP
         FETCH v_cursor_log INTO v_cod_tipo_mensagem, v_expr_regular_validacao, v_filtro_validacao;
         v_expr_regular_validacao := REPLACE(v_expr_regular_validacao , '''', '''''');
         EXIT WHEN v_cursor_log%NOTFOUND;
         
         -- Para cada tipo de erro, adiciona uma constraint de CHECK para cada coluna
         -- Os erros vão para a tabela "v_exception_table_name"
         v_query_error := '';
         v_query_error := v_query_error || 'SELECT ''alter table etl2.'||v_flow_error_table_name||' add constraint ck_'||p_cod_historico||'_';
         v_query_error := v_query_error ||    v_cod_tipo_mensagem||'_''||a.cod_coluna||'' ';
         v_query_error := v_query_error ||    'check (''||tools.fn_multiple_replace('''||v_expr_regular_validacao||''', ';
         v_query_error := v_query_error ||    '''#COLUNA=''||a.nome||''/#TAMANHO=''||a.tamanho||''/#PRECISAO=''||NVL(a.precisao, '||c_max_precisao_numerica||'))||'') ';
         v_query_error := v_query_error ||    'VALIDATE exceptions into '||v_exception_table_name||''' exec_sql ';
         v_query_error := v_query_error || 'FROM (SELECT * FROM etl2.vw_interface_coluna WHERE '|| v_filtro_validacao ||') a ';
         v_query_error := v_query_error || 'WHERE a.cod_interface = :1  ';
         v_query_error := v_query_error || 'AND a.tipo_tabela = '''||c_tipo_tabela_alvo||'''  ';
         v_query_error := v_query_error || 'AND a.coluna_usada_interface = ''S'' ';
         
         dbms_output.put_line(v_query_error);
         
         OPEN v_cursor_error FOR v_query_error USING p_cod_interface;
         LOOP
            FETCH v_cursor_error INTO v_exec_sql;
            EXIT WHEN v_cursor_error%NOTFOUND;
            
            BEGIN
               EXECUTE IMMEDIATE v_exec_sql;
            EXCEPTION 
               WHEN OTHERS THEN
                  IF SQLCODE = -c_sqlcode_check_cnstr_violate THEN
                     NULL;
                  ELSE
                     RAISE;
                  END IF;
            END;
         
         END LOOP;
         CLOSE v_cursor_error;
      
      END LOOP;
      CLOSE v_cursor_log;
   END;
   
   PROCEDURE SP_GERAR_ENRIQUECIMENTO AS 
   BEGIN
      sp_add_interface_ddl('ENRIQUECIMENTO','BEGIN ');
      sp_add_interface_ddl('ENRIQUECIMENTO','   ETL2.PACK_ETL.SP_ENRIQUECER_INTERFACE( ');
      sp_add_interface_ddl('ENRIQUECIMENTO','      p_cod_interface => '||vp_cod_interface||', ');
      sp_add_interface_ddl('ENRIQUECIMENTO','      p_cod_historico => '||vp_cod_historico||' ');
      sp_add_interface_ddl('ENRIQUECIMENTO','   ); ');
      sp_add_interface_ddl('ENRIQUECIMENTO','END; ');
      sp_add_interface_ddl('ENRIQUECIMENTO','/', TRUE);         
   END;
   
   procedure SP_GERAR_LOG_INTERFACE as
   begin
      sp_add_interface_ddl('GERAR_LOG','BEGIN ');
      sp_add_interface_ddl('GERAR_LOG','   ETL2.PACK_ETL.SP_GERAR_LOG( ');
      sp_add_interface_ddl('GERAR_LOG','      p_cod_interface => '||vp_cod_interface||', ');
      sp_add_interface_ddl('GERAR_LOG','      p_cod_historico => '||vp_cod_historico||' ');
      sp_add_interface_ddl('GERAR_LOG','   ); ');
      sp_add_interface_ddl('GERAR_LOG','END; ');
      sp_add_interface_ddl('GERAR_LOG','/', TRUE);
   end;
   
   PROCEDURE SP_INSERIR_ALVO AS
      v_tabela_alvo     VARCHAR2(50) := NULL;
      v_juncao          VARCHAR2(2)  := NULL;
      v_juncao_atualiza VARCHAR2(2)  := NULL;
      v_juncao_insere   VARCHAR2(2)  := NULL;
      v_juncao_and      VARCHAR2(6)  := NULL;
      v_cmd_sql         CLOB         := NULL;
      
      v_lista_coluna_insere    CLOB    := NULL;
      v_coluna_insere          CLOB    := NULL;
      v_coluna_atualiza        CLOB    := NULL;
      v_estrat_atualz          CHAR(1) := NULL;
      v_chave_atualizacao      CLOB    := NULL;
      
   BEGIN
      -- Buscar definições da interface
      SELECT esquema_alvo||'.'||tabela_alvo, 
             estrategia_atualizacao
      INTO v_tabela_alvo, v_estrat_atualz
      FROM etl2.vw_interface
      WHERE cod_interface = vp_cod_interface;
      
      -- Define colunas e transformações restantes
      FOR v_map IN (
         SELECT map.*
         FROM etl2.vw_interface_mapeamento map
         WHERE map.cod_interface = vp_cod_interface
         ORDER BY map.posicao
      )
      LOOP
         IF v_map.participa_insercao = 'S' THEN
            v_coluna_insere := v_coluna_insere || v_juncao_insere || '   ' || v_map.transformacao_alvo;
            v_juncao_insere := ',' || chr(13);
            v_lista_coluna_insere  := v_lista_coluna_insere || v_juncao || '   ' || v_map.nome;
         END IF;
         IF v_map.participa_atualizacao = 'S' THEN
            v_coluna_atualiza := v_coluna_atualiza || v_juncao_atualiza || '   ' || v_map.nome || ' = ' || v_map.transformacao_alvo;
            v_juncao_atualiza := ',' || chr(13);
            --v_lista_coluna_atualiza  := v_lista_coluna_atualiza || v_juncao || '   ' || v_map.nome;
         END IF;
         
         IF v_map.chave_atualizacao = 'S' THEN
            v_chave_atualizacao := v_chave_atualizacao || v_juncao_and || 's.' || v_map.alias_coluna_alvo ||' = t.' || v_map.nome;
            v_juncao_and := ' AND '|| chr(13);
         END IF;
         
         v_juncao := ',' || chr(13);
         
      END LOOP;
      
      IF v_estrat_atualz IN (c_estrat_atulz_insert, c_estrat_atulz_merge) AND v_coluna_insere IS NULL THEN 
         RAISE e_estrat_insercao_invalida;
      ELSIF v_estrat_atualz IN (c_estrat_atulz_update, c_estrat_atulz_merge) AND v_coluna_atualiza IS NULL THEN
         RAISE e_estrat_atualizacao_invalida;
      END IF;
      
      IF v_chave_atualizacao IS NOT NULL THEN 
         sp_add_interface_ddl('BACKUP_ALVO_FLUXO','-- Criar backup dos registros da tabela alvo que serão atualizados pela tabela de fluxo');
         sp_add_interface_ddl('BACKUP_ALVO_FLUXO','CREATE GLOBAL TEMPORARY TABLE etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'backup')||' ON COMMIT PRESERVE ROWS AS ');
         sp_add_interface_ddl('BACKUP_ALVO_FLUXO','SELECT t.*, s.cod_fluxo ');
         sp_add_interface_ddl('BACKUP_ALVO_FLUXO','FROM '||v_tabela_alvo||' t ');
         sp_add_interface_ddl('BACKUP_ALVO_FLUXO','JOIN etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' s ON ('||v_chave_atualizacao||') ', true);
      END IF;
      
      SP_GERAR_ENRIQUECIMENTO;

      IF v_estrat_atualz = c_estrat_atulz_insert THEN -- INSERT 
         sp_add_interface_ddl('INSERT_ALVO','--Inserir registros novos na tabela alvo' );
         sp_add_interface_ddl('INSERT_ALVO','INSERT /*+ append */ INTO '||v_tabela_alvo );
         sp_add_interface_ddl('INSERT_ALVO','(' );
         sp_add_interface_ddl('INSERT_ALVO',v_lista_coluna_insere );
         sp_add_interface_ddl('INSERT_ALVO',')');
         sp_add_interface_ddl('INSERT_ALVO','SELECT ' );
         sp_add_interface_ddl('INSERT_ALVO',v_coluna_insere );
         sp_add_interface_ddl('INSERT_ALVO','FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' S ');
         sp_add_interface_ddl('INSERT_ALVO','WHERE ind_update = ''I''' );
         sp_add_interface_ddl('INSERT_ALVO','  AND cod_fluxo NOT IN (' );
         sp_add_interface_ddl('INSERT_ALVO','         SELECT cod_fluxo FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
         sp_add_interface_ddl('INSERT_ALVO','      )' , TRUE);
      ELSE
         -- MERGE
         sp_add_interface_ddl('MERGE_ALVO','--Mesclar registros na tabela alvo' );
         sp_add_interface_ddl('MERGE_ALVO','MERGE INTO '||v_tabela_alvo ||' t ' );
         sp_add_interface_ddl('MERGE_ALVO','USING (SELECT * ');
         sp_add_interface_ddl('MERGE_ALVO','       FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'fluxo')||' ');
         sp_add_interface_ddl('MERGE_ALVO','       WHERE cod_fluxo ');
         sp_add_interface_ddl('MERGE_ALVO','       NOT IN (' );
         sp_add_interface_ddl('MERGE_ALVO','         SELECT cod_fluxo FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico,'erro'));
         sp_add_interface_ddl('MERGE_ALVO','        )' );
         sp_add_interface_ddl('MERGE_ALVO',') S ');
         sp_add_interface_ddl('MERGE_ALVO','ON  (');
         sp_add_interface_ddl('MERGE_ALVO','   '|| v_chave_atualizacao );
         sp_add_interface_ddl('MERGE_ALVO',')');
         sp_add_interface_ddl('MERGE_ALVO','WHEN MATCHED ' );
         sp_add_interface_ddl('MERGE_ALVO','THEN UPDATE SET ' );
         sp_add_interface_ddl('MERGE_ALVO',v_coluna_atualiza );      
         IF v_estrat_atualz = 'M' THEN  -- M = INSERT + UPDATE
            sp_add_interface_ddl('MERGE_ALVO','WHEN NOT MATCHED ' );
            sp_add_interface_ddl('MERGE_ALVO','THEN INSERT ' );
            sp_add_interface_ddl('MERGE_ALVO','   ( ' );
            sp_add_interface_ddl('MERGE_ALVO',v_lista_coluna_insere );
            sp_add_interface_ddl('MERGE_ALVO','   ) ' );
            sp_add_interface_ddl('MERGE_ALVO','VALUES ' );
            sp_add_interface_ddl('MERGE_ALVO','   ( ' );
            sp_add_interface_ddl('MERGE_ALVO',v_coluna_insere );
            sp_add_interface_ddl('MERGE_ALVO','   )  ' , TRUE);
         ELSE
            sp_add_interface_ddl('MERGE_ALVO',' ' , TRUE);
         END IF;
      END IF;      
   END;
   
   PROCEDURE SP_INSERIR_RESUMO AS 
   BEGIN      
      sp_add_interface_ddl('INSERT_RESUMO','' );
      sp_add_interface_ddl('INSERT_RESUMO','MERGE INTO etl2.historico_execucao t ' );
      sp_add_interface_ddl('INSERT_RESUMO','USING ( ' );
      sp_add_interface_ddl('INSERT_RESUMO','   SELECT org.*, ' );
      sp_add_interface_ddl('INSERT_RESUMO','      (SELECT count(*) FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'externa_materializada')||') qtde_reg_arquivo ');
      sp_add_interface_ddl('INSERT_RESUMO','   FROM (  ' );
      sp_add_interface_ddl('INSERT_RESUMO','      SELECT ' );
      sp_add_interface_ddl('INSERT_RESUMO','         '||vp_cod_historico||' as cod_historico, ');      
      sp_add_interface_ddl('INSERT_RESUMO','         sum(inseriu)   qtde_reg_inseridos, ');
      sp_add_interface_ddl('INSERT_RESUMO','         sum(atualizou) qtde_reg_atualizados, ');
      sp_add_interface_ddl('INSERT_RESUMO','         sum(rejeitou)  qtde_reg_rejeitados ');
      sp_add_interface_ddl('INSERT_RESUMO','      FROM ( ');
      sp_add_interface_ddl('INSERT_RESUMO','         SELECT a.cod_origem, ');
      sp_add_interface_ddl('INSERT_RESUMO','            max(case when b.cod_fluxo is null and c.cod_fluxo is not null and c.ind_update = ''I'' then 1 else 0 end) inseriu, ');
      sp_add_interface_ddl('INSERT_RESUMO','            max(case when b.cod_fluxo is null and c.cod_fluxo is not null and c.ind_update = ''U'' then 1 else 0 end) atualizou, ');
      sp_add_interface_ddl('INSERT_RESUMO','            max(case when b.cod_fluxo is not null then 1 else 0 end) rejeitou ');
      sp_add_interface_ddl('INSERT_RESUMO','         FROM etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'origem')||' a  ');
      sp_add_interface_ddl('INSERT_RESUMO','         LEFT JOIN etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'erro')||' b ON (a.cod_fluxo = b.cod_fluxo) ');
      sp_add_interface_ddl('INSERT_RESUMO','         LEFT JOIN etl2.'||fn_get_nome_tabela(vp_cod_interface, vp_cod_historico, 'fluxo')||' c ON (a.cod_fluxo = c.cod_fluxo) ');
      sp_add_interface_ddl('INSERT_RESUMO','         GROUP BY a.cod_origem ');
      sp_add_interface_ddl('INSERT_RESUMO','      )   ' );
      sp_add_interface_ddl('INSERT_RESUMO','   ) org   ' );
      sp_add_interface_ddl('INSERT_RESUMO',') s' );
      sp_add_interface_ddl('INSERT_RESUMO','ON (t.cod_historico = s.cod_historico)' );
      sp_add_interface_ddl('INSERT_RESUMO','WHEN MATCHED' );
      sp_add_interface_ddl('INSERT_RESUMO','THEN UPDATE SET ' );
      sp_add_interface_ddl('INSERT_RESUMO','   t.qtde_reg_arquivo = s.qtde_reg_arquivo, ' );
      sp_add_interface_ddl('INSERT_RESUMO','   t.qtde_reg_inseridos = s.qtde_reg_inseridos, ' );
      sp_add_interface_ddl('INSERT_RESUMO','   t.qtde_reg_atualizados = s.qtde_reg_atualizados, ' );
      sp_add_interface_ddl('INSERT_RESUMO','   t.qtde_reg_ignorados = s.qtde_reg_arquivo-NVL(s.qtde_reg_inseridos,0)-NVL(s.qtde_reg_atualizados,0)-NVL(s.qtde_reg_rejeitados,0), ');   
      sp_add_interface_ddl('INSERT_RESUMO','   t.qtde_reg_rejeitados = s.qtde_reg_rejeitados', TRUE);      
      
      sp_add_interface_ddl('INSERT_RESUMO','   UPDATE etl2.historico SET data_termino = sysdate WHERE cod_historico = '||vp_cod_historico, TRUE );      
      
      sp_add_interface_ddl('COMMIT','   commit', TRUE );      
   END;
   
   FUNCTION FN_ADD_PROJETO_PASSO (
      p_cod_projeto    etl2.projeto.cod_projeto%TYPE,
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_nome           etl2.passo.nome%TYPE,
      p_primeiro_passo etl2.passo.primeiro_passo%TYPE
   ) RETURN etl2.passo.cod_passo%TYPE AS
      v_cod_passo etl2.passo.cod_passo%TYPE;
   BEGIN
      INSERT INTO etl2.passo (
         cod_passo,
         cod_projeto,
         cod_interface,
         nome,
         primeiro_passo
      )
      VALUES (
         etl2.sq_passo_pk.nextval,
         p_cod_projeto,
         p_cod_interface,
         p_nome,
         p_primeiro_passo
      )
      RETURNING cod_passo INTO v_cod_passo;
      
      RETURN v_cod_passo;
   END;
   
   PROCEDURE SP_SET_CICLO_PROJETO_PASSO (
      p_cod_passo    etl2.passo.cod_passo%TYPE,
      p_cod_passo_ok etl2.passo.cod_passo%TYPE,
      p_cod_passo_ko etl2.passo.cod_passo%TYPE
   ) AS
   BEGIN
      UPDATE etl2.passo
      SET cod_proximo_passo_ok = p_cod_passo_ok,
          cod_proximo_passo_ko = p_cod_passo_ko
      WHERE cod_passo = p_cod_passo;
   END;
   
   PROCEDURE SP_DELETE_PROJETO_PASSO (
      p_cod_projeto    etl2.projeto.cod_projeto%TYPE,
      p_tipo_execucao  CHAR := 'A'
   ) AS
   BEGIN
      -- Remover referências entre os passos
      UPDATE etl2.passo 
      SET cod_proximo_passo_ok = NULL, 
          cod_proximo_passo_ko = NULL
      WHERE cod_passo IN (SELECT cod_passo FROM etl2.passo WHERE cod_projeto = p_cod_projeto)
        AND (CASE p_tipo_execucao 
                WHEN c_tipo_exec_atualizacao THEN participa_atualizacao
                WHEN c_tipo_exec_exclusao THEN participa_exclusao
             END = 'S' OR p_tipo_execucao IS NULL);
      
      -- Excluir todos os passos desse projeto
      DELETE FROM etl2.passo 
      WHERE cod_projeto = p_cod_projeto
        AND (CASE p_tipo_execucao 
                WHEN c_tipo_exec_atualizacao THEN participa_atualizacao
                WHEN c_tipo_exec_exclusao THEN participa_exclusao
             END = 'S' OR p_tipo_execucao IS NULL);
   END;
   
   PROCEDURE SP_SET_PROJETO_PASSO (
      p_cod_projeto    etl2.projeto.cod_projeto%TYPE,
      p_lista_passos   VARCHAR2,
      p_tipo_execucao  CHAR := 'A'
   ) AS
      v_posicao NUMBER;
      v_posicao_tipo_execucao NUMBER;
      v_cod_passo_inicio_atualiza etl2.passo.cod_passo%TYPE;
      v_cod_passo_prox_atualiza   etl2.passo.cod_passo%TYPE;
      v_cod_passo_inicio_exclui   etl2.passo.cod_passo%TYPE;
      v_cod_passo_prox_exclui     etl2.passo.cod_passo%TYPE;
      v_passo_atual VARCHAR2(100);
      v_qtde_passos NUMBER;
      v_cod_prox_passo  NUMBER := NULL;
      
      c_pos_interface     CONSTANT NUMBER := 1;
      c_pos_descricao     CONSTANT NUMBER := 2;
      c_pos_log           CONSTANT NUMBER := 3;
      
   BEGIN
      -- Excluir todos os passos desse projeto.
      sp_delete_projeto_passo(p_cod_projeto, p_tipo_execucao);
      
      v_qtde_passos := REGEXP_COUNT(p_lista_passos, '[^;]+');
      v_cod_passo_inicio_atualiza := NULL;
      v_cod_passo_inicio_exclui := NULL;
      
      -- Varrer lista de passos na ordem reversa
      v_posicao        := v_qtde_passos;
      v_cod_passo_prox_atualiza := NULL;
      v_cod_passo_prox_exclui   := NULL;
      
      LOOP
         v_passo_atual := REGEXP_SUBSTR(p_lista_passos, '[^;]+', 1, v_posicao);
          
         INSERT INTO etl2.passo (
            cod_passo,
            cod_projeto,
            cod_interface,
            nome,
            cod_proximo_passo_ok,
            participa_atualizacao,
            participa_exclusao,
            gerar_log,
            primeiro_passo
         )
         VALUES (
            etl2.sq_passo_pk.nextval,
            p_cod_projeto,
            REGEXP_SUBSTR(v_passo_atual, '[^|]+', 1, c_pos_interface),
            REGEXP_SUBSTR(v_passo_atual, '[^|]+', 1, c_pos_descricao),
            v_cod_prox_passo,
            CASE WHEN p_tipo_execucao = 'A' THEN 'S' ELSE 'N' END,
            CASE WHEN p_tipo_execucao = 'E' THEN 'S' ELSE 'N' END,
            NVL(REGEXP_SUBSTR(v_passo_atual, '[^|]+', 1, c_pos_log), 'N'),
            CASE WHEN v_posicao = 1 THEN 'S' ELSE 'N' END
         )
         RETURNING cod_passo INTO v_cod_prox_passo; --Guarda o código do passo para preencher no passo anterior.
         
         EXIT WHEN v_posicao = 1;
         
         v_posicao := v_posicao - 1;
      END LOOP;
   END;
   
   PROCEDURE SP_GERAR_INTERFACE_IMPORTACAO(
      p_cod_historico       etl2.historico.cod_historico%TYPE,
      p_cod_interface       etl2.interface.cod_interface%TYPE,
      p_diretorio           etl2.projeto.diretorio%TYPE,
      p_nome_arquivo        VARCHAR2,
      p_gerar_log           CHAR := 'S',
      p_remover_tabelas_$   CHAR := 'S',
      p_validar_agendamento CHAR := 'S'
   ) AS 
   BEGIN
      
      --Exclusão das tabelas auxiliares
      --SP_EXCLUIR_TABELAS;
      
      --Criar tabela de erros gerais
      --SP_CRIAR_TAB_ERRO_GERAL;
      
      -- Validação de cabeçalho
      SP_GERAR_VALIDACAO_ARQUIVO(p_nome_arquivo => p_nome_arquivo, p_diretorio => p_diretorio);
            
      -- Criação da tabela externa
      SP_CRIAR_TABELA_EXTERNA(p_nome_arquivo => p_nome_arquivo, p_diretorio => p_diretorio);
      
      -- Validação de agendamento
      SP_GERAR_VALIDACAO_AGENDAMENTO(p_validar_agendamento => p_validar_agendamento);
      
      -- Tabela de fluxo
      SP_CRIAR_TABELA_FLUXO;
      
      -- Tabela de erros
      SP_CRIAR_TABELA_ERRO;
      
      SP_CRIAR_TABELA_EXCEPTION;

      -- Carga na tabela intermediária
      SP_INSERIR_FLUXO;
      
      -- Validações de constraints 
      SP_VERIFICAR_RESTRICOES;
      
      -- Inserções na tabela alvo
      SP_INSERIR_ALVO;
      
      IF p_gerar_log = 'S' THEN
         SP_INSERIR_RESUMO;
         SP_GERAR_LOG_INTERFACE;
      END IF;      
            
      IF p_remover_tabelas_$ = 'S' THEN
         SP_EXCLUIR_TABELAS;
      END IF;
      
   EXCEPTION
      WHEN e_estrat_insercao_invalida THEN
         DBMS_OUTPUT.PUT_LINE(c_estrategia_atualiz_invalida);
      WHEN e_estrat_atualizacao_invalida THEN
         DBMS_OUTPUT.PUT_LINE(c_estrategia_insercao_invalida);
      WHEN e_tabela_nao_existe_banco THEN
         DBMS_OUTPUT.PUT_LINE(c_tabela_nao_existe);
      WHEN OTHERS THEN 
         RAISE;
      
   END SP_GERAR_INTERFACE_IMPORTACAO;
   
   
   
   
   PROCEDURE SP_GERAR_INTERFACE_PROCESS(
      p_cod_historico  etl2.historico.cod_historico%TYPE,
      p_cod_interface  etl2.interface.cod_interface%TYPE,
      p_gerar_log      CHAR := 'S',
      p_diretorio           etl2.projeto.diretorio%TYPE,
      p_nome_arquivo        VARCHAR2,
      p_remover_tabelas_$   CHAR := 'S'
   ) AS 
   BEGIN
      IF p_nome_arquivo IS NOT NULL THEN 
         SP_CRIAR_TABELA_EXTERNA(p_nome_arquivo => p_nome_arquivo, p_diretorio => p_diretorio);
      END IF;
   
      sp_add_interface_ddl('PROCESSAMENTO','--Atualização de processamento' );
      sp_add_interface_ddl('PROCESSAMENTO','BEGIN ');
      sp_add_interface_ddl('PROCESSAMENTO','   ETL2.PACK_ETL.SP_EXECUTAR_INTERFACE_PROCESS (  ');
      sp_add_interface_ddl('PROCESSAMENTO','      p_cod_historico  => '||p_cod_historico||', ');
      sp_add_interface_ddl('PROCESSAMENTO','      p_cod_interface  => '||p_cod_interface||'  ');
      sp_add_interface_ddl('PROCESSAMENTO','   ); ');
      sp_add_interface_ddl('PROCESSAMENTO','END; ');
      sp_add_interface_ddl('PROCESSAMENTO','/', TRUE); 
      
      IF p_remover_tabelas_$ = 'S' THEN
         SP_EXCLUIR_TABELAS;
      END IF;
      
   END SP_GERAR_INTERFACE_PROCESS;
   
   
   
   PROCEDURE SP_EXECUTAR_PROJETO (
      p_cod_projeto         etl2.projeto.cod_projeto%TYPE,
      p_cod_usuario_perfil  etl2.historico.cod_usuario_perfil%TYPE,
      p_nome_arquivo        etl2.historico.nome_arquivo%TYPE,
      p_cod_passo_inicial   etl2.passo.cod_passo%TYPE := NULL,
      p_cod_passo_final     etl2.passo.cod_passo%TYPE := NULL,
      p_saida_dbms          CHAR := 'N',
      p_characterset        VARCHAR2 := NULL,
      p_cod_historico       etl2.historico.cod_historico%TYPE := NULL,
      p_enviar_email        CHAR := 'S',
      p_email_adicional     VARCHAR2 := NULL,
      p_enviar_anexos       CHAR := 'S',
      p_remover_tabelas_$   CHAR := 'S'
   ) AS
      CURSOR cur_passo_inicial IS 
         SELECT cod_passo
         FROM etl2.vw_passo a
         WHERE a.cod_projeto = p_cod_projeto
           AND participa_atualizacao = 'S' 
           AND (cod_passo = p_cod_passo_inicial 
                OR 
               (primeiro_passo = 'S' AND p_cod_passo_inicial IS NULL)
               );
      v_cod_passo_inicial NUMBER;
   BEGIN
      
      BEGIN
         OPEN cur_passo_inicial;
         FETCH cur_passo_inicial INTO v_cod_passo_inicial;
         IF cur_passo_inicial%NOTFOUND THEN 
            RAISE etl2.pack_etl.e_passo_inexistente;
         END IF;
         CLOSE cur_passo_inicial;
      EXCEPTION 
         WHEN e_passo_inexistente THEN 
            RAISE_APPLICATION_ERROR(-c_sqlcode_passo_nao_existe, c_passo_atualizacao_nao_existe);
      END;
      
   
      SP_EXECUTAR_PASSOS (
         p_cod_projeto         => p_cod_projeto,
         p_cod_usuario_perfil  => p_cod_usuario_perfil,
         p_nome_arquivo        => p_nome_arquivo,
         p_cod_passo_inicial   => v_cod_passo_inicial,
         p_cod_passo_final     => p_cod_passo_final,
         p_saida_dbms          => p_saida_dbms,
         p_cod_historico       => p_cod_historico,
         p_enviar_email        => p_enviar_email,
         p_email_adicional     => p_email_adicional,
         p_enviar_anexos       => p_enviar_anexos,
         p_tipo_execucao       => 'A',
         p_characterset        => p_characterset,
         p_remover_tabelas_$   => p_remover_tabelas_$
      );   
   END;
   
   PROCEDURE SP_CHECAR_PROJETO (
      p_cod_projeto etl2.projeto.cod_projeto%TYPE
   ) AS
       v_projeto etl2.vw_projeto%ROWTYPE;
       v_count NUMBER;
   BEGIN
      SELECT * INTO v_projeto
      FROM ETL2.VW_PROJETO 
      WHERE COD_PROJETO = p_cod_projeto;
      
      -- Conferir permissões do diretório
      SELECT count(*) INTO v_count
      FROM user_tab_privs
      WHERE table_name = v_projeto.diretorio
         and privilege in ('READ','WRITE');
      
      IF v_count <> 2 THEN
         RAISE_APPLICATION_ERROR(-20000, 'Confira as permissões para o ETL2 do diretório configurado "'||v_projeto.diretorio||'"');
      END IF;
   END;
   
   PROCEDURE SP_EXCLUIR_DADOS_PROJETO(
      p_cod_projeto         etl2.projeto.cod_projeto%TYPE,
      p_cod_usuario_perfil  etl2.historico.cod_usuario_perfil%TYPE,
      p_cod_passo_inicial   etl2.passo.cod_passo%TYPE := NULL,
      p_cod_passo_final     etl2.passo.cod_passo%TYPE := NULL,
      p_saida_dbms          CHAR := 'N',
      p_remover_tabelas_$   CHAR := 'S'
   ) AS 
      CURSOR cur_passo_inicial IS 
         SELECT cod_passo
         FROM etl2.vw_passo a
         WHERE a.cod_projeto = p_cod_projeto
           AND participa_exclusao = 'S' 
           AND (cod_passo = p_cod_passo_inicial 
                OR 
               (primeiro_passo = 'S' AND p_cod_passo_inicial IS NULL)
               );
      v_cod_passo_inicial NUMBER;
   BEGIN
      OPEN cur_passo_inicial;
      FETCH cur_passo_inicial INTO v_cod_passo_inicial;
      IF cur_passo_inicial%NOTFOUND THEN 
         RAISE etl2.pack_etl.e_passo_inexistente;
      END IF;
      CLOSE cur_passo_inicial;
   
      SP_EXECUTAR_PASSOS (
         p_cod_projeto         => p_cod_projeto,
         p_cod_usuario_perfil  => p_cod_usuario_perfil,
         p_nome_arquivo        => NULL,
         p_cod_passo_inicial   => v_cod_passo_inicial,
         p_cod_passo_final     => p_cod_passo_final,
         p_saida_dbms          => p_saida_dbms,
         p_cod_historico       => NULL,
         p_enviar_email        => 'N',
         p_email_adicional     => NULL,
         p_enviar_anexos       => NULL,
         p_tipo_execucao       => 'E',
         p_remover_tabelas_$   => p_remover_tabelas_$
      );
   EXCEPTION 
      WHEN etl2.pack_etl.e_passo_inexistente THEN 
         RAISE_APPLICATION_ERROR(-c_sqlcode_passo_nao_existe, c_passo_exclusao_nao_existe);
   END;      
      
   PROCEDURE SP_EXECUTAR_PASSOS (
      p_cod_projeto         etl2.projeto.cod_projeto%TYPE,
      p_cod_usuario_perfil  etl2.historico.cod_usuario_perfil%TYPE,
      p_nome_arquivo        etl2.historico.nome_arquivo%TYPE,
      p_cod_passo_inicial   etl2.passo.cod_passo%TYPE := NULL,
      p_cod_passo_final     etl2.passo.cod_passo%TYPE := NULL,
      p_saida_dbms          CHAR := 'N',
      p_cod_historico       etl2.historico.cod_historico%TYPE := NULL,
      p_enviar_email        CHAR := 'S',
      p_email_adicional     VARCHAR2 := NULL,
      p_enviar_anexos       CHAR := 'S',
      p_tipo_execucao       CHAR := 'A',
      p_characterset        VARCHAR2 := NULL,
      p_remover_tabelas_$   CHAR := 'S'
   ) AS 
      v_ordem_comando     NUMBER := NULL;
      v_tipo_comando      VARCHAR2(100);
      v_comando_anterior  CLOB;
      v_cmd_sql           CLOB;
      v_cod_historico     etl2.historico.cod_historico%TYPE;
      v_cod_passo         etl2.passo.cod_passo%TYPE;
      v_cod_passo_inicial etl2.passo.cod_passo%TYPE;
      v_cod_passo_final   etl2.passo.cod_passo%TYPE;
      v_status            etl2.historico.status%TYPE;
      v_status_antigo     etl2.historico.status%TYPE;
      v_erro              CLOB;
      v_qtde_projetos     NUMBER;
      v_qtde_exec         NUMBER;
      v_validar_agenda    BOOLEAN;
      v_mensagem_erro     CLOB;
      v_tipo_mensagem     etl2.log_tipo_mensagem.nome%TYPE;
      v_anexo_adicional   VARCHAR2(100) := NULL;
      
      v_verifica_erro     t_ref_cursor;
      
      CURSOR cur_passo IS 
         SELECT 
            a.cod_passo, 
            a.nome,
            a.cod_interface,
            a.cod_proximo_passo_ok,
            a.cod_proximo_passo_ko,
            a.gerar_log,
            a.tipo_interface
         FROM etl2.vw_passo a
         WHERE a.cod_projeto = p_cod_projeto
            AND a.cod_passo = v_cod_passo
            AND 
               CASE 
                  WHEN p_tipo_execucao = 'A' THEN a.participa_atualizacao
                  WHEN p_tipo_execucao = 'E' THEN a.participa_exclusao
               END ='S';
         
      v_passo cur_passo%ROWTYPE;
      
      CURSOR cur_historico_espera IS
         SELECT *
         FROM etl2.vw_historico_execucao
         WHERE cod_projeto = p_cod_projeto
           AND status = c_st_espera
         ORDER BY data_inicio;
         
      v_historico cur_historico_espera%ROWTYPE;
      
      -- Executa o comando existente na variável v_cmd_sql
      procedure executar as
         v_erro             CLOB;
         v_tabela           VARCHAR2(50);
         v_contagem_entrada NUMBER;
         v_max_permitido    NUMBER;
         v_erro_fatal       VARCHAR2(30);
         v_variaveis        CLOB;
         v_query            CLOB;
      begin
         execute immediate v_cmd_sql;

      EXCEPTION
         WHEN OTHERS THEN
         
            CASE SQLCODE  
               WHEN -c_sqlcode_cabecalho_invalido THEN
                  v_mensagem_erro := SQLERRM;
                  RAISE e_cabecalho_invalido;   
               WHEN -c_sqlcode_arquivo_invalido THEN
                  v_mensagem_erro := SQLERRM;
                  RAISE e_arquivo_invalido; 
               WHEN -c_sqlcode_agendamento THEN
                  v_mensagem_erro := SQLERRM;
                  RAISE e_agendamento;
               WHEN -c_sqlcode_enriquecimento THEN
                  v_mensagem_erro := SQLERRM;
                  RAISE e_enriquecimento;
               ELSE 
                  v_erro := sqlerrm;                        
                  dbms_output.put_line('ERRO NO BLOCO '||v_tipo_comando);
                  dbms_output.put_line('ERRO ORA: '||v_erro );
                  dbms_output.put_line('BLOCO => '||v_cmd_sql);               
                  
                  RAISE;
            END CASE;   
      end;
      
      procedure finalizar_execucao_erro(
         p_mensagem       VARCHAR2, 
         p_tipo_mensagem  etl2.log_tipo_mensagem.nome%TYPE := NULL
      ) as
      begin
         SP_GERAR_LOG (
            p_cod_interface    => vp_cod_interface,
            p_cod_historico    => vp_cod_historico,
            p_mensagem         => p_mensagem,
            p_tipo_mensagem    => p_tipo_mensagem
         );
            
         IF p_remover_tabelas_$ = 'S' THEN
            SP_EXCLUIR_TABELAS_INTERNAS(vp_cod_interface, vp_cod_historico);
         END IF;
      end;
      
      -- Insere o registro de histórico antes de começar a executar
      procedure inserir_registro_execucao as
         PRAGMA AUTONOMOUS_TRANSACTION;
         v_data_insercao DATE;
      begin
       
         v_data_insercao := SYSDATE;
      
         INSERT INTO etl2.historico
         (
            COD_HISTORICO,
            COD_PROJETO,
            COD_USUARIO_PERFIL,
            NOME_ARQUIVO,
            DATA_INICIO,
            STATUS            
         )
         VALUES
         (
            etl2.sq_historico_pk.nextval,
            p_cod_projeto,
            p_cod_usuario_perfil,
            p_nome_arquivo,
            v_data_insercao,
            v_status
         )
         RETURNING cod_historico INTO v_cod_historico;
         
         IF p_tipo_execucao = c_tipo_exec_atualizacao THEN
            INSERT INTO etl2.historico_execucao (cod_historico) VALUES ( v_cod_historico );
         ELSE
            INSERT INTO etl2.historico_exclusao (cod_historico) VALUES ( v_cod_historico );
         END IF;
         
         COMMIT;
      end;
      
      procedure iniciar_execucao_agora as 
         v_diretorio      VARCHAR2(30);
         v_tipo_interface CHAR(1);
      begin
         vp_cod_historico   := v_cod_historico;
         sp_registrar_execucao(v_cod_historico, c_registro_inicio_execucao, v_status);
         v_cod_passo := p_cod_passo_inicial;
                
         -- Busca diretório do projeto
         SELECT diretorio INTO v_diretorio
         FROM etl2.projeto
         WHERE cod_projeto = p_cod_projeto;
          
         -- Executa cada passo do projeto até chegar ao final ou até o passo definido no parâmetro "p_cod_passo_final"
         WHILE v_cod_passo IS NOT NULL
         LOOP
            v_tipo_mensagem := NULL;
            
            BEGIN
               OPEN cur_passo;
               FETCH cur_passo INTO v_passo;
               IF cur_passo%NOTFOUND THEN
                  RAISE e_passo_inexistente;
               END IF;
               CLOSE cur_passo;
            
               vp_ddl_linha_atual := 1;
               vp_ddl_bloco_atual := 1;
               vp_cod_interface   := v_passo.cod_interface;
               vp_characterset    := p_characterset;
               
               sp_registrar_execucao(v_cod_historico, c_registro_inicio_passo || ' ' || v_cod_passo||' ('||v_passo.nome||' ) ', c_st_execucao);
               --registrar_execucao('Início da execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) ');
             
               DELETE FROM etl2.interface_ddl
               WHERE cod_interface = vp_cod_interface;
               
               IF v_passo.tipo_interface = c_interface_importacao THEN
                  sp_gerar_interface_importacao(v_cod_historico, v_passo.cod_interface, v_diretorio, p_nome_arquivo, v_passo.gerar_log, p_remover_tabelas_$, CASE WHEN v_status_antigo = c_st_agendado THEN 'N' ELSE 'S' END);
               ELSE
                  sp_gerar_interface_process(v_cod_historico, v_passo.cod_interface, v_passo.gerar_log, v_diretorio, p_nome_arquivo);
               END IF;
               v_ordem_comando := -1;
               v_tipo_comando  := NULL;
               IF p_saida_dbms = 'S' THEN
                  dbms_output.put_line('-- Passo a ser executado: "'||v_passo.nome||'", nome do arquivo: "'||p_nome_arquivo||'"');
                  dbms_output.put_line('WHENEVER SQLERROR EXIT SQL.SQLCODE ROLLBACK');
               END IF;
               
               FOR x IN (SELECT * FROM etl2.interface_ddl WHERE cod_interface = v_passo.cod_interface ORDER BY linha)
               LOOP
                  IF v_ordem_comando <> x.ordem_comando THEN               
                     IF v_ordem_comando <> -1 THEN
                        IF p_saida_dbms = 'S' THEN
                           IF v_comando_anterior <> '/' THEN 
                              dbms_output.put_line(';');
                           END IF;
                        ELSE
                           executar;
                        END IF;
                     END IF;
                     v_ordem_comando := x.ordem_comando;
                     v_tipo_comando  := x.tipo_comando;
                     v_cmd_sql := NULL;
                  END IF;
                  
                  IF p_saida_dbms = 'S' THEN 
                     dbms_output.put_line(x.cmd_sql);
                  END IF;
                  v_comando_anterior := x.cmd_sql;
                  
                  IF x.cmd_sql <> '/' AND substr(x.cmd_sql, 1, 2) <> '--' THEN             
                     v_cmd_sql := v_cmd_sql || chr(13) || ' ' || x.cmd_sql;
                  END IF;
               END LOOP;
               
               IF p_saida_dbms = 'N' AND v_cmd_sql IS NOT NULL THEN 
                  executar;                  
               ELSIF p_saida_dbms = 'S' AND v_comando_anterior <> '/' THEN 
                     dbms_output.put_line(';');
               END IF;               
               
               --registrar_execucao('Execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) finalizada com sucesso.');
               sp_registrar_execucao(v_cod_historico, 'Execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) finalizada com sucesso.', v_status);
               
               IF v_cod_passo = p_cod_passo_final OR v_passo.cod_proximo_passo_ok IS NULL THEN 
                  v_cod_passo := NULL;  
                  v_status := c_st_finalizado;           
                  --registrar_execucao('Processo finalizado');
                  sp_registrar_execucao(v_cod_historico, 'Processo finalizado', v_status);
               ELSE
                  v_cod_passo := v_passo.cod_proximo_passo_ok;
               END IF;
               
            EXCEPTION 
               WHEN e_agendamento THEN
                  v_status := c_st_agendado;
                  v_tipo_mensagem := c_tp_msg_scheduled;
                  --registrar_execucao(v_mensagem_erro);
                  sp_registrar_execucao(v_cod_historico, v_mensagem_erro, v_status);
                  v_mensagem_erro := REGEXP_REPLACE(v_mensagem_erro, 'ORA-([0-9])*:[ ]*');
                  v_cod_passo := NULL;
               WHEN e_cabecalho_invalido THEN
                  v_status := c_st_erro;
                  v_tipo_mensagem := c_tp_msg_invalid_header;
                  v_anexo_adicional := 'modelo.csv';
                  --registrar_execucao('Execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) finalizada com erro => '||v_mensagem_erro);
                  sp_registrar_execucao(v_cod_historico, 'Execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) finalizada com erro => '||v_mensagem_erro, v_status);
                  v_mensagem_erro := REGEXP_REPLACE(v_mensagem_erro, 'ORA-([0-9])*:[ ]*');
                  v_cod_passo := v_passo.cod_proximo_passo_ko;
               WHEN e_arquivo_invalido THEN
                  v_status := c_st_erro;
                  v_tipo_mensagem := c_tp_msg_unexpected_error;
                  v_mensagem_erro := 'Execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) finalizada com erro => '||v_mensagem_erro;
                  --registrar_execucao(v_mensagem_erro);
                  sp_registrar_execucao(v_cod_historico, v_mensagem_erro, v_status);
                  v_cod_passo := v_passo.cod_proximo_passo_ko;
               WHEN e_enriquecimento THEN
                  v_status := c_st_erro;
                  v_tipo_mensagem := c_tp_msg_unexpected_error;
                  v_mensagem_erro := 'Execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) finalizada com erro => '||v_mensagem_erro;
                  --registrar_execucao(v_mensagem_erro);
                  sp_registrar_execucao(v_cod_historico, v_mensagem_erro, v_status);
                  v_cod_passo := v_passo.cod_proximo_passo_ko;
               WHEN e_passo_inexistente THEN
                  v_status := c_st_erro;
                  v_tipo_mensagem := c_tp_msg_unexpected_error;
                  v_mensagem_erro := c_sequencia_passo_invalida;
                  --registrar_execucao(v_mensagem_erro);
                  sp_registrar_execucao(v_cod_historico, v_mensagem_erro, v_status);
                  v_cod_passo := v_passo.cod_proximo_passo_ko;
               WHEN OTHERS THEN
                  -- Passo realizado sem sucesso, sempre chama o passo KO
                  v_erro := sqlerrm;
                  v_status := c_st_erro;
                  v_tipo_mensagem := c_tp_msg_unexpected_error;
                  v_mensagem_erro := 'Execução do passo '||v_cod_passo||' ('||v_passo.nome||' ) finalizada com ---erro => '||v_erro||' --- stack => '||dbms_utility.format_error_backtrace||'-------'||v_cmd_sql;
                  --registrar_execucao(v_mensagem_erro);
                  sp_registrar_execucao(v_cod_historico, v_mensagem_erro, v_status);
                  v_cod_passo := v_passo.cod_proximo_passo_ko;
            END;     
         END LOOP;
         
         IF v_status in (c_st_erro, c_st_agendado) THEN
            finalizar_execucao_erro(v_mensagem_erro, v_tipo_mensagem);
         END IF;
         
         
         IF p_enviar_email = 'S' THEN
            IF p_saida_dbms = 'S' THEN
               dbms_output.put_line('BEGIN ');
               dbms_output.put_line('   ETL2.PACK_ETL.SP_ENVIAR_EMAIL(p_cod_historico => '||v_cod_historico||', p_anexo_adicional => '''||v_anexo_adicional||''');');
               dbms_output.put_line('END; ');
               dbms_output.put_line('/');
            ELSE
               ETL2.PACK_ETL.SP_ENVIAR_EMAIL(p_cod_historico => v_cod_historico, p_anexo_adicional => v_anexo_adicional);
            END IF;
         END IF;
         
      end;

   BEGIN
      -- Verificar se esse projeto já existe
      SELECT count(*) INTO v_qtde_projetos
      FROM etl2.projeto 
      WHERE cod_projeto = p_cod_projeto;
      
      IF v_qtde_projetos = 0 THEN 
         RAISE e_projeto_inexistente;
      END IF;
      
      -- Verificar se esse projeto já está em execução
      SELECT count(*) qtde_exec 
      INTO v_qtde_exec
      FROM etl2.vw_historico_execucao
      WHERE cod_projeto = p_cod_projeto
         AND status = c_st_execucao;
      
      IF v_qtde_exec = 0 THEN -- Se não houver nenhum, pode executar
         
         --  EXECUTAR: -------------------------------------------------------------
         v_status := c_st_execucao;
         v_validar_agenda := TRUE;
         
         -- Se o código do histórico foi informado, continuar uma execução previamente registrada.
         IF p_cod_historico IS NULL THEN 
            inserir_registro_execucao;
         ELSE 
            SELECT cod_historico, sigla_status INTO v_cod_historico, v_status_antigo
            FROM etl2.vw_historico_execucao
            WHERE cod_historico = p_cod_historico;
            
            IF v_status_antigo = c_st_espera THEN
               --registrar_execucao('Esse projeto que estava em espera será executado agora.');
               sp_registrar_execucao(v_cod_historico, 'Esse projeto que estava em espera será executado agora.', v_status);
            ELSIF v_status_antigo = c_st_agendado THEN
               v_validar_agenda := FALSE;
               v_status := c_st_agendado;
               --registrar_execucao('Esse projeto que estava agendado será executado agora.');
               sp_registrar_execucao(v_cod_historico, 'Esse projeto que estava agendado será executado agora.', v_status);
            ELSE
               RAISE e_historico_finalizado;
            END IF;
         END IF;
            
         iniciar_execucao_agora;
         
         -- Após execução, verificar se existe algum item na fila para execução: ---
         v_historico.cod_historico := NULL;
         
         OPEN cur_historico_espera;
         FETCH cur_historico_espera INTO v_historico;
         CLOSE cur_historico_espera;
         
         IF v_historico.cod_historico IS NOT NULL THEN            
            SP_EXECUTAR_PROJETO (
               p_cod_projeto         => p_cod_projeto,
               p_cod_usuario_perfil  => v_historico.cod_usuario_perfil,
               p_nome_arquivo        => v_historico.nome_arquivo,
               p_cod_passo_inicial   => NULL,
               p_cod_passo_final     => NULL,
               p_saida_dbms          => 'N',
               p_characterset        => p_characterset,
               p_cod_historico       => v_historico.cod_historico,
               p_enviar_email        => p_enviar_email,
               p_email_adicional     => p_email_adicional,
               p_enviar_anexos       => p_enviar_anexos               
            );  
         END IF;
            
         ---------------------------------------------------------------------------

      ELSE
         --  COLOCAR EM ESPERA:------------------------   
         v_status := c_st_espera;
         IF p_cod_passo_inicial IS NOT NULL OR p_cod_passo_final IS NOT NULL OR p_saida_dbms = 'S' THEN 
            RAISE e_param_nao_suport_espera;
         ELSE
            IF p_cod_historico IS NULL THEN 
               inserir_registro_execucao;
            ELSE
               v_cod_historico := p_cod_historico;
               --registrar_execucao('Já existe outra execução desse projeto em andamento. A importação atual está em espera...');
               sp_registrar_execucao(v_cod_historico, c_registro_ja_existe_exec, v_status);
            END IF;
            
            
         END IF;
         
         ----------------------------------------------
      END IF;
   EXCEPTION
      WHEN e_param_nao_suport_espera THEN
         RAISE_APPLICATION_ERROR(-c_sqlcode_param_nao_sup_esp,c_param_nao_suport_espera);
      WHEN e_projeto_inexistente THEN
         RAISE_APPLICATION_ERROR(-c_sqlcode_projeto_inexistente,c_projeto_inexistente);
      WHEN e_historico_finalizado THEN
         RAISE_APPLICATION_ERROR(-c_sqlcode_historico_finalizado,c_historico_finalizado);
   END;
   
   
   
   
   PROCEDURE SP_EXCLUIR_PROJETO (p_cod_projeto etl2.projeto.cod_projeto%TYPE) AS
      v_count_passo_intf NUMBER;
   BEGIN
      UPDATE etl2.passo SET cod_proximo_passo_ok = NULL, cod_proximo_passo_ko = NULL WHERE cod_projeto = p_cod_projeto;
      
      FOR v_passo IN (SELECT cod_interface, cod_passo FROM ETL2.PASSO WHERE COD_PROJETO = p_cod_projeto)
      LOOP         
         DELETE FROM etl2.passo WHERE cod_passo = v_passo.cod_passo;
         SELECT count(*) INTO v_count_passo_intf
         FROM etl2.passo 
         WHERE cod_interface = v_passo.cod_interface;
         IF v_count_passo_intf = 0 THEN 
            SP_EXCLUIR_INTERFACE(v_passo.cod_interface);
         END IF;
      END LOOP;
      
      FOR v_historico IN (
         SELECT * FROM etl2.historico WHERE cod_projeto = p_cod_projeto
      )
      LOOP
         DELETE FROM etl2.historico_exclusao WHERE cod_historico = v_historico.cod_historico;
         DELETE FROM etl2.historico_exportacao WHERE cod_historico = v_historico.cod_historico;
         DELETE FROM etl2.historico_execucao WHERE cod_historico = v_historico.cod_historico;
      END LOOP;
      
      DELETE FROM etl2.historico WHERE cod_projeto = p_cod_projeto;
      DELETE FROM etl2.projeto WHERE cod_projeto = p_cod_projeto;
   END;
   
   PROCEDURE SP_EXCLUIR_INTERFACE (
      p_cod_interface etl2.interface.cod_interface%TYPE
   ) AS
   BEGIN
      DELETE FROM etl2.mapeamento WHERE cod_interface = p_cod_interface;
      FOR v_intf_tab IN (
         SELECT * FROM etl2.interface_tabela WHERE cod_interface = p_cod_interface
      )
      LOOP
         FOR v_intf_tab_col IN (
             SELECT * FROM etl2.interface_tabela_coluna 
             WHERE cod_interface_tabela = v_intf_tab.cod_interface_tabela
         )
         LOOP
            DELETE FROM etl2.transformacao_coluna 
            WHERE cod_interface_tabela_coluna = v_intf_tab_col.cod_interface_tabela_coluna;
            
            DELETE FROM etl2.interface_enriq_coluna 
            WHERE cod_interface_tabela_coluna = v_intf_tab_col.cod_interface_tabela_coluna;
                        
         END LOOP;
         
         DELETE FROM etl2.interface_tabela_coluna 
         WHERE cod_interface_tabela = v_intf_tab.cod_interface_tabela;
      END LOOP;
      
      DELETE FROM etl2.interface_tabela 
      WHERE cod_interface = p_cod_interface 
         AND tipo_tabela = 'F';    
      
      DELETE FROM etl2.interface_tabela WHERE cod_interface = p_cod_interface;    
      
      DELETE FROM etl2.interface_importacao where cod_interface = p_cod_interface;
      DELETE FROM etl2.interface_processamento where cod_interface = p_cod_interface;
      DELETE FROM etl2.interface_enriquecimento where cod_interface = p_cod_interface;
      DELETE FROM etl2.interface where cod_interface = p_cod_interface;
      
      
      FOR v_transformacao IN (
         SELECT * FROM etl2.transformacao WHERE cod_transformacao NOT IN (
            SELECT cod_transformacao FROM etl2.mapeamento
            UNION ALL
            SELECT cod_transformacao_join FROM etl2.interface_tabela 
            UNION ALL
            SELECT cod_transformacao_calc FROM etl2.interface_tabela_coluna
         )
      )
      LOOP         
         DELETE FROM etl2.transformacao_coluna WHERE cod_transformacao = v_transformacao.cod_transformacao;
         DELETE FROM etl2.transformacao_parametro WHERE cod_transformacao = v_transformacao.cod_transformacao;
         DELETE FROM etl2.transformacao WHERE cod_transformacao = v_transformacao.cod_transformacao;
         
      END LOOP;
   END;
   
   PROCEDURE SP_EXPORTAR_PROJETO (
      p_cod_projeto etl2.projeto.cod_projeto%TYPE
   ) AS
      v_linha NUMBER := 1;
      
      PROCEDURE add_linha (p_texto VARCHAR2) AS
      BEGIN
         v_linha := v_linha + 1;
         INSERT INTO etl2.projeto_exportado VALUES (p_cod_projeto, v_linha, p_texto);
      END;
   BEGIN      
      DELETE FROM etl2.projeto_exportado WHERE cod_projeto = p_cod_projeto;
      
      FOR x IN (SELECT * FROM etl2.vw_projeto WHERE cod_projeto = p_cod_projeto)
      LOOP
         add_linha('---------------------------------------------------------------------------');
         add_linha('BEGIN ');
         add_linha('   SP_ADD_PROJETO ( ');
         add_linha('      p_nome_projeto       => '''||x.nome||''',  ');
         add_linha('      p_descricao          => '''||x.descricao||''',  ');
         add_linha('      p_sigla_empresa      => '''||x.sigla_empresa||''',  ');
         add_linha('      p_qtde_linhas_maximo => '||x.qtde_linhas_maximo||',  ');
         add_linha('      p_diretorio          => '''||x.diretorio||''',  ');
         add_linha('      p_sql_reverso        => '''||x.sql_reverso||'''  ');
         add_linha('   ); ');
         add_linha('END; ');
         add_linha(' ');
         add_linha('---------------------------------------------------------------------------');
         add_linha('BEGIN ');
         add_linha('   SP_ADD_PROJETO ( ');
         add_linha('      p_nome_projeto       => '''||x.nome||''',  ');
         add_linha('      p_descricao          => '''||x.descricao||''',  ');
         add_linha('      p_sigla_empresa      => '''||x.sigla_empresa||''',  ');
         add_linha('      p_qtde_linhas_maximo => '||x.qtde_linhas_maximo||',  ');
         add_linha('      p_diretorio          => '''||x.diretorio||''',  ');
         add_linha('      p_sql_reverso        => '''||x.sql_reverso||'''  ');
         add_linha('   ); ');
         add_linha('END; ');
      END LOOP;
   END;
   
   PROCEDURE SP_EXCLUIR_INTERFACE (
      p_nome_interface etl2.interface.nome%TYPE
   ) AS
      CURSOR cur_interface IS
         SELECT cod_interface
         FROM etl2.interface 
         WHERE nome = p_nome_interface;
      v_cod_interface NUMBER;
   BEGIN
      
      OPEN cur_interface;
      FETCH cur_interface INTO v_cod_interface;
      CLOSE cur_interface;
   
      IF v_cod_interface IS NOT NULL THEN 
         SP_EXCLUIR_INTERFACE(v_cod_interface);
      END IF;
   END;
   
   PROCEDURE SP_EXCLUIR_TABELA_DICIONARIO (
      p_esquema etl2.tabela.esquema%TYPE,
      p_nome    etl2.tabela.nome%TYPE
   ) AS
   BEGIN
      DELETE FROM etl2.coluna WHERE cod_tabela = (
         SELECT cod_tabela 
         FROM etl2.tabela 
         WHERE esquema = p_esquema 
            AND nome = p_nome
      );
      
      DELETE FROM etl2.tabela 
      WHERE esquema = p_esquema 
         AND nome = p_nome;
   END;
   
   PROCEDURE SP_ADD_INTF_ENRIQUECIMENTO (
      p_interface            etl2.interface.nome%TYPE,
      p_nome_enriquecimento  etl2.enriquecimento.nome%TYPE,
      p_nome                 etl2.interface_enriquecimento.nome%TYPE,
      p_ordem                etl2.interface_enriquecimento.ordem%TYPE,      
      p_lista_coluna_relacao CLOB := NULL,
		p_qtde_reg_map			  VARCHAR2 := NULL
   ) AS
      v_cod_interface      etl2.interface.cod_interface%TYPE;
      v_cod_enriquecimento etl2.enriquecimento.cod_enriquecimento%TYPE;
      v_cod_intf_enriq     etl2.interface_enriquecimento.cod_interface_enriquecimento%TYPE;
      v_cod_intf_tab_alvo  etl2.interface_tabela.cod_interface_tabela%TYPE;
      v_cod_intf_tab_col   etl2.interface_tabela_coluna.cod_interface_tabela_coluna%TYPE;
      v_posicao            NUMBER;
      v_relacao_alvo_enriq VARCHAR2(200);
      v_nome_coluna_enriq  VARCHAR2(100);
      v_alias_coluna       VARCHAR2(100);
      v_cod_coluna         etl2.coluna.cod_coluna%TYPE;
		
      CURSOR cur_busca_interface_tab_coluna(p_cod_interface_tabela NUMBER, p_coluna VARCHAR) IS
         SELECT a.cod_interface_tabela_coluna
         FROM etl2.vw_interface_coluna a
         WHERE a.alias_coluna = p_coluna
           AND a.cod_interface_tabela = p_cod_interface_tabela;
      
      CURSOR cur_busca_coluna_sem_uso(p_cod_interface_tabela NUMBER, p_coluna VARCHAR) IS
         SELECT a.cod_coluna
         FROM etl2.vw_interface_coluna_sem_uso a
         WHERE a.nome = p_coluna
           AND a.cod_interface_tabela = p_cod_interface_tabela;
   BEGIN
      SELECT cod_interface, cod_interface_tabela_alvo INTO v_cod_interface, v_cod_intf_tab_alvo
      FROM etl2.vw_interface
      WHERE nome = p_interface;
      
      BEGIN
         SELECT cod_enriquecimento INTO v_cod_enriquecimento
         FROM etl2.vw_enriquecimento
         WHERE nome = p_nome_enriquecimento;
      EXCEPTION
         WHEN NO_DATA_FOUND THEN
            vp_variavel_erro := '#ENRIQ='||p_nome_enriquecimento;
            RAISE e_enriquecimento_nao_existe;
      END;
      
      INSERT INTO etl2.interface_enriquecimento (
         cod_interface_enriquecimento,
         cod_enriquecimento,
         cod_interface,
         nome,
         ordem,
			coluna_historico
      )
      VALUES (
         etl2.sq_interface_enriquecimento_pk.nextval,
         v_cod_enriquecimento,
         v_cod_interface,
         p_nome,
         p_ordem,
			p_qtde_reg_map
      )
      RETURNING cod_interface_enriquecimento INTO v_cod_intf_enriq;
      
      
      IF p_lista_coluna_relacao IS NOT NULL THEN
         v_posicao := 1;
         LOOP
            v_cod_intf_tab_col := NULL;
            
            v_relacao_alvo_enriq := REGEXP_SUBSTR(p_lista_coluna_relacao, '[^,]+', 1, v_posicao);
            EXIT WHEN v_relacao_alvo_enriq IS NULL;
            
            v_alias_coluna      := REGEXP_SUBSTR(v_relacao_alvo_enriq, '[^=]+', 1, 1);
            v_nome_coluna_enriq := REGEXP_SUBSTR(v_relacao_alvo_enriq, '[^=]+', 1, 2);
            --v_coluna_checagem := 'S';
				
            OPEN cur_busca_interface_tab_coluna(v_cod_intf_tab_alvo, v_alias_coluna);
            FETCH cur_busca_interface_tab_coluna INTO v_cod_intf_tab_col;
            IF v_cod_intf_tab_col IS NULL THEN 
               OPEN cur_busca_coluna_sem_uso(v_cod_intf_tab_alvo, v_alias_coluna);
               FETCH cur_busca_coluna_sem_uso INTO v_cod_coluna;
               IF cur_busca_coluna_sem_uso%FOUND THEN 
                  INSERT INTO etl2.interface_tabela_coluna (
                     cod_interface_tabela_coluna, 
                     cod_interface_tabela, 
                     cod_coluna, 
                     alias_coluna
                  )
                  VALUES (
                     etl2.sq_interface_tabela_coluna_pk.nextval, 
                     v_cod_intf_tab_alvo,
                     v_cod_coluna,
                     v_alias_coluna
                  )
                  RETURNING cod_interface_tabela_coluna INTO v_cod_intf_tab_col;
               ELSE 
                  vp_variavel_erro := '#COLUNA='||v_alias_coluna;
                  RAISE e_coluna_nao_existe_dicionario;
               END IF;
               CLOSE cur_busca_coluna_sem_uso;
            END IF;
            CLOSE cur_busca_interface_tab_coluna;
				
				INSERT INTO etl2.interface_enriq_coluna
            (cod_interface_enriquecimento, cod_interface_tabela_coluna, nome_coluna)
            VALUES
            (v_cod_intf_enriq, v_cod_intf_tab_col, v_nome_coluna_enriq);
				
            v_posicao := v_posicao + 1;
        END LOOP;
      END IF;
   EXCEPTION
      WHEN e_coluna_nao_existe_dicionario THEN
         RAISE_APPLICATION_ERROR(-c_sqlcode_coluna_n_exist_dic, tools.FN_MULTIPLE_REPLACE(c_coluna_nao_existe, vp_variavel_erro));
      WHEN e_enriquecimento_nao_existe THEN
         RAISE_APPLICATION_ERROR(-c_sqlcode_enriquec_n_exist, tools.FN_MULTIPLE_REPLACE(c_enriquecimento_nao_existe, vp_variavel_erro));
   END;
   
   PROCEDURE SP_DROP_INTF_ENRIQUECIMENTO (
      p_interface etl2.interface.nome%TYPE,
      p_ordem     etl2.interface_enriquecimento.ordem%TYPE := NULL
   ) AS 
   BEGIN
      FOR x IN (
         SELECT cod_interface_enriquecimento
         FROM etl2.vw_interface_enriquecimento
         WHERE nome_interface = p_interface
           AND ordem = p_ordem OR p_ordem IS NULL
      )
      LOOP
         DELETE FROM etl2.interface_enriq_coluna WHERE cod_interface_enriquecimento = x.cod_interface_enriquecimento;
         DELETE FROM etl2.interface_enriquecimento WHERE cod_interface_enriquecimento = x.cod_interface_enriquecimento;
      END LOOP;
   END;
   
   PROCEDURE SP_GERAR_LOG (
      p_cod_interface    etl2.interface.cod_interface%TYPE,
      p_cod_historico    etl2.historico.cod_historico%TYPE,
      p_mensagem         VARCHAR2 := NULL,
      p_tipo_mensagem    etl2.log_tipo_mensagem.nome%TYPE := NULL,
      p_cod_nivel_filtro etl2.log_nivel.cod_nivel%TYPE := NULL
   ) AS
      v_cursor            t_ref_cursor;
      v_occur_cursor      t_ref_cursor;
      
      TYPE rec_summary_error IS RECORD (
         cod_tipo_mensagem NUMBER,
         etl_variaveis_log VARCHAR2(4000),
         mensagem          VARCHAR2 (400),
         qtde_erro         NUMBER,
         erro_oracle       NUMBER,
         msg_oracle        VARCHAR2(4000)
      );
      v_summary_error rec_summary_error;
      
      -- Contadores
      v_linha_origem NUMBER;
      v_linha_html NUMBER;
      
      -- Dados de configuração
      -- v_diretorio etl2.projeto.diretorio%TYPE;
      v_historico        etl2.vw_historico%ROWTYPE;

      -- Variáveis auxiliares
      v_cod_fluxo      NUMBER;
      v_cabecalho      VARCHAR2(4000);
      v_colunas        VARCHAR2(4000) := NULL;      
      v_valor_colunas  CLOB;
      
      v_qtde_enriquecimento NUMBER;
      
      -- Arquivos texto
      v_log_file UTL_FILE.FILE_TYPE;
      
      v_query_summary_error CLOB;
      v_query_msg_erro CLOB;
      v_occur_query    CLOB;
      
      v_count_tabela NUMBER := 0;
      v_tabela_origem VARCHAR2(30);
      v_space_count NUMBER;
      
      v_nc_file_name VARCHAR2(400);
      v_log_file_name VARCHAR2(400);
      
      v_attachment_list VARCHAR2(4000);
      v_attachment_list_comma CHAR(1);
      
      v_cod_nivel NUMBER;
      
      v_mensagem CLOB;
      
      procedure add_log_row(
         p_row_text      VARCHAR2,
         p_show_in_email CHAR := 'N'
      ) as
      begin      
         INSERT INTO etl2.historico_log_html
         (
            cod_historico,
            linha,
            aparece_email,
            html_text,
            cod_interface,
            cod_nivel
         )
         VALUES
         (
            p_cod_historico,
            v_linha_html,
            p_show_in_email,
            p_row_text,
            p_cod_interface,
            v_cod_nivel
         );
         
         v_linha_html := v_linha_html + 1;
      end;

      procedure add_log_header(p_show_in_email CHAR := 'S') as 
      begin
         add_log_row('<html>', p_show_in_email);
         add_log_row('   <head><META http-equiv="Content-Type" content="text/html; charset=utf-8"> ',p_show_in_email);
         add_log_row('   <style type="text/css"> ',p_show_in_email);
         add_log_row('      table.gridtable { ',p_show_in_email);
         add_log_row('         font-family: verdana,arial,sans-serif; ',p_show_in_email);
         add_log_row('         font-size:11px; ',p_show_in_email);
         add_log_row('         color:#333333; ',p_show_in_email);
         add_log_row('         border-width: 1px; ',p_show_in_email);
         add_log_row('         border-color: #666666; ',p_show_in_email);
         add_log_row('         border-collapse: collapse; ',p_show_in_email);
         add_log_row('      } ',p_show_in_email);
         add_log_row('         table.gridtable th { ',p_show_in_email);
         add_log_row('         border-width: 1px; ',p_show_in_email);
         add_log_row('         padding: 8px; ',p_show_in_email);
         add_log_row('         border-style: solid; ',p_show_in_email);
         add_log_row('         border-color: #666666; ',p_show_in_email);
         add_log_row('         background-color: #dedede; ',p_show_in_email);
         add_log_row('      } ',p_show_in_email);
         add_log_row('      table.gridtable td { ',p_show_in_email);
         add_log_row('         border-width: 1px; ',p_show_in_email);
         add_log_row('         padding: 8px; ',p_show_in_email);
         add_log_row('         border-style: solid; ',p_show_in_email);
         add_log_row('         border-color: #666666; ',p_show_in_email);
         add_log_row('         background-color: #ffffff; ',p_show_in_email);
         add_log_row('      } ',p_show_in_email);
         add_log_row('   </style> ',p_show_in_email);
         add_log_row('   </head>', p_show_in_email);
         add_log_row('   <body style="font-family: arial,sans-serif">', p_show_in_email);
         add_log_row('      <h2 align="center">Importação de Dados - Geofusion OnMaps</h2>', p_show_in_email);
         add_log_row('      <hr/>', p_show_in_email);
         add_log_row('      <table>', p_show_in_email);
         add_log_row('      <tr>', p_show_in_email);
         add_log_row('         <td><b>Empresa</b></td>', p_show_in_email);
         add_log_row('         <td>: '||v_historico.nome_empresa||'</td>', p_show_in_email);
         add_log_row('      </tr>', p_show_in_email);
         add_log_row('      <tr>', p_show_in_email);
         add_log_row('         <td><b>Usuário solicitante</b></td>', p_show_in_email);
         add_log_row('         <td>: '||v_historico.nome_usuario||'</td>', p_show_in_email);
         add_log_row('      </tr>', p_show_in_email);
         add_log_row('      <tr>', p_show_in_email);
         add_log_row('         <td><b>Descrição</b></td>', p_show_in_email);
         add_log_row('         <td>: '||v_historico.descricao||'</td>', p_show_in_email);
         add_log_row('      </tr>', p_show_in_email);
         add_log_row('      <tr>', p_show_in_email);
         add_log_row('         <td><b>Arquivo enviado</b></td>', p_show_in_email);
         add_log_row('         <td>: '||v_historico.nome_arquivo||'</td>', p_show_in_email);
         add_log_row('      </tr>', p_show_in_email);
         add_log_row('      <tr>', p_show_in_email);
         add_log_row('         <td><b>Início</b></td>', p_show_in_email);
         add_log_row('         <td>: '||TO_CHAR(v_historico.data_inicio, 'DD/MM/YYYY HH24:MI:SS')||'</td>', p_show_in_email);
         add_log_row('      </tr>', p_show_in_email);
         IF v_historico.data_termino IS NOT NULL THEN 
            add_log_row('      <tr>', p_show_in_email);
            add_log_row('         <td><b>Término</b></td>', p_show_in_email);
            add_log_row('         <td>: '||TO_CHAR(v_historico.data_termino, 'DD/MM/YYYY HH24:MI:SS')||'</td>', p_show_in_email);
            add_log_row('      </tr>', p_show_in_email);
         END IF;
         add_log_row('      </table>', p_show_in_email);
         add_log_row('      <hr/>', p_show_in_email);
      end;
      
      /**
         Gera o arquivo de log para casos em que o processo finalizou corretamente.
         @param p_cod_nivel           Nível de log (tabela etl2.log_nivel);
         @param p_principal           Define se esse nível de log é o principal
         @param p_nc_file_name        Nome do arquivo de não carregados
      */
      procedure gerar_arquivo_log(
         p_cod_nivel    etl2.log_nivel.cod_nivel%TYPE,
         p_principal    VARCHAR2,
         p_nc_file_name VARCHAR2
      ) as        
         v_error_message CLOB;            -- Armazena mensagem de erro extraído da tabela de erros;
         v_log_file_name VARCHAR2(400);   -- Nome do arquivo de log;
         v_enr_count     NUMBER;          -- Contador para linhas de enriquecimento (HTML);
         v_err_count     NUMBER;          -- Contador para linhas de erros (HTML);
         v_suffix        VARCHAR2(20);    -- Define sufixo do arquivo gerado.
         
         v_enrich_query CLOB;
         v_counter      NUMBER;
         v_column_count NUMBER;
         v_column       VARCHAR2(30);
         v_column_list  VARCHAR2(400);
         
         v_value VARCHAR2(4000);
         v_ocurr_count NUMBER;
         v_ocurr_total NUMBER;
         
         v_cod_tipo_mensagem_anterior NUMBER;
      begin
         v_cod_nivel := p_cod_nivel;
         
         IF p_principal = 'S' THEN
            v_suffix      := 'logusuario';
         ELSE
            v_suffix      := 'log';
         END IF;
      
         add_log_header(p_principal);
         add_log_row('', p_principal);
         add_log_row('   <h3>Resumo da importação: </h3> ', p_principal);
         add_log_row('   <table style="padding-left:15px"> ', p_principal);
         add_log_row('      <tr> ', p_principal);
         add_log_row('         <td>Total de registros no arquivo</td> ', p_principal);
         add_log_row('         <td>: '||v_historico.qtde_reg_arquivo||'</td> ', p_principal);
         add_log_row('      </tr> ', p_principal);
         add_log_row('      <tr> ', p_principal);
         add_log_row('         <td>Total de registros inseridos</td> ', p_principal);
         add_log_row('         <td>: '||NVL(v_historico.qtde_reg_inseridos,0)||'</td> ', p_principal);
         add_log_row('      </tr> ', p_principal);
         add_log_row('      <tr> ', p_principal);
         add_log_row('         <td>Total de registros atualizados</td> ', p_principal);
         add_log_row('         <td>: '||NVL(v_historico.qtde_reg_atualizados,0)||'</td> ', p_principal);
         add_log_row('      </tr> ', p_principal);
         add_log_row('      <tr> ', p_principal);
         add_log_row('         <td>Total de registros rejeitados</td> ', p_principal);
         add_log_row('         <td>: '||NVL(v_historico.qtde_reg_rejeitados,0)||'</td> ', p_principal);
         add_log_row('      </tr> ', p_principal);
         IF v_historico.qtde_reg_ignorados > 0 THEN 
            add_log_row('      <tr> ', p_principal);
            add_log_row('         <td>Total de registros ignorados*</td> ', p_principal);
            add_log_row('         <td>: '||v_historico.qtde_reg_ignorados||'</td> ', p_principal);
            add_log_row('      </tr> ', p_principal);         
         END IF;
         add_log_row('   </table> ', p_principal);
         add_log_row('   <br/> ', p_principal);
         IF v_historico.qtde_reg_ignorados > 0 THEN 
            add_log_row('   <div><i>* Registros que não sofreram modificação são ignorados</i></div>', p_principal);
         END IF;
         
         v_enr_count := 0;
         v_enrich_query := '';
         v_enrich_query := v_enrich_query || 'SELECT count(*) ';
         v_enrich_query := v_enrich_query || 'FROM etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'fluxo')||' ';
         v_enrich_query := v_enrich_query || 'WHERE cod_fluxo NOT IN ( ';
         v_enrich_query := v_enrich_query || '   SELECT cod_fluxo FROM etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'erro')||' ';
         v_enrich_query := v_enrich_query || ') AND ';
         
         IF v_historico.qtde_reg_atualizados > 0 OR v_historico.qtde_reg_inseridos > 0 THEN
            FOR v_enriquecimento IN (
               SELECT * 
               FROM etl2.vw_interface_enriquecimento
               WHERE cod_interface = p_cod_interface
                 AND coluna_checagem IS NOT NULL
            )
            LOOP
               v_enr_count := v_enr_count + 1;
               IF v_enr_count = 1 THEN 
                  add_log_row('   <h3>Taxa de enriquecimento(s): </h3> ', p_principal);
                  add_log_row('   <table> ', p_principal);               
               END IF;
            
               OPEN v_cursor FOR v_enrich_query || v_enriquecimento.coluna_checagem||' IS NOT NULL';
               FETCH v_cursor INTO v_qtde_enriquecimento;
               EXIT WHEN v_cursor%NOTFOUND;
               
               IF v_enriquecimento.coluna_historico IS NOT NULL THEN 
                  EXECUTE IMMEDIATE 'UPDATE etl2.historico_execucao SET '||v_enriquecimento.coluna_historico||' = :1 WHERE cod_historico = :2' USING v_qtde_enriquecimento, v_historico.cod_historico;
               END IF;
               
               add_log_row('      <tr> ', p_principal);
               add_log_row('         <td>'||v_enriquecimento.nome||'</td> ', p_principal);
               
               IF v_historico.qtde_reg_inseridos+v_historico.qtde_reg_atualizados > 0 THEN 
                  add_log_row('         <td>: '||v_qtde_enriquecimento || ' ('||round(v_qtde_enriquecimento/(v_historico.qtde_reg_inseridos+v_historico.qtde_reg_atualizados)*100, 2)||'%);'||'</td> ', p_principal);
               ELSE
                  add_log_row('         <td> - </td> ', p_principal);
               END IF;
               add_log_row('      </tr> ', p_principal);
               CLOSE v_cursor;
            END LOOP;
            
            IF v_enr_count > 0 THEN 
               add_log_row('   </table> ', p_principal);
            END IF;
         END IF;
         
         IF v_historico.qtde_reg_rejeitados > 0 THEN
            v_cod_tipo_mensagem_anterior := -1;
                        
            add_log_row('   <h3>Situação dos Registros Rejeitados</h3> ');
            add_log_row('   <ol> ');
            
            DBMS_OUTPUT.PUT_LINE('-- v_query_summary_error: '||v_query_summary_error);
            
            OPEN v_cursor FOR v_query_summary_error USING p_cod_nivel;
            LOOP
               FETCH v_cursor INTO v_summary_error;
               EXIT WHEN v_cursor%NOTFOUND;
               
               IF p_cod_nivel = 1 OR v_cod_tipo_mensagem_anterior <> v_summary_error.cod_tipo_mensagem THEN 
                 
                  v_error_message := TOOLS.FN_MULTIPLE_REPLACE(v_summary_error.mensagem, v_summary_error.etl_variaveis_log);
                  add_log_row('      <li> ');
                  add_log_row('         '||v_error_message||' <br/> ');
                  v_column_list := regexp_substr(v_summary_error.etl_variaveis_log, '(#EXT=)(([^/$])*)', 1, 1, 'i', 2);
                  IF v_column_list IS NOT NULL THEN
                     add_log_row('         <table style="font-size:12;border-collapse: collapse;border-style:outset;border-width:1px" class="gridtable"> ');
                     add_log_row('            <tr style="font-weight:bold;border-style:inset;border-width:1px"> ');
                     add_log_row('               <td style="border-right-style:inset; width:80%">'||REPLACE(v_column_list, ',', ' /')||'</td> ');
                     add_log_row('               <td>OCORRÊNCIAS</td> ');
                     add_log_row('            </tr> ');
                     v_counter := 1;
                     DBMS_OUTPUT.PUT_LINE('-- V_OCCUR_CURSOR: '||REPLACE(v_occur_query, '#EXT', REPLACE(v_column_list, ', ', '|| ''/'' ||')));
                     
                     OPEN v_occur_cursor FOR REPLACE(v_occur_query, '#EXT', REPLACE(v_column_list, ', ', '|| ''/'' ||')) USING v_summary_error.cod_tipo_mensagem;
                     LOOP
                        FETCH v_occur_cursor INTO v_value, v_ocurr_count, v_ocurr_total;
                        EXIT WHEN v_occur_cursor%NOTFOUND;
                       
                        add_log_row('            <tr> ');
                        add_log_row('               <td style="border-right-style:outset">'||v_value||'</td> ');
                        add_log_row('               <td style="text-align:right">'||v_ocurr_count||'</td> ');
                        add_log_row('            </tr> ');
                      
                        v_counter := v_counter + 1;
                        IF v_counter > 10 THEN 
                           add_log_row('            <tr> ');
                           add_log_row('               <td style="border-right-style:inset">...</td> ');
                           add_log_row('               <td style="border-right-style:inset">...</td> ');
                           add_log_row('            </tr> ');
                           EXIT;
                        END IF;
                     END LOOP;
                   
                     IF v_ocurr_total > 0 THEN
                        add_log_row('            <tr style="font-weight:bold;border-style:inset;border-width:1px"> ');
                        add_log_row('               <td style="border-right-style:outset">Total</td> ');
                        add_log_row('               <td style="text-align:right">'||v_ocurr_total||'</td> ');
                        add_log_row('            </tr> ');
                     END IF;
                     add_log_row('         </table> ');
                  ELSE
                     add_log_row('         Ocorrências: '||v_summary_error.qtde_erro||'<br/> ');
                  END IF;
               
                  add_log_row('      </li> ');
                  add_log_row('      <br/> ');
                  
                  v_cod_tipo_mensagem_anterior := v_summary_error.cod_tipo_mensagem;
               
               END IF;
            END LOOP;
            CLOSE v_cursor;
            
            add_log_row('   </ol> ');
            add_log_row('   <br/><br/>');
            add_log_row('   <div><i>Obs: Todos os registros rejeitados encontram-se no arquivo "'||p_nc_file_name||'"</i></div> ');
         END IF; 

         add_log_row('   </body >', p_principal);
         add_log_row('</html>', p_principal);
         
         DECLARE
            v_file_date DATE := sysdate;
         BEGIN
            v_log_file_name := fn_get_nome_arquivo (p_cod_historico, v_suffix, v_file_date, p_cod_nivel);         
            v_log_file      := sys.utl_file.fopen_nchar(v_historico.diretorio_atual, v_log_file_name, 'w', 32767);         
         END;
         
         FOR x IN (
            SELECT html_text
            FROM etl2.historico_log_html 
            WHERE cod_historico = p_cod_historico 
               AND cod_nivel = v_cod_nivel 
               AND cod_interface = p_cod_interface
         )
         LOOP  
            UTL_FILE.put_line_nchar(v_log_file, x.html_text);
         END LOOP;    

         UTL_FILE.FCLOSE(v_log_file);
      end;
      
      function gerar_arquivo_nao_carregados return varchar2 as
         v_mensagem_erro        CLOB;
         v_file_name            VARCHAR2(400);
         v_data_geracao_arquivo DATE := SYSDATE;
         v_nc_file              UTL_FILE.FILE_TYPE;
      begin
         v_file_name := fn_get_nome_arquivo (p_cod_historico, 'nc', v_data_geracao_arquivo);
         v_nc_file  := SYS.UTL_FILE.FOPEN_NCHAR(v_historico.diretorio_atual, v_file_name, 'w', 32767);
         
         UTL_FILE.PUT_LINE_NCHAR(v_nc_file, v_cabecalho);
         
         -- Gerar arquivo de não carregados
         OPEN v_cursor FOR
           'SELECT x.cod_fluxo, 
                   '||v_colunas||'  as valor_colunas
            FROM (
                  SELECT DISTINCT cod_fluxo
                  FROM etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'erro')||'
               ) x
               JOIN etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'origem')||' y
               ON (x.cod_fluxo = y.cod_fluxo)
          ';
            
         LOOP
            FETCH v_cursor INTO v_cod_fluxo, v_valor_colunas;
            EXIT WHEN v_cursor%NOTFOUND;

            UTL_FILE.PUT_LINE_NCHAR(v_nc_file, REPLACE(REPLACE(TRIM(v_valor_colunas), chr(13), null), chr(10), null));
         END LOOP;
         CLOSE v_cursor;

         UTL_FILE.FCLOSE(v_nc_file);
         
         RETURN v_file_name;

      end;
      
      procedure gerar_arquivo_log_erro (
         p_mensagem      CLOB,
         p_tipo_mensagem etl2.log_tipo_mensagem.nome%TYPE := NULL
      )
      as
         v_data_geracao_arquivo DATE;
         c_file_maxlength CONSTANT NUMBER:= 80;
         v_space_count    NUMBER;
         
         v_mensagem_erro CLOB;
         v_log_file_name VARCHAR2(400);
         v_count NUMBER := NULL;
      begin
         v_data_geracao_arquivo := sysdate;
         v_cod_nivel := null;
         
         add_log_header;
         add_log_row('', 'S');
         add_log_row('   <div>Resultado: <b>'||p_mensagem||'</b></div>', 'S');
         add_log_row('   </body >', 'S');
         add_log_row('</html>', 'S');
         
         SELECT count(*) INTO v_count 
         FROM dba_tab_privs 
         WHERE table_name = v_historico.diretorio_atual 
         and grantee = 'ETL2'
         and privilege = 'WRITE';         
         
         IF v_count > 0 THEN
            v_log_file_name := fn_get_nome_arquivo (p_cod_historico, 'logusuario', v_data_geracao_arquivo) ;       
            v_log_file := SYS.UTL_FILE.FOPEN_NCHAR(v_historico.diretorio_atual, v_log_file_name, 'w', 32767);
            FOR x IN (SELECT * FROM etl2.historico_log_html WHERE cod_historico = p_cod_historico)
            LOOP  
               SYS.utl_file.put_line_nchar(v_log_file, x.html_text);
            END LOOP;
            UTL_FILE.FCLOSE(v_log_file);
         END IF;
      end;
   BEGIN
      SELECT NVL(count(*), 0)+1 INTO v_linha_html
      FROM etl2.historico_log_html
      WHERE cod_historico = p_cod_historico;
      
      IF p_mensagem IS NOT NULL THEN 
         FOR x IN (
            
            SELECT a.mensagem, a.cod_nivel
            FROM etl2.log_mensagem a
               JOIN etl2.log_tipo_mensagem b on (a.cod_tipo_mensagem = b.cod_tipo_mensagem)
            WHERE b.nome = NVL(p_tipo_mensagem, c_tp_msg_unexpected_error)
         )
         LOOP     
            
            IF x.cod_nivel = 1 AND p_tipo_mensagem = c_tp_msg_unexpected_error THEN  -- Nível 1 = admin
               v_mensagem := 'Histórico: '||p_cod_historico||'<br>'||p_mensagem;
               tools.sp_send_mail (
                  ReceiverAddress => null, 
                  arquivo         => null, 
                  subject         => 'Projeto ETL 2.0 ' || v_historico.nome_projeto|| ' com erros! -> '||UPPER(sys_context('USERENV', 'SERVER_HOST')), 
                  message         => p_mensagem,
                  somente_sys     => 'S',
                  is_html         => 'S'
               ); 
            ELSE
               v_mensagem := tools.fn_multiple_replace(x.mensagem, '#MENSAGEM='||p_mensagem);
            END IF;
         END LOOP;
         
         RAISE e_erro_execucao;
      END IF;
      
      SELECT listagg(nome, ';') within group (order by posicao),
             listagg('y.'||nome, '||'';''||') within group (order by posicao)
      INTO v_cabecalho, v_colunas
      FROM etl2.vw_interface_coluna
      WHERE tabela_externa = 'S'
        AND coluna_calculada = 'N'
        AND tipo <> 'SDO_GEOMETRY'
        AND cod_interface = p_cod_interface;
      
      v_query_msg_erro :=                     'SELECT a.cod_fluxo, ';
      v_query_msg_erro := v_query_msg_erro || '       a.etl_check_date, ';
      v_query_msg_erro := v_query_msg_erro || '       a.etl_cod_tipo_mensagem, ';
      v_query_msg_erro := v_query_msg_erro || '       a.etl_variaveis_log, ';
      v_query_msg_erro := v_query_msg_erro || '       ''ETL-''||LPAD(etl_cod_tipo_mensagem, 6, ''0'')||'' : ''||TOOLS.FN_MULTIPLE_REPLACE(b.mensagem, a.etl_variaveis_log) mensagem_traduzida ';
      v_query_msg_erro := v_query_msg_erro || 'FROM etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'erro')||' a ';
      v_query_msg_erro := v_query_msg_erro || '   JOIN etl2.log_mensagem b ON (a.etl_cod_tipo_mensagem = b.cod_tipo_mensagem AND b.cod_nivel = :cod_nivel)  ';
      v_query_msg_erro := v_query_msg_erro || '   JOIN etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'origem')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
      v_query_msg_erro := v_query_msg_erro || 'WHERE b.cod_nivel = :cod_nivel ';
      v_query_msg_erro := v_query_msg_erro || '   AND c.cod_fluxo = :cod_i_origem ';
      v_query_msg_erro := v_query_msg_erro || 'ORDER BY a.etl_check_date'; 
      
      v_query_summary_error := '';
      v_query_summary_error := v_query_summary_error || 'SELECT * ';
      v_query_summary_error := v_query_summary_error || 'FROM ( ';
      v_query_summary_error := v_query_summary_error || '   SELECT  ';
      v_query_summary_error := v_query_summary_error || '      a.etl_cod_tipo_mensagem,  ';
      v_query_summary_error := v_query_summary_error || '      a.etl_variaveis_log,  ';
      v_query_summary_error := v_query_summary_error || '      b.mensagem, ';
      v_query_summary_error := v_query_summary_error || '      count(a.cod_fluxo) qtde_erro, ';
      v_query_summary_error := v_query_summary_error || '      a.erro_oracle, ';
      v_query_summary_error := v_query_summary_error || '      a.msg_oracle ';
      v_query_summary_error := v_query_summary_error || '   FROM etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'erro')||' a ';
      v_query_summary_error := v_query_summary_error || '      JOIN etl2.log_mensagem b ON (a.etl_cod_tipo_mensagem = b.cod_tipo_mensagem) ';
      v_query_summary_error := v_query_summary_error || '      JOIN etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'origem')||' c ON (a.cod_fluxo = c.cod_fluxo) ';
      v_query_summary_error := v_query_summary_error || '   WHERE b.cod_nivel = :1 ';
      v_query_summary_error := v_query_summary_error || '   GROUP BY ';
      v_query_summary_error := v_query_summary_error || '      a.etl_cod_tipo_mensagem, ';
      v_query_summary_error := v_query_summary_error || '      a.etl_variaveis_log, ';
      v_query_summary_error := v_query_summary_error || '      b.mensagem, ';
      v_query_summary_error := v_query_summary_error || '      a.erro_oracle, ';
      v_query_summary_error := v_query_summary_error || '      a.msg_oracle ';
      v_query_summary_error := v_query_summary_error || ') ';
      v_query_summary_error := v_query_summary_error || 'ORDER BY qtde_erro desc, etl_cod_tipo_mensagem';
      
      v_occur_query := '';
      v_occur_query := v_occur_query || 'SELECT x.*, SUM(qtde) OVER (ORDER BY qtde) total ';
      v_occur_query := v_occur_query || 'FROM ( ';
      v_occur_query := v_occur_query || '   SELECT #EXT as valor, count(*) qtde ';
      v_occur_query := v_occur_query || '   FROM   etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'origem')||' ';
      v_occur_query := v_occur_query || '   WHERE cod_fluxo IN ( ';
      v_occur_query := v_occur_query || '      SELECT cod_fluxo FROM etl2.'||fn_get_nome_tabela(p_cod_interface, p_cod_historico,'erro')||' WHERE etl_cod_tipo_mensagem = :1 ';
      v_occur_query := v_occur_query || '   ) ';
      v_occur_query := v_occur_query || '   GROUP BY #EXT ';
      v_occur_query := v_occur_query || ') x ';
      v_occur_query := v_occur_query || 'ORDER BY x.qtde DESC ';
      
      v_attachment_list := NULL;
      v_attachment_list_comma := '';
      
      SELECT * INTO v_historico
      FROM etl2.vw_historico a
      WHERE cod_historico = p_cod_historico;
      
      v_nc_file_name := gerar_arquivo_nao_carregados;
      FOR v_cod_nivel IN (
         
         SELECT a.cod_nivel, CASE WHEN b.cod_nivel_log IS NULL THEN 'N' ELSE 'S' END as log_principal
         FROM etl2.log_nivel a
              LEFT JOIN (
                 SELECT a.cod_historico, b.cod_interface, c.cod_nivel_log
                 FROM etl2.historico a
                    JOIN etl2.passo b ON (a.cod_projeto = b.cod_projeto)
                    JOIN etl2.interface c ON (b.cod_interface = c.cod_interface)
                 WHERE b.gerar_log = 'S' 
              ) b ON (a.cod_nivel = b.cod_nivel_log AND b.cod_interface = p_cod_interface AND b.cod_historico = p_cod_historico)
         WHERE (p_cod_nivel_filtro IS NULL OR a.cod_nivel = p_cod_nivel_filtro)
         ORDER BY a.cod_nivel
      )
      LOOP     
         gerar_arquivo_log(v_cod_nivel.cod_nivel, v_cod_nivel.log_principal, v_nc_file_name);     
      END LOOP;
   EXCEPTION
      WHEN e_erro_execucao THEN
         gerar_arquivo_log_erro(v_mensagem, p_tipo_mensagem);
   END SP_GERAR_LOG;
   
   PROCEDURE SP_ENVIAR_EMAIL (
      p_cod_historico    etl2.historico.cod_historico%TYPE,
      p_enviar_anexos    CHAR := 'S',
      p_anexo_adicional  VARCHAR2 := NULL
   ) AS
      v_mail_body CLOB;
      v_historico etl2.vw_historico%ROWTYPE;
      v_attachment_list VARCHAR2(4000);
      --v_count_directory NUMBER;
   BEGIN
      v_mail_body := NULL;
      FOR x IN (SELECT * FROM etl2.historico_log_html WHERE cod_historico = p_cod_historico AND aparece_email = 'S')
      LOOP  
         v_mail_body := v_mail_body || x.html_text || chr(13);
      END LOOP;            
      
      SELECT * INTO v_historico
      FROM etl2.vw_historico a
      WHERE cod_historico = p_cod_historico;
     
      v_mail_body := v_mail_body || '<br/><br/><div><i>' || c_mail_foot_note || '</i><div/><br/><br/>';
      
      v_attachment_list := NULL;
      --IF v_count_directory = 0 THEN 
         IF v_historico.nome_arquivo_log IS NOT NULL THEN 
            v_attachment_list := v_attachment_list || v_historico.diretorio_atual || '/' || v_historico.nome_arquivo_log;
         END IF;
         IF v_historico.nome_arquivo_nao_carreg IS NOT NULL AND v_historico.nome_arquivo_log IS NOT NULL THEN 
            v_attachment_list := v_attachment_list || ',' || v_historico.diretorio_atual || '/' || v_historico.nome_arquivo_nao_carreg;
         END IF;
         IF p_anexo_adicional IS NOT NULL THEN
            v_attachment_list := v_attachment_list || ',' || v_historico.diretorio_atual || '/' || p_anexo_adicional;
         END IF;
      --END IF;
      
      DBMS_OUTPUT.PUT_LINE('ANEXOS: '||v_attachment_list);
      
      tools.sp_send_mail (
         ReceiverAddress => v_historico.email_usuario, 
         arquivo         => v_attachment_list, 
         subject         => 'Resultado da Importação/Atualização ' || v_historico.nome_projeto, 
         message         => v_mail_body,
         is_html         => 'S'
      );     
      
      DELETE FROM etl2.historico_log_html WHERE cod_historico = p_cod_historico;
   END SP_ENVIAR_EMAIL;
   
   PROCEDURE SP_EXECUTAR_INTERFACE_PROCESS(
      p_cod_historico  etl2.historico.cod_historico%TYPE,
      p_cod_interface  etl2.interface.cod_interface%TYPE
   ) AS
      CURSOR cur_interface IS 
         SELECT cod_interface, esquema_processamento, nome_procedimento, cod_interface_tabela_externa
         FROM etl2.vw_interface
         WHERE cod_interface = p_cod_interface;
      
      v_interface cur_interface%ROWTYPE;
      v_historico etl2.vw_historico%ROWTYPE;
      v_query CLOB;
   BEGIN
      OPEN cur_interface;
      FETCH cur_interface INTO v_interface;
      CLOSE cur_interface;
      
      SELECT * INTO v_historico
      FROM etl2.vw_historico
      WHERE cod_historico = p_cod_historico;   
      
      IF v_interface.cod_interface_tabela_externa is not null then
          EXECUTE IMMEDIATE 'GRANT SELECT ON ETL2.'||ETL2.PACK_ETL.FN_GET_NOME_TABELA(v_interface.cod_interface, v_historico.cod_historico, 'externa')||' TO '||v_interface.esquema_processamento;
      END IF;
      
      v_query := '';
      v_query := v_query || 'DECLARE ';
      v_query := v_query || '   proj_info ETL2.PACK_ETL.rec_projeto_info; ';
      v_query := v_query || 'BEGIN ';
      v_query := v_query || '   proj_info.cod_projeto          := :1; ';
      v_query := v_query || '   proj_info.data_inicio          := :2; ';
      v_query := v_query || '   proj_info.data_atual           := :3; ';
      v_query := v_query || '   proj_info.nome_arquivo_entrada := :4; ';
      v_query := v_query || '   proj_info.characterset         := :5; ';
      v_query := v_query || '   proj_info.cod_historico        := :6; ';
      v_query := v_query || '   proj_info.cod_interface        := :7; ';
      v_query := v_query || '   '||v_interface.esquema_processamento||'.'||v_interface.nome_procedimento||'(proj_info);' ;
      v_query := v_query || 'END; ';
      
      --DBMS_OUTPUT.PUT_LINE('processamento:  '||v_query);
      
      EXECUTE IMMEDIATE v_query 
      USING
         v_historico.cod_projeto,
         v_historico.data_inicio,
         SYSDATE,
         v_historico.nome_arquivo,
         vp_characterset,
         v_historico.cod_historico,
         v_interface.cod_interface;
   END;
   
   PROCEDURE SP_EXECUTAR_AGENDA_EMPRESA(
      p_cod_empresa admin.empresa.cod_empresa%TYPE
   ) AS
      CURSOR cur_historico_agendado IS
         SELECT *
         FROM etl2.vw_historico_execucao a
         WHERE a.sigla_status = c_st_agendado
            AND a.cod_empresa = p_cod_empresa
         ORDER BY a.cod_historico;
      
      v_agendado    cur_historico_agendado%ROWTYPE;
      vCodHistorico NUMBER;
      vNewStatus    HISTORICO.STATUS%TYPE;
   BEGIN
      OPEN cur_historico_agendado;
      LOOP
         FETCH cur_historico_agendado INTO v_agendado;
         EXIT WHEN cur_historico_agendado%NOTFOUND;
         
         ETL2.PACK_ETL.SP_EXECUTAR_PROJETO (
            p_cod_projeto        => v_agendado.cod_projeto,
            p_cod_usuario_perfil => v_agendado.cod_usuario_perfil,
            p_nome_arquivo       => v_agendado.nome_arquivo,
            p_cod_passo_inicial  => NULL,
            p_cod_passo_final    => NULL,
            p_saida_dbms         => 'N',
            p_characterset       => NULL,
            p_cod_historico      => v_agendado.cod_historico,
            p_enviar_email       => 'S',
            p_email_adicional    => NULL,
            p_enviar_anexos      => 'S',
            p_remover_tabelas_$  => 'S'
         );
      END LOOP;
      CLOSE cur_historico_agendado;
   END SP_EXECUTAR_AGENDA_EMPRESA; 
   
   PROCEDURE SP_CRIAR_JOB_AGENDAMENTO AS   
      PRAGMA AUTONOMOUS_TRANSACTION;
         CURSOR cur_empresa IS
            SELECT DISTINCT a.cod_empresa
            FROM etl2.vw_historico_execucao a
            WHERE a.sigla_status = c_st_agendado;
         
         v_empresa cur_empresa%ROWTYPE;
         v_num_job NUMBER;
   BEGIN
      OPEN cur_empresa;
      LOOP
         FETCH cur_empresa INTO v_empresa;
         EXIT WHEN cur_empresa%NOTFOUND;
        
         DBMS_JOB.SUBMIT(v_num_job, 'BEGIN
                                       etl2.pack_etl.SP_EXECUTAR_AGENDA_EMPRESA('||v_empresa.cod_empresa||');
                                    END; '
                          , systimestamp, null);
         COMMIT;
      END LOOP;
      CLOSE cur_empresa;
   END SP_CRIAR_JOB_AGENDAMENTO;
END PACK_ETL;
/
