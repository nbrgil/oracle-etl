CREATE OR REPLACE PACKAGE                                              GEOCODER
IS
  TYPE SegmentType IS RECORD ( 
     StreetTypeId          NUMBER,        -- id tipo do logradouro
     StreetType            VARCHAR2(30),  -- tipo do logradouro (Rua, Av., Rodovia)
     StreetTypePhonetic    VARCHAR2(30),  -- fonética do tipo do logradouro
     StreetPrefixId        NUMBER,        -- id prefixo do logradouro
     StreetPrefix          VARCHAR2(30),  -- prefixo do logradouro (Prof., Dr., Vereador)
     StreetPrefixPhonetic  VARCHAR2(30),  -- fonética do prefixo do logradouro
     StreetName            VARCHAR2(100), -- nome do lagradouro
     StreetNamePhonetic    VARCHAR2(100), -- fonética do nome do lagradouro
     HouseNumber           NUMBER,        -- número  
     Side                  VARCHAR2(1),   -- lado (não esta sendo usado)
     RelaxedHouseNumber    NUMBER,        -- número após o relaxamento
     ToleranciaUsada       NUMBER,        -- número após o relaxamento
     AcertouMais           NUMBER,        -- número após o relaxamento
     Exact                 CHAR(1),       -- recebe T ou F, T significa que o número encontrado é exato, F significa que o número encontrado é aproximado
     PostalCode4           VARCHAR2(4),   -- os 4 primeiros digitos do cep
     PostalCode5           VARCHAR2(5),   -- os 5 primeiros digitos do cep
     PostalCode            VARCHAR2(20),  -- cep
     NeighborhoodId        NUMBER,        -- id do bairro
     Neighborhood          VARCHAR2(100),  -- bairro
     NeighborhoodPhonetic  VARCHAR2(100),  -- fonética do bairro
     MunicipalityId        NUMBER,        -- id do municipio
     Municipality          VARCHAR2(100), -- municipio
     MunicipalityPhonetic  VARCHAR2(100), -- fonética do municipio
     StateId               NUMBER,        -- id do estado
     State                 VARCHAR2(50),  -- estado
     StatePhonetic         VARCHAR2(50),  -- fonética do estado
     CountryId             NUMBER,        -- id do pais    
     Country               VARCHAR2(50),  -- pais    
     CountryPhonetic       VARCHAR2(50),  -- fonética do pais
     RoadId                NUMBER,        -- id do segmento
     SegmentId             NUMBER,        -- id do segmento
     StartHouseNumber      NUMBER,        -- numeração inicial do segmento
     EndHouseNumber        NUMBER,        -- numeração final do segmento
     Longitude             NUMBER,        -- longitude
     Latitude              NUMBER,        -- latitude
     Point                 VARCHAR2(50),  -- função de ponto
     ErrorMessage          VARCHAR2(11),   -- vetor binario (0s e 1s) indicando quais os campos do endereço de entrada casaram com os campos do endereço encontrado na base de dados 
     MatchValue            NUMBER,        -- quanto maior o valor maior a semelhança do endereço retornado com o endereço de entrada (de 0 a 100)
     MatchMode             VARCHAR2(30)
  );
/*
  TYPE RoadType IS RECORD ( 
      road_id NUMBER,
      hn NUMBER,
      hn_side VARCHAR2(1),
      longitude NUMBER,
      latitude NUMBER,
      road_segment_id NUMBER
  );
  */
  TYPE SegmentTableType IS TABLE OF SegmentType; 
  --TYPE RoadTableType IS TABLE OF RoadType; 
  TYPE v_number_array IS TABLE OF NUMBER(38) INDEX BY BINARY_INTEGER; 
  TYPE v_string_array IS TABLE OF VARCHAR2(2000) INDEX BY BINARY_INTEGER;

  FUNCTION Geocode(addr_type IN VARCHAR2, addr_prefix IN VARCHAR2, addr_name IN VARCHAR2,
                   addr_number IN NUMBER, addr_postal_code IN VARCHAR2, addr_neighborhood IN VARCHAR2,
                   addr_municipality IN VARCHAR2, addr_state IN VARCHAR2, addr_country IN VARCHAR2,
                   match_mode IN VARCHAR2, type_return IN VARCHAR2, usar_offset BOOLEAN := FALSE, numero_obrigatorio BOOLEAN := FALSE) RETURN GC_TP_GEO_ADDR_ARRAY;

  FUNCTION GeocodeAll(scheme IN VARCHAR2, addr_type IN VARCHAR2, addr_prefix IN VARCHAR2, addr_name IN VARCHAR2, addr_number IN NUMBER, addr_postal_code IN VARCHAR2,
                      addr_neighborhood IN VARCHAR2, addr_municipality IN VARCHAR2, addr_state IN VARCHAR2, addr_country IN VARCHAR2, match_mode IN VARCHAR2) RETURN GC_TP_GEO_ADDR_ARRAY;
  FUNCTION GeocodeReverse(scheme IN VARCHAR2, x IN NUMBER, y IN NUMBER) RETURN GC_TP_GEO_ADDR;

  FUNCTION GetMatchValue(ErrorMessage VARCHAR2) RETURN NUMBER;
  FUNCTION GetErrorMessage(addr_Formatted IN GC_TP_GEO_ADDR, addr IN GC_TP_GEO_ADDR) RETURN VARCHAR2;
  FUNCTION PositionNumber(pNumber NUMBER, vLeftStartHN NUMBER, vLeftEndHN NUMBER, vRightStartHN NUMBER, vRightEndHN NUMBER, vLeftScheme VARCHAR2, vRightScheme VARCHAR2) RETURN NUMBER;
  FUNCTION GetRoadPoint(pGeometry SDO_GEOMETRY, pNumber NUMBER, pLeftScheme VARCHAR2, 
                        pRightScheme VARCHAR2, pLeftStartHN NUMBER, pLeftEndHN NUMBER,
                        pRightStartHN NUMBER, pRightEndHN NUMBER, pSide CHAR := NULL) RETURN SDO_GEOMETRY;
  FUNCTION CheckErrorMessageAttribute(pErrorMessage VARCHAR2, pNomeAtributo VARCHAR2) RETURN CHAR;
  FUNCTION FindNumberSide(pNumber NUMBER, pLeftScheme VARCHAR2, pRightScheme VARCHAR2) RETURN CHAR;
  
  FUNCTION GetSegmentTablePipelined RETURN SegmentTableType PIPELINED;
END geocoder;
/


CREATE OR REPLACE PACKAGE BODY                                                                "GEOCODER" IS  
      
-- CONSTANTES DO PACOTE
   cScheme               CONSTANT VARCHAR2(11) := 'MAPA_URBANO';
   cSchemeType_Impar     CONSTANT VARCHAR2(50) := 'IMPAR';
   cSchemeType_Par       CONSTANT VARCHAR2(50) := 'PAR';
   cSchemeType_SemRegra  CONSTANT VARCHAR2(50) := 'MIX';
 
   cSearchType_ParImpar  CONSTANT NUMBER := 1;
   cSearchType_Indef     CONSTANT NUMBER := 2;
   cSearchType_Todos     CONSTANT NUMBER := 3;
   
   cToleranciaRelaxNumero CONSTANT NUMBER := 300;
   
   -- VARIAVEIS DE PACOTE
   vUsarOffset BOOLEAN      := FALSE;
   vTypeReturn VARCHAR2(50) := NULL;
   vRoadTable SegmentTableType;
   v_tab_road_id GC_TP_ROAD_ARRAY;
   

   FUNCTION GetProjectReverse(lineString VARCHAR2, x NUMBER, y NUMBER) 
   RETURN NUMBER AS 
   /*--------------------------------------------------------------------------
     DESCRICAO: 
        * CHAMADA A BIBLIOTECA JAVA PARA GEOCODER REVERSO
    --------------------------------------------------------------------------*/
   LANGUAGE JAVA NAME 'com.vividsolutions.jts.linearref.LinearReference.ProjectReverse(java.lang.String, double, double) return java.lang.String';
   
   
   FUNCTION FindNumberSide(pNumber NUMBER, pLeftScheme VARCHAR2, pRightScheme VARCHAR2)  RETURN CHAR 
   AS
    /*--------------------------------------------------------------------------
     DESCRICAO: 
        * RETORNA O LADO QUE O NÚMERO FICA EM UM RUA
     PARAMETROS:
        * pNumber       = NUMERO A SER ANALISADO;
        * pLeftScheme   = FORMATO DA NUMERACAO NO LADO ESQUERDO DA VIA;
        * pRightScheme  = FORMATO DA NUMERACAO NO LADO DIREITO DA VIA;
        * pNumberType   = INDICA SE O NUMERO É PAR OU IMPAR;
        
     RETORNO:
        * CARACTERE COM 'L', 'R' OU NULO
    --------------------------------------------------------------------------*/
      vSideNumber CHAR;
      vNumberType VARCHAR2(10);
   BEGIN
      -- Verifica se o segmento é valido e adiciona na lista
      vNumberType := NULL;
      vSideNumber := NULL;
 
      IF (TOOLS.FN_PAR_IMPAR(pNumber) = 'PAR') THEN
         vNumberType := cSchemeType_Par;
      ELSE
         vNumberType := cSchemeType_Impar;
      END IF;
      
      IF (pLeftScheme = vNumberType) THEN
         vSideNumber := 'L';
      ELSIF (pRightScheme = vNumberType) THEN
         vSideNumber := 'R'; 
      ELSIF (pLeftScheme = cSchemeType_SemRegra) THEN
         vSideNumber := 'L';
      ELSIF (pLeftScheme = cSchemeType_SemRegra) THEN
         vSideNumber := 'R';
      ELSE 
         vSideNumber := NULL;
      END IF;
 
      RETURN vSideNumber;
   END;

   FUNCTION GeocodeBySquare(pAddrFormatted IN GC_TP_GEO_ADDR) 
   RETURN GC_TP_GEO_ADDR_ARRAY AS
   /*--------------------------------------------------------------------------
     DESCRICAO: 
        * BUSCA ENDEREÇO NA TABELA DE PRAÇAS
     PARAMETROS:
        * pAddrFormatted = ENDEREÇO FORMATADO
     RETURN
        * 
    --------------------------------------------------------------------------*/
    
      CURSOR curSquare(pcrUfmunCod NUMBER, 
                       pcrStreetNamePhonetic VARCHAR2) IS
         SELECT a.name as nome, a.ufmun_cod, b.municipality, b.state_id, 
                c.state, c.country_id, d.country, 
                a.centroide.sdo_point.x as longitude, 
                a.centroide.sdo_point.y as latitude
         FROM mapa_urbano.praca a
             JOIN mapa_urbano.gc_municipality b ON (a.ufmun_cod = b.ufmun_cod)
             JOIN mapa_urbano.gc_state c ON (b.state_id = c.state_id)
             JOIN mapa_urbano.gc_country d ON (c.country_id = d.country_id)
         WHERE a.name_phonetic = pcrStreetNamePhonetic
           AND a.ufmun_cod = pcrUfmunCod;
 
      vSquare curSquare%ROWTYPE;
      vector_addr   GC_TP_GEO_ADDR_ARRAY;
      addr          GC_TP_GEO_ADDR; -- tipo endereço
 
   BEGIN
 
      vector_addr := GC_TP_GEO_ADDR_ARRAY();
      addr := GC_TP_GEO_ADDR();
 
      OPEN curSquare(pAddrFormatted.MunicipalityId, pAddrFormatted.StreetNamePhonetic);
      LOOP
          FETCH curSquare INTO vSquare;
          EXIT WHEN curSquare%NOTFOUND;
 
          addr.MunicipalityId := vSquare.Ufmun_Cod;
          addr.Municipality   := vSquare.Municipality;
          addr.StateId        := vSquare.State_Id; 
          addr.State          := vSquare.State;
          addr.CountryId      := vSquare.Country_Id;
          addr.Country        := vSquare.Country;
          addr.Longitude      := vSquare.Longitude;
          addr.Latitude       := vSquare.Latitude;
          addr.StreetName     := vSquare.Nome;
          addr.ErrorMessage := GetErrorMessage(pAddrFormatted, addr);
          addr.MatchValue   := GetMatchValue(addr.ErrorMessage);
 
          vector_addr.EXTEND(1);
          vector_addr(vector_addr.COUNT):= addr;
 
      END LOOP;
 
      CLOSE curSquare;
 
      RETURN vector_addr;
 
   END GeocodeBySquare;
   
    FUNCTION PositionNumber(pNumber NUMBER, vLeftStartHN NUMBER, vLeftEndHN NUMBER, vRightStartHN NUMBER, vRightEndHN NUMBER, vLeftScheme VARCHAR2, vRightScheme VARCHAR2) RETURN NUMBER
    AS

    -- Esta função recebe um número de entrada, os números de iniciais e finais de cada lado de uma via (segmento) e se o lado é par ou impar,
    -- e retorna a posição do número (em %) naquele segmento.

    v_position NUMBER; --a posição (%) onde o ponto esta no segmento

    BEGIN
        -- Casa o número do endereço de entrada formatado com o lado par ou impar da via (segmento)
        IF (EVEN_ODD(pNumber) = 'PAR') THEN -- se o número de entrada é par
            IF (vLeftScheme = 'PAR') THEN -- se o lado esquerdo da via é par
                IF ((vLeftEndHN <> vLeftStartHN) AND pNumber IS NOT NULL) THEN
                    v_position := ((pNumber - vLeftStartHN) * 100) / (vLeftEndHN - vLeftStartHN);
                ELSE
                    v_position := 50;
                END IF;
            ELSE -- se o lado direito da via é par
                IF ((vRightEndHN <> vRightStartHN) AND pNumber IS NOT NULL) THEN
                    v_position := (((pNumber - vRightStartHN) * 100) / (vRightEndHN - vRightStartHN));
                ELSE
                    v_position := 50;
                END IF;
            END IF;

        ELSE -- se o número da entrada é impar
            IF (vLeftScheme = 'IMPAR') THEN -- se o lado esquerno da via é impar
                IF ((vLeftEndHN <> vLeftStartHN) AND pNumber IS NOT NULL) THEN
                    v_position := ((pNumber - vLeftStartHN) * 100) / (vLeftEndHN - vLeftStartHN);
                ELSE
                    v_position := 50;
                END IF;
            ELSE -- se o lado direito da via é impar
                IF ((vRightEndHN <> vRightStartHN) AND pNumber IS NOT NULL) THEN
                    v_position := ((pNumber - vRightStartHN) * 100) / (vRightEndHN - vRightStartHN);
                ELSE
                    v_position := 50;
                END IF;
            END IF;
        END IF;

        --para a interpolação o valor de v_position deve estar entre 0 e 100, então:
        IF (v_position < 0) THEN
            v_position := 0;
        END IF;
        IF (v_position > 100) THEN
            v_position := 100;
        END IF;

        -- Limita o valor de v_position a no mínimo 1
        IF (v_position < 1) THEN
            v_position := 1;
        END IF;

        RETURN v_position;

    END;

   FUNCTION GeocodeFullAddress(
      scheme IN VARCHAR2, 
      addr_Unformatted IN GC_TP_GEO_ADDR, 
      addr_Formatted IN GC_TP_GEO_ADDR, 
      v_mm_type IN CHAR, 
      v_mm_prefix IN CHAR, 
      v_mm_number IN CHAR, 
      v_mm_basename IN CHAR, 
      v_mm_zip IN CHAR,
      v_mm_unique_zip IN CHAR,
      match_mode IN VARCHAR2
   ) RETURN GC_TP_GEO_ADDR_ARRAY 
   AS
   --   v_roads        V_NUMBER_ARRAY;
      vSegments      SegmentTableType; 
     -- vRoads         RoadTableType; 
      
      addr            GC_TP_GEO_ADDR; 
      vector_addr     GC_TP_GEO_ADDR_ARRAY;
      vector_addr_out GC_TP_GEO_ADDR_ARRAY;
      
      vPointGeometry  SDO_GEOMETRY;
      vPositionNumber NUMBER;
      vGeometry       SDO_GEOMETRY;
      vLeftStartHN    NUMBER;
      vLeftEndHN      NUMBER;
      vRightStartHN   NUMBER;
      vRightEndHN     NUMBER;
      vLeftScheme     VARCHAR2(50);
      vRightScheme    VARCHAR2(50);
      vNumberSide     CHAR;
      vNumberType     VARCHAR2(50);
      vResultCount    NUMBER := 0;
      vRoadId         NUMBER;
      
      vHNPosition     VARCHAR2(50);
      
      cPesoPrefixo    CONSTANT NUMBER := 5; 
      cPesoTipo       CONSTANT NUMBER := 5;
      cPesoNum        CONSTANT NUMBER := 5;
      cPesoCEP        CONSTANT NUMBER := 30; 
      cPesoNome       CONSTANT NUMBER := 50;
      cPesoBairro     CONSTANT NUMBER := 5;
      
      vCenterGeom SDO_GEOMETRY;
      vSegmentIdControl NUMBER;
      
      -- BUSCA EXATA:
      --   Prefixo deve estar correto (mesmo se for nulo);
      --   Tipo deve estar correto (mesmo se for nulo);
      --   Numeração deve estar correta a não ser quano o valor é nulo;
      --   Nome da rua deve estar correto a não ser quando o valor é nulo;
      --   CEP deve estar correto a não ser quando o valor é nulo
      --   * Obs: Nunca nome da rua e CEP estarão nulos ao mesmo tempo devido a uma validação anterior
      CURSOR curBuscaExactSemNum IS          
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 null as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
            AND A.TYPE_ID = addr_Formatted.StreetTypeId
            AND NVL(A.PREFIX_ID,0) = addr_Formatted.StreetPrefixId
            AND A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC)
            --AND (addr_Formatted.PostalCode IS NULL OR NVL(A.POSTAL_CODE,0) = NVL(addr_Formatted.PostalCode,0))
            AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (A.POSTAL_CODE_5 = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000')   )
            ;
                    
      CURSOR curBuscaExactComNum IS          
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 addr_Formatted.HouseNumber as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
            AND A.TYPE_ID = addr_Formatted.StreetTypeId
            AND NVL(A.PREFIX_ID,0) = addr_Formatted.StreetPrefixId
            AND A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC)
            AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (SUBSTR(A.POSTAL_CODE, 1, 5) = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000'))
            AND addr_Formatted.HouseNumber BETWEEN START_HN AND END_HN
            ;
      
      --   Exact com tipo relaxado;
      CURSOR curBuscaRelaxTypeSemNum IS        
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 null as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
          AND (NVL(A.PREFIX_ID,0) = addr_Formatted.StreetPrefixId)
          AND (A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC))
          AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (SUBSTR(A.POSTAL_CODE, 1, 5) = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000'))
          ;
         
      CURSOR curBuscaRelaxTypeComNum IS        
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 addr_Formatted.HouseNumber as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
            AND (NVL(A.PREFIX_ID,0) = addr_Formatted.StreetPrefixId)
            AND (A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC))
            AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (SUBSTR(A.POSTAL_CODE, 1, 5) = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000'))
            AND addr_Formatted.HouseNumber BETWEEN START_HN AND END_HN
            ;
         
      
      --   Exact com prefixo e tipo relaxados;
      CURSOR curBuscaRelaxPrefixSemNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 null as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
          AND (A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC))
          AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (SUBSTR(A.POSTAL_CODE, 1, 5) = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000'))
          ;
          
      CURSOR curBuscaRelaxPrefixComNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 addr_Formatted.HouseNumber as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
          AND (A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC))
          AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (SUBSTR(A.POSTAL_CODE, 1, 5) = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000'))
          AND addr_Formatted.HouseNumber BETWEEN START_HN AND END_HN
          ;

      
      --   Exact com prefixo, tipo e numeração relaxados;
      CURSOR curBuscaRelaxHNSemNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 null as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
          AND (A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC))
          AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (SUBSTR(A.POSTAL_CODE, 1, 5) = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000'))
          ;
          
      CURSOR curBuscaRelaxHNComNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn as hn, 
                 a.center_hn_side as hn_side,
                 a.relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 a.center_road_seg_id,
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM ( 
             SELECT CASE WHEN addr_Formatted.HouseNumber <= start_hn THEN start_hn
                         WHEN addr_Formatted.HouseNumber >= end_hn THEN end_hn
                         ELSE addr_Formatted.HouseNumber END relaxed_hn, -- Usado no cursor de segmentos
                    a.*
             FROM MAPA_URBANO.VW_GC_ROAD A
             WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId          
             AND (A.PHONETIC = NVL(addr_Formatted.StreetNamePhonetic, A.PHONETIC))
             AND (addr_Formatted.PostalCode IS NULL OR A.POSTAL_CODE IS NULL OR A.POSTAL_CODE = addr_Formatted.PostalCode OR (SUBSTR(A.POSTAL_CODE, 1, 5) = SUBSTR(addr_Formatted.PostalCode, 1, 5) AND SUBSTR(addr_Formatted.PostalCode, -3) = '000'))
             AND addr_Formatted.HouseNumber BETWEEN (START_HN - cToleranciaRelaxNumero) AND (END_HN + cToleranciaRelaxNumero)
          ) a;
      
      --   Exact com prefixo, tipo e numeração e nome da rua relaxados;
      CURSOR curBuscaRelaxBaseNameSemNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 null as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId          
          AND (NVL(A.POSTAL_CODE,0) = NVL(addr_Formatted.PostalCode,0));
                            
      CURSOR curBuscaRelaxBaseNameComNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 a.relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM ( 
             SELECT CASE WHEN addr_Formatted.HouseNumber <= start_hn THEN start_hn
                         WHEN addr_Formatted.HouseNumber >= end_hn THEN end_hn
                         ELSE addr_Formatted.HouseNumber END relaxed_hn, -- Usado no cursor de segmentos
                    a.*
             FROM MAPA_URBANO.VW_GC_ROAD A
             WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId          
             AND addr_Formatted.HouseNumber BETWEEN (START_HN - cToleranciaRelaxNumero) AND (END_HN + cToleranciaRelaxNumero)
             AND (NVL(A.POSTAL_CODE,0) = NVL(addr_Formatted.PostalCode,0))
          ) a;
      
      
      --   Exact com prefixo, tipo e numeração e cep relaxados;
      CURSOR curBuscaRelaxPostalCodeSemNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 null as relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId          
          AND (addr_Formatted.StreetNamePhonetic IS NOT NULL AND A.PHONETIC = addr_Formatted.StreetNamePhonetic);
      
      CURSOR curBuscaRelaxPostalCodeComNum IS      
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 a.relaxed_hn,
                 null as tolerancia_usada,
                 null as acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM (          
             SELECT CASE WHEN addr_Formatted.HouseNumber <= start_hn THEN start_hn
                         WHEN addr_Formatted.HouseNumber >= end_hn THEN end_hn
                         ELSE addr_Formatted.HouseNumber END relaxed_hn, -- Usado no cursor de segmentos
                    a.*
             FROM MAPA_URBANO.VW_GC_ROAD A
             WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId          
             AND addr_Formatted.HouseNumber BETWEEN (START_HN - cToleranciaRelaxNumero) AND (END_HN + cToleranciaRelaxNumero)
             AND (addr_Formatted.StreetNamePhonetic IS NOT NULL AND A.PHONETIC = addr_Formatted.StreetNamePhonetic)
         ) a
         ;
      
      --   Exact com prefixo, tipo, numeração, nome da rua ou cep relaxados;
      CURSOR curBuscaRelaxAllSemNum IS
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 null as relaxed_hn,
                 null as tolerancia_usada,
                 CASE WHEN PHONETIC = addr_Formatted.StreetNamePhonetic THEN 1 ELSE 0 END + CASE WHEN A.POSTAL_CODE = addr_Formatted.PostalCode THEN 1 ELSE 0 END ACERTOU_MAIS,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM MAPA_URBANO.VW_GC_ROAD A
          WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
          AND ((addr_Formatted.StreetNamePhonetic IS NOT NULL AND PHONETIC = addr_Formatted.StreetNamePhonetic) OR 
               (addr_Formatted.PostalCode IS NOT NULL AND A.POSTAL_CODE = addr_Formatted.PostalCode));
      
      CURSOR curBuscaRelaxAllComNum IS      
          SELECT a.type_id, 
                 a.type, 
                 null as streettypephonetic, 
                 a.prefix_id, 
                 a.prefix, 
                 null as streetprefixphonetic, 
                 a.base_name, 
                 a.phonetic, 
                 a.center_hn  as hn, 
                 a.center_hn_side as hn_side,
                 a.relaxed_hn,
                 a.tolerancia_usada,
                 a.acertou_mais,
                 'F' as exact, 
                 null as postalcode4, 
                 null as postalcode5, 
                 a.postal_code,
                 a.neighborhood_id, 
                 a.neighborhood, 
                 null as neighborhoodphonetic, 
                 a.ufmun_cod, 
                 a.municipality, 
                 null as MunicipalityPhonetic, 
                 a.State_Id,
                 a.State, 
                 null as StatePhonetic, 
                 a.Country_Id, 
                 a.Country, 
                 null as CountryPhonetic, 
                 a.road_id, 
                 center_road_seg_id as road_segment_id,  
                 a.START_HN, 
                 a.END_HN, 
                 a.center_long as longitude, 
                 a.center_lat as latitude,
                 'POINT(' || TRIM(REPLACE(a.center_long, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(a.center_lat, ',', '.')), chr(13), NULL) || ')' as point,
                 null as ErrorMessage, 
                 null as MatchValue,  
                 null as MatchMode
          FROM (
             SELECT CASE WHEN addr_Formatted.HouseNumber <= start_hn THEN start_hn
                         WHEN addr_Formatted.HouseNumber >= end_hn THEN end_hn
                         ELSE addr_Formatted.HouseNumber END relaxed_hn, -- Usado no cursor de segmentos
                    CASE WHEN addr_Formatted.HouseNumber <= start_hn THEN start_hn - addr_Formatted.HouseNumber
                         WHEN addr_Formatted.HouseNumber >= end_hn THEN addr_Formatted.HouseNumber - end_hn
                         ELSE 0 END tolerancia_usada, 
                    CASE WHEN PHONETIC = addr_Formatted.StreetNamePhonetic THEN 1 ELSE 0 END + CASE WHEN A.POSTAL_CODE = addr_Formatted.PostalCode THEN 1 ELSE 0 END ACERTOU_MAIS,
                    a.*
             FROM MAPA_URBANO.VW_GC_ROAD A
             WHERE A.UFMUN_COD = addr_Formatted.MunicipalityId
             AND addr_Formatted.HouseNumber BETWEEN (START_HN - cToleranciaRelaxNumero) AND (END_HN + cToleranciaRelaxNumero)
             AND ((addr_Formatted.StreetNamePhonetic IS NOT NULL AND PHONETIC = addr_Formatted.StreetNamePhonetic) OR 
                  (addr_Formatted.PostalCode IS NOT NULL AND A.POSTAL_CODE = addr_Formatted.PostalCode))
          ) a;

      CURSOR curBuscaMunicipioCepUnico IS
          SELECT 
            null as type_id, 
            null as type, 
            null as streettypephonetic, 
            null as prefix_id, 
            null as prefix, 
            null as streetprefixphonetic, 
            null as base_name, 
            null as phonetic, 
            null as hn, 
            null as hn_side,
            null as relaxed_hn,
            null as tolerancia_usada,
            null as acertou_mais,
            'F' as exact, 
            substr(a.faixa_inicial, 1, 4) as postalcode4, 
            substr(a.faixa_inicial, 1, 5) as postalcode5, 
            a.faixa_inicial as postal_code,
            null as neighborhood_id, 
            null as neighborhood, 
            null as neighborhoodphonetic, 
            b.ufmun_cod, 
            b.municipio as municipality, 
            b.phonetic_municipality as MunicipalityPhonetic, 
            b.state_id,
            b.estado as State, 
            null as StatePhonetic, 
            b.Country_Id, 
            null as Country, 
            null as CountryPhonetic, 
            null as road_id, 
            null as road_segment_id,  
            null as START_HN, 
            null as END_HN, 
            b.sede.sdo_point.x as longitude, 
            b.sede.sdo_point.y as latitude,
            'POINT(' || TRIM(REPLACE(b.sede.sdo_point.x, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(b.sede.sdo_point.y, ',', '.')), chr(13), NULL) || ')' as point,
            null as ErrorMessage, 
            50 as MatchValue,  
            null as MatchMode
         FROM mapa_urbano.municipio_faixa_cep a
              JOIN mapa_urbano.vw_municipio b ON (a.ufmun_cod = b.ufmun_cod)
              JOIN mapa_urbano.gc_country c ON (b.country_id = c.country_id)
         WHERE a.ufmun_cod = addr_Formatted.MunicipalityId
           AND (to_number(a.faixa_inicial) = to_number(a.faixa_final));

      CURSOR curBuscaPostalCodeCepUnico IS
          SELECT 
            null as type_id, 
            null as type, 
            null as streettypephonetic, 
            null as prefix_id, 
            null as prefix, 
            null as streetprefixphonetic, 
            null as base_name, 
            null as phonetic, 
            null as hn, 
            null as hn_side,
            null as relaxed_hn,
            null as tolerancia_usada,
            null as acertou_mais,
            'F' as exact, 
            substr(a.faixa_inicial, 1, 4) as postalcode4, 
            substr(a.faixa_inicial, 1, 5) as postalcode5, 
            a.faixa_inicial as postal_code,
            null as neighborhood_id, 
            null as neighborhood, 
            null as neighborhoodphonetic, 
            b.ufmun_cod, 
            b.municipio as municipality, 
            b.phonetic_municipality as MunicipalityPhonetic, 
            b.state_id,
            b.estado as State, 
            null as StatePhonetic, 
            b.Country_Id, 
            null as Country, 
            null as CountryPhonetic, 
            null as road_id, 
            null as road_segment_id,  
            null as START_HN, 
            null as END_HN, 
            b.sede.sdo_point.x as longitude, 
            b.sede.sdo_point.y as latitude,
            'POINT(' || TRIM(REPLACE(b.sede.sdo_point.x, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(b.sede.sdo_point.y, ',', '.')), chr(13), NULL) || ')' as point,
            null as ErrorMessage, 
            50 as MatchValue,  
            null as MatchMode
         FROM mapa_urbano.municipio_faixa_cep a
              JOIN mapa_urbano.vw_municipio b ON (a.ufmun_cod = b.ufmun_cod)
              JOIN mapa_urbano.gc_country c ON (b.country_id = c.country_id)
         WHERE to_number(REPLACE(addr_Formatted.PostalCode,'-','')) BETWEEN to_number(a.faixa_inicial) AND to_number(a.faixa_final)
           AND to_number(a.faixa_inicial) = to_number(a.faixa_final);
   
  
   /*   CURSOR curBuscaSegmentoFull--(pRoadId NUMBER, pRoadSegmentId NUMBER, pPoint SDO_GEOMETRY, pExact VARCHAR2, pHNSide VARCHAR2, pHN NUMBER) 
      IS
         SELECT a.type_id,
                c.type,
                null as StreetTypePhonetic,  
                a.PREFIX_ID,        
                d.prefix,
                null as StreetPrefixPhonetic,
                a.BASE_NAME,
                a.PHONETIC,
                b.hn,
                b.hn_side,
                b.Exact,
                null as PostalCode4,
                null as PostalCode5,
                a.POSTAL_CODE,
                a.NEIGHBORHOOD_ID,
                e.Neighborhood,
                null as NeighborhoodPhonetic, 
                a.UFMUN_COD,
                f.Municipality,
                null as MunicipalityPhonetic,
                g.State_Id,
                g.State,
                null as StatePhonetic,
                h.Country_Id,
                h.Country,
                null as CountryPhonetic,
                b.road_id,
                b.road_segment_id, 
                a.START_HN,
                a.END_HN,
                b.point.sdo_point.x as longitude,
                b.point.sdo_point.y as latitude,
                'POINT(' || TRIM(REPLACE(b.point.sdo_point.x, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(b.point.sdo_point.y, ',', '.')), chr(13), NULL) || ')' as point,
                null as ErrorMessage,
                null as MatchValue, 
                null as MatchMode
        FROM (select * from table(GetSegmentTablePipelined)) b 
             --(select road_id, road_segment_id, point, exact, hn_side, hn  from table(GetSegmentTablePipelined)) b 
             --(select 567616 as road_id, 1076002611810 as road_segment_id, SDO_GEOMETRY('POINT(-1 -1)', 8307) as point, 'T' as Exact, 'L' as hn_side, 140 as hn from dual) b
             --(select pRoadId as road_id, pRoadSegmentId as road_segment_id, pPoint as point, pExact as Exact, pHNSide as hn_side, pHN as hn from dual) b
             JOIN MAPA_URBANO.vw_gc_road a ON (a.road_id = b.road_id)
             JOIN MAPA_URBANO.gc_type c ON (a.type_id = c.type_id)
             LEFT JOIN MAPA_URBANO.gc_prefix d ON (a.prefix_id = d.prefix_id)
             LEFT JOIN MAPA_URBANO.gc_neighborhood e ON (a.neighborhood_id = e.neighborhood_id)
             JOIN MAPA_URBANO.gc_municipality f ON (a.ufmun_cod = f.ufmun_cod)
             JOIN MAPA_URBANO.gc_state g ON (f.state_id = g.state_id)
             JOIN MAPA_URBANO.gc_country h ON (g.country_id = h.country_id); */
     
     CURSOR curBuscaSegmento IS
        SELECT x.type_id, 
               x.type, 
               null as streettypephonetic, 
               x.prefix_id, 
               x.prefix, 
               null as streetprefixphonetic, 
               x.base_name, 
               x.phonetic, 
               x.hn, 
               x.hn_side,
               null as relaxed_hn,
               null as tolerancia_usada,
               null as acertou_mais,
               CASE WHEN x.hn = addr_Formatted.HouseNumber THEN 'T' ELSE 'F' END as exact, 
               null as postalcode4, 
               null as postalcode5, 
               x.postal_code,
               x.neighborhood_id, 
               x.neighborhood, 
               null as neighborhoodphonetic, 
               x.ufmun_cod, 
               x.municipality, 
               null as MunicipalityPhonetic, 
               x.State_Id,
               x.State, 
               null as StatePhonetic, 
               x.Country_Id, 
               x.Country, 
               null as CountryPhonetic, 
               x.road_id, 
               x.road_segment_id,  
               CASE WHEN x.hn_side = 'L' THEN LEAST(L_START_HN,L_END_HN) ELSE LEAST(R_START_HN, R_END_HN) END AS START_HN,
               CASE WHEN x.hn_side = 'L' THEN GREATEST(L_START_HN,L_END_HN) ELSE GREATEST(R_START_HN, R_END_HN) END AS END_HN,
               x.point.sdo_point.x as longitude,
               x.point.sdo_point.y as latitude,
               'POINT(' || TRIM(REPLACE(x.point.sdo_point.x, ',', '.')) || ' ' || REPLACE(TRIM(REPLACE(x.point.sdo_point.y, ',', '.')), chr(13), NULL) || ')' as point,
               null as ErrorMessage,
               null as MatchValue,
               null as MatchMode
        FROM (
           SELECT b.*,
                  GetRoadPoint(b.geom, b.hn, b.left_scheme, b.right_scheme, b.l_start_hn, b.l_end_hn, b.r_start_hn, b.r_end_hn, b.hn_side) as point
           FROM (
              SELECT b1.*, dense_rank() over (order by ACERTOU_MAIS desc, TOLERANCIA_USADA, diferenca_numero) ranking
                 
              FROM (              
                 SELECT a.*,
                        b.hn as hn,
                        'R' as hn_side,
                        GREATEST(R_START_HN, R_END_HN) - LEAST(R_START_HN, R_END_HN) diferenca_numero,
                        b.TOLERANCIA_USADA,
                        b.ACERTOU_MAIS
                        --FindNumberSide(pHouseNumber, a.left_scheme, a.right_scheme) as hn_side
                 FROM MAPA_URBANO.VW_GC_ROAD_SEGMENT A
                 JOIN table(v_tab_road_id) b on (a.road_id = b.road_id)
                 --WHERE a.ROAD_ID = pRoadId
                 WHERE --a.ROAD_ID in (SELECT road_id from table(v_tab_road_id))
                   --AND 
                   ( 
                          --((EVEN_ODD(pHouseNumber) = LEFT_SCHEME OR LEFT_SCHEME = 'MIX') AND pHouseNumber BETWEEN LEAST(L_START_HN,L_END_HN) AND GREATEST(L_START_HN,L_END_HN)) OR 
                          ((EVEN_ODD(b.hn) = RIGHT_SCHEME OR RIGHT_SCHEME = 'MIX') AND b.hn BETWEEN LEAST(R_START_HN, R_END_HN) AND GREATEST(R_START_HN, R_END_HN))
                       )
                       
                 UNION ALL
                 
                 SELECT a.*,
                        b.hn as hn,
                        'L' as hn_side,
                        GREATEST(L_START_HN, L_END_HN) - LEAST(L_START_HN, L_END_HN) diferenca_numero,
                        b.TOLERANCIA_USADA,
                        b.ACERTOU_MAIS
                        --FindNumberSide(pHouseNumber, a.left_scheme, a.right_scheme) as hn_side
                 FROM MAPA_URBANO.VW_GC_ROAD_SEGMENT A
                 JOIN table(v_tab_road_id) b on (a.road_id = b.road_id)
                 WHERE --a.ROAD_ID = pRoadId
                   --AND 
                   ( 
                          ((EVEN_ODD(b.hn) = LEFT_SCHEME OR LEFT_SCHEME = 'MIX') AND b.hn BETWEEN LEAST(L_START_HN,L_END_HN) AND GREATEST(L_START_HN,L_END_HN))
                           
                         --OR ((EVEN_ODD(pHouseNumber) = RIGHT_SCHEME OR RIGHT_SCHEME = 'MIX') AND pHouseNumber BETWEEN LEAST(R_START_HN, R_END_HN) AND GREATEST(R_START_HN, R_END_HN))
                       )
              ) b1
              --ORDER BY diferenca_numero
           ) b
           --where rownum = 1
           where ranking = 1
        ) x
        ;
  
BEGIN
   vector_addr     := GC_TP_GEO_ADDR_ARRAY();  
   vector_addr_out := GC_TP_GEO_ADDR_ARRAY();  
   
   vRoadTable := SegmentTableType();
   vRoadTable.DELETE;

   -- Busca a sede do município por UFMUN_COD ou por CEP
   IF v_mm_unique_zip = 'T' THEN
      IF addr_Formatted.MunicipalityId = 0 THEN 
         OPEN curBuscaPostalCodeCepUnico;
         FETCH curBuscaPostalCodeCepUnico BULK COLLECT INTO vRoadTable;
         CLOSE curBuscaPostalCodeCepUnico;
      ELSE
         OPEN curBuscaMunicipioCepUnico;
         FETCH curBuscaMunicipioCepUnico BULK COLLECT INTO vRoadTable;
         CLOSE curBuscaMunicipioCepUnico;
      END IF;   
   --Se o número foi informado
   ELSIF (addr_Formatted.HouseNumber IS NOT NULL) THEN
       
       OPEN curBuscaExactComNum;
       FETCH curBuscaExactComNum BULK COLLECT INTO vRoadTable;
       CLOSE curBuscaExactComNum;
       
       --Se não encontrou nenhuma rua com EXACT
       IF vRoadTable.COUNT = 0 THEN 
          IF (v_mm_type = 'T') THEN 
             IF (v_mm_prefix = 'T') THEN
                IF (v_mm_number = 'T') THEN 
                   IF (v_mm_basename = 'T') THEN
                      IF (v_mm_zip = 'T') THEN
                         -- Todos os relaxamentos ativos => Relaxamento máximo
                         OPEN curBuscaRelaxAllComNum;
                         FETCH curBuscaRelaxAllComNum BULK COLLECT INTO vRoadTable;
                         CLOSE curBuscaRelaxAllComNum;
                      ELSE
                         -- Todos os relaxamentos, exceto CEP
                         OPEN curBuscaRelaxBaseNameComNum;
                         FETCH curBuscaRelaxBaseNameComNum BULK COLLECT INTO vRoadTable;
                         CLOSE curBuscaRelaxBaseNameComNum;
                      END IF;
                   ELSE
                      IF (v_mm_zip = 'T') THEN
                         -- Todos os relaxamentos, exceto nome da rua
                         OPEN curBuscaRelaxPostalCodeComNum;
                         FETCH curBuscaRelaxPostalCodeComNum BULK COLLECT INTO vRoadTable;
                         CLOSE curBuscaRelaxPostalCodeComNum;
                      ELSE
                         -- Todos os relaxamentos, exceto CEP e nome da rua
                         OPEN curBuscaRelaxHNComNum;
                         FETCH curBuscaRelaxHNComNum BULK COLLECT INTO vRoadTable;
                         CLOSE curBuscaRelaxHNComNum;
                      END IF;
                   END IF;
                ELSE
                   -- Somente relaxar tipo e prefixo
                   OPEN curBuscaRelaxPrefixComNum;
                   FETCH curBuscaRelaxPrefixComNum BULK COLLECT INTO vRoadTable;
                   CLOSE curBuscaRelaxPrefixComNum;
                END IF;
             ELSE
                -- Somente relaxar tipo
                OPEN curBuscaRelaxTypeComNum;
                FETCH curBuscaRelaxTypeComNum BULK COLLECT INTO vRoadTable;
                CLOSE curBuscaRelaxTypeComNum;
             END IF;
          END IF;
       END IF;
       
       --Se encontrou alguma rua, busca os segmentos
       IF vRoadTable.COUNT > 0 THEN            
           DECLARE
              vId NUMBER; 
              vAllSegments SegmentTableType;
              v_obj_road_id GC_OBJ_ROAD;
           BEGIN
              vAllSegments := SegmentTableType();
              
              v_tab_road_id := GC_TP_ROAD_ARRAY();
              
              FOR I IN vRoadTable.FIRST..vRoadTable.LAST
              LOOP
                 v_obj_road_id := GC_OBJ_ROAD();
                 v_obj_road_id.road_id          := vRoadTable(i).RoadId;
                 v_obj_road_id.hn               := vRoadTable(i).RelaxedHouseNumber;
                 v_obj_road_id.hn               := vRoadTable(i).RelaxedHouseNumber;
                 v_obj_road_id.tolerancia_usada := vRoadTable(i).ToleranciaUsada;
                 v_obj_road_id.acertou_mais     := vRoadTable(i).AcertouMais;
                 v_tab_road_id.extend(1);
                 v_tab_road_id(v_tab_road_id.count) := v_obj_road_id;
              END LOOP;
                 
              --OPEN curBuscaSegmento(vRoadTable(i).RoadId, vRoadTable(i).RelaxedHouseNumber);
              OPEN curBuscaSegmento();
              FETCH curBuscaSegmento BULK COLLECT INTO vAllSegments;
              CLOSE curBuscaSegmento;     
              
              --vAllSegments := vAllSegments MULTISET UNION ALL vSegments;
              
              
              -- A condição a seguir permite as seguintes situações:
              -- 1) Encontrou ao menos um segmento em alguma das vias (vRoadTable). 
              --    Isso faz com que o resultado sejam SOMENTE esses segmentos. Nenhum centróide é retornado;
              -- 2) Nenhuma via possui um segmento válido para a consulta. Nenhum resultado é retornado;
              -- 3) Nenhuma via possui um segmento válido, mas é permitido o RELAXAMENTO de numeração. Com isso, o resultado 
              --    são as vias com os respectivos centróides;
              IF v_mm_number = 'F' OR vAllSegments.COUNT > 0 THEN
                 vRoadTable := vAllSegments;
              END IF;
           END;
       END IF;

   --Se o número não foi informado
   ELSE
      OPEN curBuscaExactSemNum;
      FETCH curBuscaExactSemNum BULK COLLECT INTO vRoadTable;
      CLOSE curBuscaExactSemNum;
      
      IF vRoadTable.COUNT = 0 THEN 
     
         IF (v_mm_type = 'T') THEN 
            IF (v_mm_prefix = 'T') THEN
               IF (v_mm_number = 'T') THEN 
                  IF (v_mm_basename = 'T') THEN
                     IF (v_mm_zip = 'T') THEN
                        -- Todos os relaxamentos ativos => Relaxamento máximo
                        OPEN curBuscaRelaxAllSemNum;
                        FETCH curBuscaRelaxAllSemNum BULK COLLECT INTO vRoadTable;
                        CLOSE curBuscaRelaxAllSemNum;
                     ELSE
                        -- Todos os relaxamentos, exceto CEP
                        OPEN curBuscaRelaxBaseNameSemNum;
                        FETCH curBuscaRelaxBaseNameSemNum BULK COLLECT INTO vRoadTable;
                        CLOSE curBuscaRelaxBaseNameSemNum;
                     END IF;
                  ELSE
                     IF (v_mm_zip = 'T') THEN
                        -- Todos os relaxamentos, exceto nome da rua
                        OPEN curBuscaRelaxPostalCodeSemNum;
                        FETCH curBuscaRelaxPostalCodeSemNum BULK COLLECT INTO vRoadTable;
                        CLOSE curBuscaRelaxPostalCodeSemNum;
                     ELSE
                        -- Todos os relaxamentos, exceto CEP e nome da rua
                        OPEN curBuscaRelaxHNSemNum;
                        FETCH curBuscaRelaxHNSemNum BULK COLLECT INTO vRoadTable;
                        CLOSE curBuscaRelaxHNSemNum;
                     END IF;
                  END IF;
               ELSE
                  -- Somente relaxar tipo e prefixo
                  OPEN curBuscaRelaxPrefixSemNum;
                  FETCH curBuscaRelaxPrefixSemNum BULK COLLECT INTO vRoadTable;
                  CLOSE curBuscaRelaxPrefixSemNum;
               END IF;
            ELSE
               -- Somente relaxar tipo
               OPEN curBuscaRelaxTypeSemNum;
               FETCH curBuscaRelaxTypeSemNum BULK COLLECT INTO vRoadTable;
               CLOSE curBuscaRelaxTypeSemNum;
            END IF;
         END IF;      
      END IF;
   END IF;  
   
   vResultCount := 0;

                      
   IF vRoadTable.COUNT > 0 THEN 
      FOR I IN vRoadTable.FIRST..vRoadTable.LAST
      LOOP
         addr := GC_TP_GEO_ADDR();

         addr.StreetTypeId         := vRoadTable(i).StreetTypeId;
         addr.StreetType           := vRoadTable(i).StreetType;
         addr.StreetTypePhonetic   := vRoadTable(i).StreetTypePhonetic;
         addr.StreetPrefixId       := vRoadTable(i).StreetPrefixId;
         addr.StreetPrefix         := vRoadTable(i).StreetPrefix;
         addr.StreetPrefixPhonetic := vRoadTable(i).StreetPrefixPhonetic;
         addr.StreetName           := vRoadTable(i).StreetName;
         addr.StreetNamePhonetic   := vRoadTable(i).StreetNamePhonetic;
         addr.HouseNumber          := vRoadTable(i).HouseNumber;
         addr.Side                 := vRoadTable(i).Side;
         addr.Exact                := vRoadTable(i).Exact;
         addr.PostalCode4          := vRoadTable(i).PostalCode4;
         addr.PostalCode5          := vRoadTable(i).PostalCode5;
         addr.PostalCode           := vRoadTable(i).PostalCode;
         addr.NeighborhoodId       := vRoadTable(i).NeighborhoodId;
         addr.Neighborhood         := vRoadTable(i).Neighborhood;
         addr.NeighborhoodPhonetic := vRoadTable(i).NeighborhoodPhonetic;
         addr.MunicipalityId       := vRoadTable(i).MunicipalityId;
         addr.Municipality         := vRoadTable(i).Municipality;
         addr.MunicipalityPhonetic := vRoadTable(i).MunicipalityPhonetic;
         addr.StateId              := vRoadTable(i).StateId;
         addr.State                := vRoadTable(i).State;
         addr.StatePhonetic        := vRoadTable(i).StatePhonetic;
         addr.CountryId            := vRoadTable(i).CountryId;
         addr.Country              := vRoadTable(i).Country;
         addr.CountryPhonetic      := vRoadTable(i).CountryPhonetic;
         addr.RoadId               := vRoadTable(i).RoadId;
         addr.SegmentId            := vRoadTable(i).SegmentId;
         addr.StartHouseNumber     := vRoadTable(i).StartHouseNumber;
         addr.EndHouseNumber       := vRoadTable(i).EndHouseNumber;
         addr.Longitude            := vRoadTable(i).Longitude;
         addr.Latitude             := vRoadTable(i).Latitude;
         addr.Point                := vRoadTable(i).Point;
         addr.ErrorMessage         := vRoadTable(i).ErrorMessage;
         addr.MatchValue           := vRoadTable(i).MatchValue;
         addr.MatchMode            := match_mode;

         IF addr.SegmentId IS NOT NULL THEN
            addr.ErrorMessage := GetErrorMessage(addr_Formatted, addr);
            addr.MatchValue   := GetMatchValue(addr.ErrorMessage);
         END IF;

         vResultCount := vResultCount + 1;
         IF vResultCount > 2000 THEN 
            EXIT;
         END IF;
           
         vector_addr.EXTEND(1);
         vector_addr(vResultCount):= addr; 
      END LOOP;
   END IF;
   
   SELECT GC_TP_GEO_ADDR(StreetTypeId, StreetType, StreetTypePhonetic, StreetPrefixId, StreetPrefix, StreetPrefixPhonetic,
          StreetName, StreetNamePhonetic, HouseNumber, Side, Exact, PostalCode4, PostalCode5, PostalCode, NeighborhoodId,
          Neighborhood, NeighborhoodPhonetic, MunicipalityId, Municipality, MunicipalityPhonetic, StateId, State, StatePhonetic,
          CountryId, Country, CountryPhonetic, RoadId, SegmentId, StartHouseNumber, EndHouseNumber, Longitude, Latitude, Point,
          ErrorMessage, MatchValue, MatchMode)
   BULK COLLECT INTO vector_addr_out          
   FROM (
      SELECT row_number() OVER (PARTITION BY SegmentId ORDER BY SegmentId, PostalCode DESC, NeighborhoodId) AS ORDEM, A.*
      FROM TABLE(vector_addr) A
   )
   WHERE ordem = 1;
  
   RETURN vector_addr_out;
END;

/* -----------------------------------------------------------------------------
   Esta função retorna o endereço mais semelhante a um dado endereço de entrada.
   Chamada: GetBestAddr(vector_addr)
   Parametros: - vector_addr é um vetor de gc_geo_addr;
               - se podeIgual for FALSE, quando tivermos o melhor repetido, 
                 não retorna nada;
------------------------------------------------------------------------------*/
FUNCTION GetBestAddr(vector_addr IN GC_TP_GEO_ADDR_ARRAY, podeIgual BOOLEAN := TRUE) RETURN GC_TP_GEO_ADDR
AS


   points NUMBER := 0;
   points_max NUMBER := points;
   best_segment NUMBER;
   i NUMBER;
   addr GC_TP_GEO_ADDR := GC_TP_GEO_ADDR();
   vContaBest NUMBER := 0;

BEGIN
   best_segment := 1; -- best_segment_id recebe o primeiro endereço que casou com o endereço de entrada
   
   IF (vector_addr.count > 0) THEN
      FOR i IN 1..vector_addr.COUNT
      LOOP
         IF (vector_addr(i).MatchValue > points_max) THEN
            points_max := vector_addr(i).MatchValue;
            best_segment := i;
         END IF;
      END LOOP; 
   
      -- Verificar se tem alguém com o valor máximo repetido
      IF (not podeIgual) THEN
         vContaBest := 0;
         FOR i IN 1..vector_addr.COUNT
         LOOP
            IF (vector_addr(i).MatchValue = points_max) THEN
               vContaBest := vContaBest + 1;
            END IF;
         END LOOP;
      ELSE
         vContaBest := 1;
      END IF;
   
      IF vContaBest = 1 THEN
         RETURN vector_addr(best_segment); 
      ELSE
         RETURN addr;
      END IF;
   ELSE
       RETURN addr;
   END IF;
END;

  FUNCTION Geocode(addr_type IN VARCHAR2, addr_prefix IN VARCHAR2, addr_name IN VARCHAR2, addr_number IN NUMBER, addr_postal_code IN VARCHAR2, addr_neighborhood IN VARCHAR2, addr_municipality IN VARCHAR2, addr_state IN VARCHAR2, addr_country IN VARCHAR2,  match_mode IN VARCHAR2, type_return IN VARCHAR2, usar_offset BOOLEAN := FALSE, numero_obrigatorio BOOLEAN := FALSE) RETURN GC_TP_GEO_ADDR_ARRAY
  AS
     -- Chamada: SELECT geocoder.GEOCODE('esquema','fonte_da_fonetica','tipo', 'prefixo', 'nome', 'numero', 'cep', 'bairro', 'municipio', 'estado', 'pais', 'match_mode', 'tipo_retorno') FROM DUAL;
     -- tipo_retorno deve ser 'ALL' ou 'BEST'
     -- vetor onde cada posição é um atributo do endereço não formatado 1-Tipo, 2-Prefixo, 3-Nome, 4-Numero, 5-CEP, 6-Bairro, 7-Cidade, 8-Estado, 9-Pais
     vector_addr GC_TP_GEO_ADDR_ARRAY; -- vetor de GC_TP_GEO_ADDR
     addr        GC_TP_GEO_ADDR_ARRAY; -- esta variável foi criada somente para o retorno, quando vector_addr é nulo
  BEGIN                                  
  
     vUsarOffset := usar_offset;
     

     IF ((UPPER(type_return) <> 'ALL') AND (UPPER(type_return) <> 'BEST') AND (UPPER(type_return) <> 'SINGLE') ) THEN
        DBMS_OUTPUT.PUT_LINE('INVALID TYPE OF RETURN!');
        DBMS_OUTPUT.PUT_LINE('VALID TYPES: ALL, BEST or SINGLE');
        RETURN addr;
     END IF;
     
     vTypeReturn := type_return;

     vector_addr := NULL;
     IF NOT numero_obrigatorio OR addr_number IS NOT NULL THEN 
         SELECT GEOCODEALL(cScheme,                       
                           addr_type,
                           addr_prefix,
                           addr_name,                       
                           addr_number,
                           addr_postal_code,
                           addr_neighborhood,
                           addr_municipality,
                           addr_state,
                           addr_country,
                           match_mode) INTO vector_addr FROM DUAL;
     END IF;

     IF vector_addr IS NULL THEN
        vector_addr := GC_TP_GEO_ADDR_ARRAY();
     END IF;
     
    -- vector_addr := GetBestSegments(vector_addr);

     IF (UPPER(type_return) = 'BEST') THEN
        addr := GC_TP_GEO_ADDR_ARRAY();
        addr.EXTEND(1);
        addr(1):= GetBestAddr(vector_addr);

        RETURN addr;
     ELSIF (UPPER(type_return) = 'SINGLE') THEN
        addr := GC_TP_GEO_ADDR_ARRAY();
        addr.EXTEND(1);
        addr(1):= GetBestAddr(vector_addr, false);

        RETURN addr;
     ELSE
        RETURN vector_addr;
     END IF;
  END;

  FUNCTION GeocodeAll(scheme IN VARCHAR2, addr_type IN VARCHAR2, addr_prefix IN VARCHAR2, addr_name IN VARCHAR2, addr_number IN NUMBER, addr_postal_code IN VARCHAR2, addr_neighborhood IN VARCHAR2, addr_municipality IN VARCHAR2, addr_state IN VARCHAR2, addr_country IN VARCHAR2, match_mode IN VARCHAR2) RETURN GC_TP_GEO_ADDR_ARRAY
  AS
    -- Esta função retorna todos os endereços encontrados dado um endereço de entrada.
    -- SELECT geocoder.GeocodeAll('esquema','tipo', 'prefixo', 'nome', 'numero', 'cep', 'bairro', 'municipio', 'estado', 'pais', 'match_mode') FROM DUAL;

    -- match_mode é uma é uma variável que especifica quão próximo do endereço de entrada deve ser do endereço retornado.
    -- match_mode pode receber os seguinte valores: 
    -- 'EXACT': todos os atributos do endereço de entrada devem casar com o endereço da base.
    -- 'RELAX_STREET_TYPE': O prefixo da rua pode ser diferente do prefixo do endereço entrada.
    -- 'RELAX_STREET_PREFIX': O prefixo (Dr, General, etc) e o tipo podem ser diferentes do prefixo e do tipo do endereço de entrada.
    -- 'RELAX_HOUSE_NUMBER': O prefixo, o tipo e o número podem ser diferentes daqueles especificados no endereço de entrada.

    addr_Unformatted     GC_TP_GEO_ADDR; -- tipo endereço
    addr_Formatted       GC_TP_GEO_ADDR; -- tipo endereço
    vector_addr          GC_TP_GEO_ADDR_ARRAY; -- vetor de GC_GEO_ADDR

    -- variáveis de match mode
    v_mm_exact      CHAR(1);
    v_mm_type       CHAR(1);
    v_mm_prefix     CHAR(1);
    v_mm_number     CHAR(1);
    v_mm_basename   CHAR(1);    
    v_mm_zip        CHAR(1);
    v_mm_unique_zip CHAR(1);
    
   -- vetor onde cada posição é um atributo do endereço não formatado 1-Tipo, 2-Prefixo, 3-Nome, 4-Numero, 5-CEP, 6-Bairro, 7-Cidade, 8-Estado, 9-Pais
    --v_addr_unformatted  V_STRING_ARRAY;
    -- vetor onde cada posição é um atributo do endereço formatado 1-Tipo, 2-Prefixo, 3-Nome, 4-Numero, 5-CEP, 6-Bairro, 7-Cidade, 8-Estado, 9-Pais
    v_addr_formatted    V_STRING_ARRAY;
    
    vPracaTypeId NUMBER;
    vRelaxLevel NUMBER;

    -- v_segments é um vetor de segment_id:
    v_match_mode         VARCHAR2(30) := UPPER(match_mode);    
    v_geocode_method     VARCHAR2(20);
    
    cNivelZero CONSTANT NUMBER   := 0;
    cNivelUm   CONSTANT NUMBER   := 1;
    cNivelDois CONSTANT NUMBER   := 2;
    cNivelTres CONSTANT NUMBER   := 3;
    cNivelQuatro CONSTANT NUMBER := 4;
    cNivelCinco CONSTANT NUMBER  := 5;
    
    CURSOR curPracaTypeId IS
      SELECT DISTINCT type_id
      FROM MAPA_URBANO.vw_gc_type
      WHERE Type = 'Pça.';

    BEGIN

         vector_addr := GC_TP_GEO_ADDR_ARRAY();

         -- Valida os valores do match_mode e seta as variáveis
         v_mm_exact      := 'F'; -- Exato - Todos os campos devem 'bater'
         v_mm_type       := 'F'; -- Pode efetuar relaxamento de tipo
         v_mm_prefix     := 'F'; -- Pode efetuar relaxamento de prefixo
         v_mm_number     := 'F'; -- Pode efetuar relaxamento de número
         v_mm_zip        := 'F'; -- Relaxamento de zip: Busca por nome da rua ou CEP
         v_mm_unique_zip := 'F'; -- Mapeia no centróide do municipio quando o CEP é único
         vRelaxLevel  := 0;   -- Nível de relaxamento. Quanto maior, mais relaxamentos aceita

         IF (
            v_match_mode NOT IN (
               'EXACT', 
               'RELAX_STREET_TYPE',
               'RELAX_STREET_PREFIX',
               'RELAX_HOUSE_NUMBER',
               'RELAX_BASE_NAME',
               'RELAX_POSTAL_CODE',
               'RELAX_ALL',
               'RELAX_UNIQUE_POSTAL_CODE'
            )
         ) THEN 
            DBMS_OUTPUT.PUT_LINE('match_mode inválido: ' || v_match_mode);
            RETURN vector_addr;
         ELSE
           CASE v_match_mode 
              WHEN 'EXACT' THEN 
                  v_mm_exact := 'T';
              WHEN 'RELAX_STREET_TYPE' THEN 
                  v_mm_type := 'T';
              WHEN 'RELAX_STREET_PREFIX' THEN 
                  v_mm_type := 'T';
                  v_mm_prefix := 'T';
              WHEN 'RELAX_HOUSE_NUMBER' THEN
                  v_mm_type := 'T';
                  v_mm_prefix := 'T';
                  v_mm_number := 'T';
              WHEN 'RELAX_BASE_NAME' THEN 
                  v_mm_type := 'T';
                  v_mm_prefix := 'T';
                  v_mm_number := 'T';
                  v_mm_basename := 'T';              
              WHEN 'RELAX_POSTAL_CODE' THEN
                  v_mm_type := 'T';
                  v_mm_prefix := 'T';
                  v_mm_number := 'T';
                  v_mm_zip := 'T';
              WHEN 'RELAX_ALL' THEN
                  v_mm_type := 'T';
                  v_mm_prefix := 'T';
                  v_mm_number := 'T';
                  v_mm_zip := 'T';
                  v_mm_basename := 'T'; 
               WHEN 'RELAX_UNIQUE_POSTAL_CODE' THEN
                  v_mm_type := 'T';
                  v_mm_prefix := 'T';
                  v_mm_number := 'T';
                  v_mm_zip := 'T';
                  v_mm_basename := 'T'; 
                  v_mm_unique_zip := 'T';  
           END CASE;
        END IF;
        --------------------------------------
        addr_Unformatted := GC_TP_GEO_ADDR();

        addr_Unformatted.StreetType    := addr_type;
        addr_Unformatted.StreetPrefix  := addr_prefix;
        addr_Unformatted.StreetName    := addr_name;
        addr_Unformatted.HouseNumber   := addr_number;
        addr_Unformatted.PostalCode    := addr_postal_code;        
        addr_Unformatted.Neighborhood  := addr_neighborhood;
        addr_Unformatted.Municipality  := addr_municipality;
        addr_Unformatted.State         := addr_state;
        addr_Unformatted.Country       := addr_country;
        
        addr_formatted := GC_TP_GEO_ADDR();

        GC_MANAGE_ADDR.FORMAT_ADDR(scheme, addr_Unformatted, addr_Formatted);

        -- Validação dos atributos de entrada e chamada aos métodos de geocodificação
        DECLARE 
           vContinue BOOLEAN := TRUE;
        BEGIN
           -- Tipo só pode ser válido, caso contrário deve sofrer relaxamento
           vContinue := (addr_formatted.StreetTypeId > 0) OR (v_mm_type = 'T');                                           

           -- Prefixo só pode ser válido ou nulo, caso contrário deve sofrer relaxamento
           vContinue := vContinue AND ((addr_formatted.StreetPrefixId <> -1) OR (v_mm_prefix = 'T'));                     

           -- Nome da rua e o cep não podem ser nulos ao mesmo tempo, com exceção de mapeamento em mun. de CEP único
           vContinue := vContinue AND ((addr_formatted.StreetName IS NOT NULL OR addr_formatted.PostalCode IS NOT NULL OR v_mm_unique_zip = 'T')); 

           -- Município sempre deve ser válido e não nulo, com exceção de mapeamento em mun. de CEP único
           -- Caso seja válido, o estado e o país também são.
           vContinue := vContinue AND (addr_formatted.MunicipalityId > 0 or v_mm_unique_zip = 'T');

           -- CEP e municípios não podem ficar ambos nulos
           vContinue := vContinue AND ((addr_formatted.MunicipalityId > 0 OR addr_formatted.PostalCode IS NOT NULL)); 
           
           
           IF vContinue THEN   
              vector_addr := GeocodeFullAddress(scheme, addr_Unformatted, addr_Formatted, v_mm_type, v_mm_prefix, v_mm_number, v_mm_basename, v_mm_zip, v_mm_unique_zip, match_mode);
               
              IF vector_addr.COUNT = 0 THEN
                 --Verifica se deve buscar na base de praças---------------------------------
                 OPEN curPracaTypeId;
                 FETCH curPracaTypeId INTO vPracaTypeId;
                 IF curPracaTypeId%NOTFOUND THEN
                    RAISE_APPLICATION_ERROR(-20000, 'Tipo "Praça" não encontrado');
                 END IF;
                 CLOSE curPracaTypeId;
                 
                 IF (addr_Formatted.StreetTypeId IS NOT NULL) AND (addr_Formatted.StreetTypeId = vPracaTypeId) THEN
                    vector_addr := GeocodeBySquare(addr_Formatted);
                 END IF;
                 ----------------------------------------------------------------------------
              END IF;
   
           END IF;
        END;

        RETURN vector_addr;

    END;

/**
 *  Define qual é o método de geocodificação a ser usado
 *  - GeocodeBySquare = Geocodificar usando a tabela de praças quando o type do endereço for igual a 'Pça.';
 *  - GeocodeMunicipality = Geocodificar por município quando não há logradouro, número e CEP, mas há município preenchido;
 *  - GeocodeFullAddress = Geocodificar pelo endereço completo considerando possíveis relaxamentos;
 *  @param addr_Formatted Endereço formatado
 *  @return Nome do método que deve ser chamado
 */
 
 /*
FUNCTION GetGeocodeMethod(addr_Formatted IN OUT GC_TP_GEO_ADDR) RETURN VARCHAR2 AS
   CURSOR curPracaTypeId IS
      SELECT DISTINCT type_id
      FROM MAPA_URBANO.vw_gc_type
      WHERE Type = 'Pça.';
      
   CURSOR curBuscaMun(pcUfMunCod NUMBER, pcCEP NUMBER) IS
      SELECT a.ufmun_cod, 
             (SELECT municipality FROM mapa_urbano.gc_municipality WHERE ufmun_cod = a.ufmun_cod) as municipality,
             CASE WHEN first_postal_code = last_postal_code THEN 'S' ELSE 'N' END AS CEP_UNICO
      FROM mapa_urbano.gc_municipality_zip_range a
      WHERE (NVL(pcUfMunCod,0) = 0 or ufmun_cod = pcUfMunCod)
        AND NVL(pcCEP,0) between a.first_postal_code and a.last_postal_code
      
      UNION
      
      SELECT a.ufmun_cod, 
             (SELECT municipality FROM mapa_urbano.gc_municipality WHERE ufmun_cod = a.ufmun_cod) as municipality,
             'S' AS CEP_UNICO
      FROM mapa_urbano.gc_municipality_zip_range a
      WHERE (NVL(pcUfMunCod,0) = 0 or ufmun_cod = pcUfMunCod)
        AND (a.first_postal_code = NVL(pcCEP,0) or a.last_postal_code = NVL(pcCEP,0))
        
      UNION
      
      SELECT x.ufmun_cod, 
             (SELECT municipality FROM mapa_urbano.gc_municipality WHERE ufmun_cod = x.ufmun_cod) as municipality,
             CASE WHEN x.num_ruas > 1 THEN 'S' ELSE 'N' END AS CEP_UNICO
      FROM (SELECT b.ufmun_cod, b.num_ruas
            FROM MAPA_URBANO.VW_GC_POSTAL_CODE b
            WHERE b.postal_code = NVL(TO_CHAR(pcCEP),'0')) x
      WHERE (NVL(pcUfMunCod,0) = 0 or x.ufmun_cod = pcUfMunCod)
      
      ORDER BY cep_unico DESC
      ;
   
   vBuscaMun curBuscaMun%ROWTYPE;        
   vPracaTypeId NUMBER;
   vPostalCode NUMBER;
   vMethod VARCHAR2(200);
BEGIN

    --Verifica se deve buscar na base de praças---------------------------------
    OPEN curPracaTypeId;
    FETCH curPracaTypeId INTO vPracaTypeId;
    IF curPracaTypeId%NOTFOUND THEN
       RAISE_APPLICATION_ERROR(-20000, 'Tipo "Praça" não encontrado');
    END IF;
    CLOSE curPracaTypeId;
    ----------------------------------------------------------------------------
    
    
    -- Verificar se deve buscar por CEP ----------------------------------------
    -- Deve-se limpar o CEP nesse caso
    vBuscaMun.cep_unico := 'N'; 
    
    IF TOOLS.FN_E_NUMERO(NVL(TRIM(addr_Formatted.PostalCode),0)) = 'S' THEN
       vPostalCode := TO_NUMBER(NVL(TRIM(addr_Formatted.PostalCode),0));
    ELSE 
       vPostalCode := 0;
    END IF;
    
    IF addr_Formatted.PostalCode IS NOT NULL THEN
       OPEN curBuscaMun(addr_Formatted.MunicipalityId, vPostalCode);
       FETCH curBuscaMun INTO vBuscaMun;
       IF (curBuscaMun%FOUND) AND (vBuscaMun.cep_unico = 'S') THEN 
          addr_Formatted.PostalCode := NULL;
          addr_Formatted.MunicipalityId := vBuscaMun.ufmun_cod;
          addr_Formatted.Municipality := vBuscaMun.municipality;
       END IF;
       CLOSE curBuscaMun;
    END IF;  
    ----------------------------------------------------------------------------
    vMethod := NULL; 
    
    IF (addr_Formatted.StreetTypeId IS NOT NULL) AND (addr_Formatted.StreetTypeId = vPracaTypeId) THEN
       vMethod := 'GeocodeBySquare';
    ELSE
       vMethod := 'GeocodeFullAddress';
    END IF;

    RETURN vMethod;

END; 

*/

   FUNCTION GetSegmentTablePipelined RETURN SegmentTableType PIPELINED
   AS
      vRoad SegmentType;
      i NUMBER;
   BEGIN
      i:=vRoadTable.FIRST; 
      WHILE (i IS NOT NULL)
      LOOP
         PIPE ROW (vRoadTable(i));
         i := vRoadTable.NEXT (i);
      END LOOP;   
   END;
 
   
    /*----------------------------------------------------------------------------
      Descrição:
         Dado um vetor de road_id, um número e o relaxamento, esta função encontra os
         segmentos da via onde esta o número.
      Parametros:
         -> scheme : Esquema que contém a tabela de ruas;
         -> v_roads  : Lista de ids das ruas.
         -> v_number : Número da rua;
         -> v_mm_number IN CHAR : ('T'/'F') - Identifica se o parâmetro NÚMERO
            pode ser 'relaxado';
      Exemplo:
         FindHouseNumber(scheme, v_roads, v_number, v_mm_number)
      Histórico:
         05/05/2011 - Rodrigo L. Gil
         -- Criação da função nova
   ---------------------------------------------------------------------------*/
    FUNCTION FindHouseNumber(scheme IN VARCHAR2, v_roads IN V_NUMBER_ARRAY, v_number IN NUMBER, v_mm_number IN CHAR, v_postalcode VARCHAR2) RETURN SegmentTableType
    AS
       vSegmentKey VARCHAR2(200);
       vRoadSegmentId  NUMBER;
       number_ok      CHAR(1);
       --v_segments     v_number_array2;
       vSegments SegmentTableType;
       i              NUMBER;
     --  j              NUMBER := 1;
       v_start_hn     NUMBER;
       v_end_hn       NUMBER;
       v_l_start_hn   NUMBER;
       v_l_end_hn     NUMBER;
       v_r_start_hn   NUMBER;
       v_r_end_hn     NUMBER;
       v_left_scheme  NUMBER;
       v_right_scheme NUMBER;
       vNumberType NUMBER;
       --vFirstSegment V_NUMBER_ARRAY;
       --vMinStart V_NUMBER_ARRAY;
       --vLastSegment V_NUMBER_ARRAY;
       --vLastEnd V_NUMBER_ARRAY;
       v_side_number_aux CHAR;
       min_number NUMBER;
       max_number NUMBER;
       --vRightPostalCode VARCHAR2(50);
       --vLeftPostalCode VARCHAR2(50);
       vPostalCode VARCHAR2(50);
       vAchouAlgumExato BOOLEAN;


       TYPE EmpCurTyp IS REF CURSOR;
       segmentCursor EmpCurTyp;



       FUNCTION getNumberSide(pfNumberType NUMBER) RETURN CHAR IS
       BEGIN
          IF v_left_scheme = pfNumberType THEN
             return 'L';
          ELSIF v_right_scheme = pfNumberType THEN
             return 'R';
          ELSE
             return NULL;
          END IF;
       END;

       FUNCTION buscaSegmento(pTipoBusca NUMBER, pRoadId NUMBER) RETURN BOOLEAN AS
          CURSOR segmentCursor(pCurRoadId NUMBER, pCurSegmentNumber NUMBER) IS
             SELECT *
             FROM (
             SELECT  road_segment_id,
                     CASE WHEN l_start_hn IS NULL AND l_end_hn IS NULL AND r_start_hn IS NULL AND r_end_hn IS NULL THEN NULL
                          ELSE LEAST(NVL(l_start_hn, 9999999),NVL(l_end_hn, 9999999),NVL(r_start_hn, 9999999),NVL(r_end_hn, 9999999)) 
                     END AS start_hn,
                     CASE WHEN l_start_hn IS NULL AND l_end_hn IS NULL AND r_start_hn IS NULL AND r_end_hn IS NULL THEN NULL
                          ELSE GREATEST(NVL(l_start_hn, 0),NVL(l_end_hn, 0),NVL(r_start_hn, 0),NVL(r_end_hn, 0))
                     END AS end_hn,
                     left_scheme, 
                     right_scheme,
                     l_start_hn,
                     l_end_hn,
                     r_start_hn,
                     r_end_hn
             FROM mapa_urbano.vw_gc_road_segment
             WHERE road_id = pCurRoadId)
             WHERE pCurSegmentNumber BETWEEN start_hn AND end_hn ;
       BEGIN

          OPEN segmentCursor(pRoadId, v_number);
          LOOP
              FETCH segmentCursor INTO vRoadSegmentId, 
                                       v_start_hn,
                                       v_end_hn,
                                       v_left_scheme,
                                       v_right_scheme,
                                       v_l_start_hn, 
                                       v_l_end_hn,
                                       v_r_start_hn,
                                       v_r_end_hn;
              EXIT WHEN segmentCursor%NOTFOUND;
              
              vSegmentKey := pRoadId || '-' || vRoadSegmentId;

              -- Verifica se o segmento é valido e adiciona na lista
              vNumberType := NULL;
              v_side_number_aux := NULL;
 
--              v_side_number_aux := FindNumberSide(v_number, v_left_scheme, v_right_scheme, v_l_start_hn, v_l_end_hn, v_r_start_hn, v_r_end_hn, vNumberType, pTipoBusca);

              -- Caso ainda não esteja definido o lado da rua, isto é,
              -- o número não foi encontrado nesse segmento,
              -- ele é deixado de fora
              IF v_side_number_aux IS NOT NULL THEN
                 /*v_segments(j) := v_segment_id;
                 v_new_number(j) := v_number;
                 v_side_number(j) := v_side_number_aux;
                 j := j + 1;*/
                 /*vSegments(vSegmentKey).road_id :=pRoadId;
                 vSegments(vSegmentKey).road_segment_id    := vRoadSegmentId;
                 vSegments(vSegmentKey).new_number  := v_number;
                 vSegments(vSegmentKey).side_number := v_side_number_aux;
                 number_ok := 'T';
                 vSegments(vSegmentKey).number_exact := number_ok;*/
                 vAchouAlgumExato := TRUE; 
              END IF;
          END LOOP;
          CLOSE segmentCursor;

          RETURN number_ok = 'T';
       END;
    BEGIN
       vAchouAlgumExato := FALSE;

       -- Varrer lista de ruas
       FOR i IN 1..v_roads.COUNT
       LOOP
          number_ok := 'F';

          -- Zera variáveis para uso posterior
          vRoadSegmentId     := NULL;
          vSegmentKey        := NULL;
          --vLeftPostalCode  := NULL;
          --vRightPostalCode := NULL;
          vPostalCode := NULL;

          --se o número do endereço de entrada é nulo
          IF (v_number IS NULL) THEN
             
             DECLARE 
                vRoadGeometry SDO_GEOMETRY;
                vPoint SDO_GEOMETRY;
             BEGIN
                 --Busca geometria da rua
                 /*SELECT SDO_AGGR_CONCAT_LINES(T1.GEOM) INTO vRoadGeometry
                 FROM geocoder.vw_GC_ROAD_SEGMENT t1 
                 WHERE t1.ROAD_ID = v_roads(i);*/
                 
                 SELECT SDO_AGGR_CONCAT_LINES(T1.GEOM) INTO vRoadGeometry
                 FROM mapa_urbano.vw_GC_ROAD_SEGMENT t1  
                 WHERE t1.ROAD_ID = v_roads(i);
                                     
                 -- Busca centróide
                 vPoint := SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(vRoadGeometry), SDO_GEOM.SDO_LENGTH(vRoadGeometry, 0.05) / 2));
                 
                 -- Busca segmento do centróide
                 SELECT road_segment_id, /*r_postal_code_correios, l_postal_code_correios*/ postal_code INTO /*v_segments(j)*/vRoadSegmentId, /*vRightPostalCode, vLeftPostalCode*/ vPostalCode
                 FROM (
                       SELECT road_segment_id, /*r_postal_code_correios, l_postal_code_correios*/null as postal_code
                       FROM MAPA_URBANO.vw_GC_ROAD_SEGMENT t1
                       WHERE sdo_relate(t1.geom, vPoint, 'mask=ANYINTERACT') = 'TRUE' 
                       ORDER BY LENGTH(POSTAL_CODE) DESC
                 ) 
                 WHERE ROWNUM < 2;
                 
                 vSegmentKey := v_roads(i) || '-' || vRoadSegmentId;
                 /*
                 vSegments(vSegmentKey).road_id := v_roads(i);
                 vSegments(vSegmentKey).road_segment_id := vRoadSegmentId;*/
    
                 -- Se o centróide  não tiver o mesmo CEP que foi usado na busca, buscar segmento correto
                 IF (vPostalCode IS NOT NULL AND v_postalcode IS NOT NULL AND vPostalCode <> v_postalcode)  THEN
                    OPEN segmentCursor FOR 'SELECT road_segment_id, /*r_postal_code_correios, l_postal_code_correios*/ postal_code
                                             FROM '||scheme||'.vw_gc_road_segment
                                             WHERE road_id = :road_id
                                             ' USING v_roads(i);
                    LOOP
                       FETCH segmentCursor INTO vRoadSegmentId, /*vRightPostalCode, vLeftPostalCode*/ vPostalCode;
                       EXIT WHEN segmentCursor%NOTFOUND;
                       
                          vSegmentKey := v_roads(i) || '-' || vRoadSegmentId;
       
                          --IF ((vRightPostalCode = v_postalcode) OR (vLeftPostalCode = v_postalcode)) THEN
                          IF (vPostalCode = v_postalcode) THEN
                            /* vSegments(vSegmentKey).road_id := v_roads(i);
                             vSegments(vSegmentKey).road_segment_id := vRoadSegmentId;*/
                             EXIT; -- Sai qdo encontra CEP igual
                          END IF;
                    END LOOP;
                    
                    CLOSE segmentCursor;
                 END IF;
                 
                 

                /* vSegments(vSegmentKey).new_number   := NULL;
                 vSegments(vSegmentKey).side_number  := '';
                 number_ok                            := 'T';
                 vSegments(vSegmentKey).number_exact := number_ok;*/
                 vAchouAlgumExato := TRUE; 
             END;
          ELSE
             -- Busca o segmento de acordo com o número
             DECLARE
                vAchou BOOLEAN;
             BEGIN
                vAchou := buscaSegmento(cSearchType_ParImpar, v_roads(i));
                IF NOT vAchou THEN
                   vAchou := buscaSegmento(cSearchType_Indef, v_roads(i));
                   IF NOT vAchou THEN
                      vAchou := buscaSegmento(cSearchType_Todos, v_roads(i));
                   END IF;
                END IF;
             END;

          END IF;

          ---- Se o campo numero do endereço de entrada formatado não casou com o campo numero de nenhum segmento da base de dados,
          ---- nenhum road_segment_id foi retornado, então os relaxamentos são utilizados.
          ---- O relaxamento de número consistem em verificar qual segmento possui numero (inicio ou fim) mais próximo do numero do
          ---- endereço de entrada
          DECLARE
              vLeftScheme NUMBER;
              vRightScheme NUMBER;
              vLeftStartHN NUMBER;
              vLeftEndHN NUMBER;
              vRightStartHN NUMBER;
              vRightEndHN NUMBER;
              vNumberType NUMBER;
              vRoadGeometry SDO_GEOMETRY;
              vPoint SDO_GEOMETRY;
          BEGIN
          IF (number_ok = 'F') AND (v_mm_number = 'T') /*AND (vFirstSegment.COUNT > 0)*/ THEN -- o numero pode ser relaxado
             OPEN segmentCursor FOR 'SELECT min(CASE WHEN l_start_hn IS NULL AND l_end_hn IS NULL AND r_start_hn IS NULL AND r_end_hn IS NULL THEN NULL
                                                     ELSE LEAST(NVL(l_start_hn, 9999999),NVL(l_end_hn, 9999999),NVL(r_start_hn, 9999999),NVL(r_end_hn, 9999999)) 
                                                END) AS min_number,                                            
                                            max(CASE WHEN l_start_hn IS NULL AND l_end_hn IS NULL AND r_start_hn IS NULL AND r_end_hn IS NULL THEN NULL
                                                 ELSE GREATEST(NVL(l_start_hn, 0),NVL(l_end_hn, 0),NVL(r_start_hn, 0),NVL(r_end_hn, 0))
                                            END) AS max_number
                                     FROM '||scheme||'.vw_gc_road_segment
                                     WHERE road_id = '||v_roads(i);
             LOOP
                 FETCH segmentCursor INTO min_number, max_number;
                 EXIT WHEN segmentCursor%NOTFOUND;

                 IF (v_number < min_number AND min_number - v_number <= cToleranciaRelaxNumero) THEN
                     EXECUTE IMMEDIATE 'SELECT road_segment_id, left_scheme, right_scheme,l_start_hn, l_end_hn, r_start_hn, r_end_hn
                                    FROM '||scheme||'.vw_gc_road_segment WHERE road_id = ' || v_roads(i) || ' AND LEAST(NVL(l_start_hn, 9999999),NVL(l_end_hn, 9999999),NVL(r_start_hn, 9999999),NVL(r_end_hn, 9999999)) = ' || min_number || ' AND ROWNUM < 2'
                     INTO vRoadSegmentId, vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN;
                     
                     /*vSegmentKey := v_roads(i) || '-' || vRoadSegmentId;
                     vSegments(vSegmentKey).road_id := v_roads(i);
                     vSegments(vSegmentKey).road_segment_id := vRoadSegmentId;
                     vSegments(vSegmentKey).new_number := min_number;
                     vSegments(vSegmentKey).side_number := FindNumberSide(min_number,vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN, vNumberType, cSearchType_ParImpar);
                     IF vSegments(vSegmentKey).side_number IS NULL THEN
                        vSegments(vSegmentKey).side_number := FindNumberSide(min_number,vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN, vNumberType, cSearchType_Indef);
                        IF vSegments(vSegmentKey).side_number IS NULL THEN
                           vSegments(vSegmentKey).side_number := FindNumberSide(min_number,vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN, vNumberType, cSearchType_Todos);
                        END IF;
                     END IF;*/
                     --j := j + 1;
                 ELSE
                     IF (v_number > max_number AND v_number - max_number <= cToleranciaRelaxNumero) THEN
                         EXECUTE IMMEDIATE 'SELECT road_segment_id, left_scheme, right_scheme,l_start_hn, l_end_hn, r_start_hn, r_end_hn
                                        FROM '||scheme||'.vw_gc_road_segment WHERE road_id = ' || v_roads(i) || ' AND GREATEST(NVL(l_start_hn, 0),NVL(l_end_hn, 0),NVL(r_start_hn, 0),NVL(r_end_hn, 0)) = ' || max_number || ' AND ROWNUM < 2'
                         INTO vRoadSegmentId, vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN;
                         
                         /*vSegmentKey := v_roads(i) || '-' || vRoadSegmentId;
                         vSegments(vSegmentKey).road_id := v_roads(i);
                         vSegments(vSegmentKey).road_segment_id := vRoadSegmentId;
                         vSegments(vSegmentKey).new_number := max_number;
                         vSegments(vSegmentKey).side_number := FindNumberSide(max_number,vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN, vNumberType, cSearchType_ParImpar);
                         IF vSegments(vSegmentKey).side_number IS NULL THEN
                            vSegments(vSegmentKey).side_number := FindNumberSide(max_number,vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN, vNumberType, cSearchType_Indef);
                            IF vSegments(vSegmentKey).side_number IS NULL THEN
                               vSegments(vSegmentKey).side_number := FindNumberSide(max_number,vLeftScheme, vRightScheme, vLeftStartHN, vLeftEndHN, vRightStartHN, vRightEndHN, vNumberType, cSearchType_Todos);
                            END IF;
                         END IF;*/
                         --j := j + 1;
                     ELSE
                        IF (min_number IS NULL AND max_number IS NULL)  OR ((v_number > min_number) AND (v_number < max_number)) THEN
                             SELECT SDO_AGGR_CONCAT_LINES(T1.GEOM) INTO vRoadGeometry
                             FROM mapa_urbano.vw_GC_ROAD_SEGMENT t1 
                             WHERE t1.ROAD_ID = v_roads(i);
                                 
                             vPoint := SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(vRoadGeometry), SDO_GEOM.SDO_LENGTH(vRoadGeometry, 0.05) / 2));
                                 
                             SELECT road_segment_id INTO vRoadSegmentId
                             FROM mapa_urbano.vw_GC_ROAD_SEGMENT t1
                             WHERE sdo_relate(t1.geom, vPoint, 'mask=ANYINTERACT') = 'TRUE' AND ROWNUM < 2;

                             /*vSegmentKey := v_roads(i) || '-' || vRoadSegmentId;
                             vSegments(vSegmentKey).road_id := v_roads(i);
                             vSegments(vSegmentKey).road_segment_id := vRoadSegmentId;
                             vSegments(vSegmentKey).new_number := '';
                             vSegments(vSegmentKey).side_number := '';*/
                             --j := j + 1;
                        END IF;
                     END IF;
                  END IF;
                  
                  /*IF vSegmentKey IS NOT NULL THEN
                     vSegments(vSegmentKey).number_exact := number_ok;
                  END IF;*/
             END LOOP;
             CLOSE segmentCursor;
          END IF;
          END;
          
          
          
       END LOOP;

       IF vAchouAlgumExato THEN -- Remove resultados não exatos
          DECLARE
             vSegmentKey VARCHAR2(200);
          BEGIN   
             /*vSegmentKey := vSegments.FIRST;
             WHILE (vSegmentKey IS NOT NULL) LOOP
                IF vSegments(vSegmentKey).number_exact = 'F' THEN
                   vSegments.DELETE(vSegmentKey);
                END IF;
                vSegmentKey := vSegments.NEXT(vSegmentKey);
             END LOOP;    */         
             null;
          END;
       END IF;
       
       RETURN vSegments;

    END;
    
    FUNCTION CheckErrorMessageAttribute(pErrorMessage VARCHAR2, pNomeAtributo VARCHAR2) RETURN CHAR AS
       vResult CHAR(1) := NULL;
    BEGIN
       IF pNomeAtributo = 'TIPO' THEN
          vResult := CASE WHEN substr(pErrorMessage, 1, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'PREFIXO' THEN
          vResult := CASE WHEN substr(pErrorMessage, 2, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'NOME_LOGRADOURO' THEN
          vResult := CASE WHEN substr(pErrorMessage, 3, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'NUMERO' THEN
          vResult := CASE WHEN substr(pErrorMessage, 4, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'CEP' THEN
          vResult := CASE WHEN substr(pErrorMessage, 7, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'BAIRRO' THEN
          vResult := CASE WHEN substr(pErrorMessage, 8, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'MUNICIPIO' THEN
          vResult := CASE WHEN substr(pErrorMessage, 9, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'ESTADO' THEN
          vResult := CASE WHEN substr(pErrorMessage, 10, 1) = 1 THEN 'S' ELSE 'N' END;
       ELSIF pNomeAtributo = 'PAIS' THEN
          vResult := CASE WHEN substr(pErrorMessage, 11, 1) = 1 THEN 'S' ELSE 'N' END;
       END IF;
       
       RETURN vResult;
    END;
    

  FUNCTION GetErrorMessage(addr_Formatted IN GC_TP_GEO_ADDR, addr IN GC_TP_GEO_ADDR) RETURN VARCHAR2
  AS
    /*--
    Chamada:
    GetErrorMessage(addr_Formatted, addr)
    Parametros:
    - addr_Formatted é o endereço formatado
    - addr é o endereço de saida
    --*/

    ErrorMessage VARCHAR2(11);  -- 0 indica que o atributo não casou e 1 indica que o atributo casou
                               -- posicao 1: tipo,
                               -- posicao 2: prefixo,
                               -- posicao 3: nome,
                               -- posicao 4: numero,
                               -- posicao 5: cep4,
                               -- posicao 6: cep5,
                               -- posicao 7: cep,
                               -- posicao 8: bairro,
                               -- posicao 9: municipio,
                               -- posicao 10: estado,
                               -- posicao 11: pais
    BEGIN
        IF (COALESCE(addr_Formatted.StreetType,'0') = COALESCE(addr.StreetType,'0')) THEN
            ErrorMessage := '1';
        ELSE
            ErrorMessage := '0';
        END IF;

        IF (COALESCE(addr_Formatted.StreetPrefix,'0') = COALESCE(addr.StreetPrefix,'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        IF (COALESCE(addr_Formatted.StreetNamePhonetic,'0') = COALESCE(TOOLS.FN_PHONETIC(addr.StreetName),'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        IF (COALESCE(addr_Formatted.HouseNumber,-1) = addr.HouseNumber) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        --IF (COALESCE(SUBSTR(addr_Formatted.PostalCode, 1, 4),'0') = COALESCE(addr.PostalCode4,'0')) THEN
        --    ErrorMessage := ErrorMessage || '1';
        --ELSE
            ErrorMessage := ErrorMessage || '0';
        --END IF;

        --IF (COALESCE(SUBSTR(addr_Formatted.PostalCode, 1, 5),'0') = COALESCE(addr.PostalCode5,'0')) THEN
        IF (COALESCE(SUBSTR(addr_Formatted.PostalCode, 1, 5),'0') = COALESCE(REPLACE(addr.PostalCode, '-', NULL),'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        IF (COALESCE(addr_Formatted.PostalCode,'0') = COALESCE(REPLACE(addr.PostalCode, '-', NULL),'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        IF (COALESCE(addr_Formatted.NeighborhoodId,'0') = COALESCE(addr.NeighborhoodId,'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        IF (COALESCE(addr_Formatted.MunicipalityId,'0') = COALESCE(addr.MunicipalityId,'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        IF (COALESCE(addr_Formatted.StateId,'0') = COALESCE(addr.StateId,'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        IF (COALESCE(addr_Formatted.CountryId,'0') = COALESCE(addr.CountryId,'0')) THEN
            ErrorMessage := ErrorMessage || '1';
        ELSE
            ErrorMessage := ErrorMessage || '0';
        END IF;

        RETURN ErrorMessage;
    END;

  FUNCTION GetMatchValue(ErrorMessage VARCHAR2) RETURN NUMBER
  AS
    /*--
    Chamada:
    GetMatchValue(ErrorMessage)
    Parametros:
    - vetor de 0 e 1 indicando quais os campos do endereço de entrada casaram com a base de dados
    --*/

    points NUMBER := 0;

    BEGIN
        -- os atributos recebem pesos de acordo com sua importância
        points := ((SUBSTR(ErrorMessage,1,1) * 5) + -- o peso do tipo da via é 5
                   (SUBSTR(ErrorMessage,2,1) * 5)  + -- o peso do prefixo da via é 5
                   (SUBSTR(ErrorMessage,3,1) * 25) + -- o peso do nome da via é 25
                   (SUBSTR(ErrorMessage,4,1) * 12) + -- o peso do numero da via é 10
                   (SUBSTR(ErrorMessage,5,1) * 5) + -- o peso do cep4 da via é 5
                   (SUBSTR(ErrorMessage,6,1) * 10) + -- o peso do cep5 da via é 10
                   (SUBSTR(ErrorMessage,7,1) * 15) + -- o peso do cep da via é 15
                   (SUBSTR(ErrorMessage,8,1) * 8) + -- o peso do bairro da via é 10
                   (SUBSTR(ErrorMessage,9,1) * 5)  + -- o peso do municipio é 5
                   (SUBSTR(ErrorMessage,10,1) * 5)  + -- o peso do estado é 5
                   (SUBSTR(ErrorMessage,11,1) * 5));  -- o peso do pais é 5

        RETURN points;
    END;




/* -------------------------------------------------------------------------

----
   Esta função remove os segmentos repetidos (mesmo id) usando o match_value
   Chamada: GetBestSegment(pVectorAddr)
   Parametros: - pVectorAddr é um vetor de gc_geo_addr;
               - se podeIgual for FALSE, quando tivermos o melhor repetido, 
                 não retorna nada;
------------------------------------------------------------------------------*/
FUNCTION GetBestSegments(pVectorAddr IN GC_TP_GEO_ADDR_ARRAY) RETURN GC_TP_GEO_ADDR_ARRAY 
AS 
   TYPE SegmentType IS RECORD (
     match_value NUMBER, 
     segment_index NUMBER -- índice no vetor recebido
  );
  
  TYPE SegmentTableType IS TABLE OF SegmentType INDEX BY VARCHAR2(100); 
   
   vAddr GC_TP_GEO_ADDR_ARRAY := GC_TP_GEO_ADDR_ARRAY();
   
   CURSOR curSegments IS
      SELECT SegmentId
      FROM TABLE(pVectorAddr)
      ORDER BY SegmentId, MatchValue DESC;
      
   vListaSegmentos SegmentTableType;
   vSegIndex NUMBER;

BEGIN
   IF (pVectorAddr.count > 0) THEN
      FOR i IN 1..pVectorAddr.COUNT
      LOOP
         IF (pVectorAddr(i).SegmentId IS NOT NULL) AND ((NOT vListaSegmentos.EXISTS(pVectorAddr(i).SegmentId)) OR (pVectorAddr(i).MatchValue > vListaSegmentos(pVectorAddr(i).SegmentId).match_value)) THEN 
            vListaSegmentos(pVectorAddr(i).SegmentId).match_value   := pVectorAddr(i).MatchValue;
            vListaSegmentos(pVectorAddr(i).SegmentId).segment_index := i;
         END IF;
      END LOOP;
      
      vSegIndex := vListaSegmentos.FIRST;
      WHILE vSegIndex IS NOT NULL LOOP
      
         vAddr.EXTEND(1);
         vAddr(vAddr.COUNT) := pVectorAddr(vListaSegmentos(vSegIndex).segment_index);
         vSegIndex := vListaSegmentos.NEXT(vSegIndex);
      END LOOP; 

   END IF;
   RETURN vAddr;
END;

   FUNCTION GeocodeReverse(scheme IN VARCHAR2, x IN NUMBER, y IN NUMBER) RETURN GC_TP_GEO_ADDR AS
   /*--
   Chamada:
   SELECT geocoder.GEOCODEREVERSE('scheme', X, Y) FROM DUAL;
   */--

      v_road_id           NUMBER;
      v_segment_id        NUMBER;
      v_street_type       VARCHAR2(20);
      v_street_prefix     VARCHAR2(20);
      v_street_name       VARCHAR2(200);
      v_neighborhood      VARCHAR2(50);
      v_postal_code       VARCHAR2(20);
      vl_postal_code       VARCHAR2(20);
      vr_postal_code       VARCHAR2(20);
      v_ufmun_cod         NUMBER;
      v_municipality      VARCHAR2(100);
      v_state             VARCHAR2(50);
      v_country           VARCHAR2(50);
      v_r_start_hn        NUMBER;
      v_r_end_hn          NUMBER;
      v_l_start_hn        NUMBER;
      v_l_end_hn          NUMBER;
      v_house_number      NUMBER;
      v_left_scheme       NUMBER;
      v_right_scheme      NUMBER;
      v_side_number_aux   CHAR(1);
      vNumberType NUMBER;
      vLineString CLOB;
      vRelate VARCHAR2(200);
      idx                 NUMBER;
         v_country_id  NUMBER;
      addr GC_TP_GEO_ADDR; -- tipo endereço

   BEGIN

    -- Validar se o ponto está dentro MBR Brasil antes de executar o SDO_NN
      SELECT SDO_GEOM.RELATE(MDSYS.SDO_GEOMETRY(2001,8307,MDSYS.SDO_POINT_TYPE(x, y,NULL),NULL,NULL), 'DETERMINE', SDO_GEOMETRY(2003,8307,NULL,SDO_ELEM_INFO_ARRAY(1,1003,3),SDO_ORDINATE_ARRAY(-73.991482487999,-33.751050571999, -32.378186508999,5.271920717)), 0.05) INTO vRelate
      FROM DUAL ;

      IF vRelate <> 'INSIDE' THEN
       addr := GC_TP_GEO_ADDR();
      ELSE
         -- Recupera o segmento mais próximo do par de cordenadas passado e as informações da via a qual o segmento pertence.
         EXECUTE IMMEDIATE
         'SELECT road_id, road_segment_id, postal_code, 
                   r_start_hn, r_end_hn, l_start_hn, l_end_hn, type, prefix, base_name, neighborhood, ufmun_cod, digibase_left_scheme, digibase_right_scheme, country_id
            FROM mapa_urbano.vw_gc_road_segment
         WHERE SDO_NN(geom, SDO_GEOMETRY(2001,8307,SDO_POINT_TYPE(:1, :2, NULL), NULL,NULL),''sdo_batch_size=10'')=''TRUE'' 
          AND rownum < 2
          AND base_name IS NOT NULL'
         INTO v_road_id, v_segment_id, v_postal_code, v_r_start_hn, v_r_end_hn, v_l_start_hn, v_l_end_hn,
                v_street_type, v_street_prefix, v_street_name, v_neighborhood, v_ufmun_cod, v_left_scheme, v_right_scheme, v_country_id
         USING x, y;

         -- Recupera o nome do municipio
         EXECUTE IMMEDIATE
         'SELECT t1.municipality
         FROM ' || scheme || '.gc_municipality t1
         WHERE t1.ufmun_cod = ' || v_ufmun_cod
         INTO v_municipality;
         ----------------------------------

         -- Recupera o nome do estado
         EXECUTE IMMEDIATE
         'SELECT t1.abbreviation
         FROM ' || scheme || '.gc_state t1
         WHERE t1.state_id = ' || SUBSTR(v_ufmun_cod, 0, 2)
         INTO v_state; 
         ----------------------------------
         v_country := 'BR';
           
         SELECT t.geom.GET_WKT() INTO vLineString
         FROM mapa_urbano.vw_gc_road_segment t
         WHERE t.road_segment_id = v_segment_id and t.road_id = v_road_id;
           
         IF DBMS_LOB.getlength(vLineString) > 4000 THEN
            v_house_number := NULL; -- Oracle não consegue trabalhar com VARCHAR maior que 4000 caracteres
         ELSE
            idx := GEOCODER.GetProjectReverse(vLineString, TO_NUMBER(x), TO_NUMBER(y));

            
            IF ((v_r_start_hn IS NOT NULL) AND (v_r_end_hn IS NOT NULL)) THEN -- se o lado direito não é nulo
               v_house_number := ROUND(((((v_r_end_hn - v_r_start_hn) * idx)/100) + v_r_start_hn));
               IF EVEN_ODD(v_house_number) <> EVEN_ODD(v_right_scheme) THEN
                  v_house_number := v_house_number + 1;
               END IF;
            ELSE -- se o lado direito é nulo, procura do lado esquerdo
               v_house_number := ROUND(((((v_l_end_hn - v_l_start_hn) * idx)/100) + v_l_start_hn));
               IF EVEN_ODD(v_house_number) <> EVEN_ODD(v_left_scheme) THEN
                  v_house_number := v_house_number + 1;
               END IF;
            END IF;
         END IF;

         v_side_number_aux := FindNumberSide(v_house_number, v_left_scheme, v_right_scheme);
         /* IF v_side_number_aux IS NULL THEN
           v_side_number_aux := FindNumberSide(v_house_number, v_left_scheme, v_right_scheme, v_l_start_hn, v_l_end_hn, v_r_start_hn, v_r_end_hn, vNumberType, cSearchType_Indef);
           IF v_side_number_aux IS NULL THEN
              v_side_number_aux := FindNumberSide(v_house_number, v_left_scheme, v_right_scheme, v_l_start_hn, v_l_end_hn, v_r_start_hn, v_r_end_hn, vNumberType, cSearchType_Todos);
           END IF;
         END IF;*/

         addr := GC_TP_GEO_ADDR();

         addr.StreetType := v_street_type;
         addr.StreetPrefix := v_street_prefix;
         addr.StreetName := v_street_name;
         addr.HouseNumber := v_house_number;
         addr.PostalCode := v_postal_code;
         addr.Neighborhood := v_neighborhood;
         addr.Municipality := v_municipality;
         addr.MunicipalityId := v_ufmun_cod;
         addr.State := v_state; 
         addr.Country := v_country;
         addr.SegmentId := v_segment_id;
         addr.Longitude := x;
         addr.Latitude := y;
         addr.Side := v_side_number_aux;
         /* 
           IF v_side_number_aux = 'L' THEN
              addr.PostalCode := vl_postal_code;
           ELSIF v_side_number_aux = 'R' THEN
              addr.PostalCode := vr_postal_code;
           ELSE
              addr.PostalCode := v_postal_code;
           END IF;
         */
      END IF;

      RETURN addr;
   END;

   

     
    /** Versão atual da função que localiza o ponto em uma geometria usando o pacote SDO_LRS 
        Além de localizar o ponto, ela faz um deslocamento desse ponto em direção a quadra de acordo com o LADO da numeração encontrada
        Parâmetros: vGeometry - Geometria em questão
                    vPositionIdx - Porcentagem a ser deslocada
                    vSide - Pode ser 'L' (Left), 'R' (Right) e NULL quando a numeração não foi definida
        Retorno: Ponto deslocado na geometria informada.
        Criada em 02/08/2012 - R.GIL
    */
    FUNCTION GetRoadPoint(pGeometry SDO_GEOMETRY, pNumber NUMBER, pLeftScheme VARCHAR2, 
                          pRightScheme VARCHAR2, pLeftStartHN NUMBER, pLeftEndHN NUMBER,
                          pRightStartHN NUMBER, pRightEndHN NUMBER, pSide CHAR := NULL) RETURN SDO_GEOMETRY 
    AS
       vRoadPoint  SDO_GEOMETRY := NULL;
       vLength     NUMBER;
       vPosition   NUMBER;
       vSideSign   NUMBER;
       cPointOffset CONSTANT NUMBER := 10;
       cSegmentOffset CONSTANT NUMBER := 13; 
       vDiminfo SDO_DIM_ARRAY := MDSYS.SDO_DIM_ARRAY(MDSYS.SDO_DIM_ELEMENT('X',-180,180,0.05),MDSYS.SDO_DIM_ELEMENT('Y',-90,90,0.05));
    BEGIN
       
       IF pGeometry IS NOT NULL THEN
          IF pSide IS NULL THEN
             vSideSign := 0;
          ELSIF pSide = 'L' THEN 
             vSideSign := 1;
          ELSE
             vSideSign := -1;
          END IF;
          
          IF NOT vUsarOffset THEN 
             vSideSign := 0;
          END IF;
          
          vLength := SDO_LRS.GEOM_SEGMENT_LENGTH(pGeometry,vDiminfo);
          vPosition := vLength * PositionNumber(pNumber, pLeftStartHN, pLeftEndHN, pRightStartHN, pRightEndHN, pLeftScheme, pRightScheme) / 100;
          
          IF vUsarOffset THEN 
             IF vPosition <= cSegmentOffset THEN
                vPosition := cSegmentOffset;
             ELSIF vPosition >= vLength - cSegmentOffset THEN
                vPosition := vLength - cSegmentOffset;
             END IF;
          END IF;
          
          IF vPosition > vLength THEN
             vPosition := vLength*0.5;
          END IF;
          
          vRoadPoint := SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(pGeometry, vDiminfo), vPosition, cPointOffset*vSideSign)); 
       END IF;

       RETURN vRoadPoint;

    END;
    
    -- FUNÇÕES ANTIGAS -- 
    
    /*
    FUNCTION GetRoadPoint(v_geom SDO_GEOMETRY, idx NUMBER) RETURN SDO_GEOMETRY AS

    -- Dado uma geometria (trecho ou trechos agrupados) e um indice, esta função retorna um ponto referente a posição
    -- do indice na geometria.
    -- Esta função foi criada porque estava dando um erro na chamada das funções GetCoordinateX e Get CoordinateY quando o WKT
    -- do agrupamento dos trechos da via tinha mais de 4000 caracteres (limitação do Oracle). Agora, quando o WKT tem mais de
    -- 4000 caracteres, é considerando apenas centroide da da geometria.

    v_wkt          VARCHAR2(30000) := v_geom.GET_WKT();
    v_centroide    SDO_GEOMETRY;
    dist NUMBER;
    geoma SDO_GEOMETRY;
    geomb SDO_GEOMETRY;

    BEGIN

    BEGIN
        SELECT SDO_GEOMETRY('POINT(' || REPLACE(geocoder.GetCoordinateX(v_wkt, idx), ',', '.') || ' ' || REPLACE(geocoder.GetCoordinateY(v_wkt, idx), ',', '.') ||')') INTO geomb FROM dual;
        --SELECT SDO_GEOMETRY('POINT(' || REPLACE(geocoder.GetCoordinateX(v_wkt, idx), ',', '.') || ' ' || REPLACE(geocoder.GetCoordinateY(v_wkt, idx), ',', '.') ||', 8307)') INTO geomb FROM dual;
        EXCEPTION
            WHEN OTHERS THEN
                v_centroide := sdo_geom.sdo_centroid(SDO_GEOM.SDO_MBR(v_geom), 0.005);
                sdo_geom.sdo_closest_points(v_centroide, v_geom, 0.0001, NULL, dist, geoma, geomb);
                IF (dist = 0) THEN
                    geomb := v_centroide;
                END IF;
    END;

    RETURN geomb;

    END;*/
    
    /*FUNCTION GetRoadPoint2(v_geom SDO_GEOMETRY, idx NUMBER) RETURN SDO_GEOMETRY
    AS

    -- Dado uma geometria (trecho ou trechos agrupados) e um indice, esta função retorna um ponto referente a posição
    -- do indice na geometria.
    -- Esta função foi criada porque estava dando um erro na chamada das funções GetCoordinateX e Get CoordinateY quando o WKT
    -- do agrupamento dos trechos da via tinha mais de 4000 caracteres (limitação do Oracle). Agora, quando o WKT tem mais de
    -- 4000 caracteres, é considerando apenas centroide da da geometria.

    vConvertedGeom SDO_GEOMETRY;
    vCenterPoint SDO_GEOMETRY;
    vDiminfo SDO_DIM_ARRAY;
    vTamanho NUMBER;

    BEGIN
       SELECT diminfo INTO vDiminfo
       FROM all_sdo_geom_metadata 
       WHERE owner = 'GC_ORACLE' 
         AND table_name = 'vw_GC_ROAD_SEGMENT';

       IF idx > 0 THEN
          vTamanho := SDO_GEOM.SDO_LENGTH(v_geom,vDiminfo)/(100/idx);
       ELSE
          vTamanho := 0;
       END IF;
       RAISE_APPLICATION_ERROR(-20000, vTamanho);
       vCenterPoint := sdo_cs.transform(SDO_LRS.CONVERT_TO_STD_GEOM(SDO_LRS.LOCATE_PT(SDO_LRS.CONVERT_TO_LRS_GEOM(v_geom, vDiminfo), vTamanho)),8307);
    
       RETURN vCenterPoint;

    END;*/

END GEOCODER;
/
