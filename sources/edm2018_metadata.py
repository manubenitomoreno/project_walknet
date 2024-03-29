files = {'viajes':'EDM2018VIAJES','individuos':'EDM2018INDIVIDUOS','hogares':'EDM2018HOGARES','etapas':'EDM2018XETAPAS'}

schemas = {
    'etapas':{
        'ID_HOGAR':int,
        'ID_IND':int,
        'ID_VIAJE':int,
        'ID_ETAPA':int,
        'EMODO':int,
        'EMODO1':str,
        'MODO_PRIORITARIO':int,
        'MODO':int, 
        'ELINEAEMPRESA_ORIGINAL':str,
        'ESUBIDA':str,
        'ESUBIDA_cod':str,
        'EBAJADA':str,
        'EBAJADA_cod':str,
        'ETITULO':str,
        'EESTACIONA':str,
        'EOCUPACION':str,
        'ETDESPH':str,
        'TIPO_ENCUESTA':str,
        'COD_MUN_PARADA':str,
        'ELE_G_POND_Esc2':float},
    'viajes':{
        'ID_HOGAR': int,
        'ID_IND': int,
        'ID_VIAJE': int,
        'VORI': int,
        'VORIHORAINI': int,
        'VDES': int,
        'VDESHORAFIN': int,
        'VFRECUENCIA': int,
        'VVEHICULO': float,
        'VNOPRIVADO': float,
        'VNOPUBLICO': float,
        'VORIZT1259': str,
        'VDESZT1259': str,
        'TIPO_ENCUESTA': str,
        'N_ETAPAS_POR_VIAJE': int,
        'MOTIVO_PRIORITARIO': int,
        'DISTANCIA_VIAJE': float,
        'MODO_PRIORITARIO': int,
        'ELE_G_POND_ESC2': float},
    'individuos':{
        'ID_HOGAR':int,
        'ID_IND':int,
        'C2SEXO':int,
        'EDAD_FIN':int,
        'ELE_G_POND':float,
        'C4NAC':int,
        'C5CAM':int,
        'C6CARNE':int,
        'C7ESTUD':int,
        'C8ACTIV':int,
        'C9PROF':str,
        'C10SECTOR':str,
        'C13TARJETA':int,
        'C14ABONO':str,
        'DDIA':int,
        'DMES':int,
        'DANNO':int,
        'DIASEM':int,
        'DNOVIAJO':str,
        'C11ZT1259':str,
        'C12ZT1259':str,
        'CPMR':int,
        'TIPO_ENCUESTA':str},
    'hogares':{
        'ID_HOGAR':int,
        'CODMUNI':int,
        'NOMMUNI':str,
        'CODPROV':str,
        'NOMPROV':str,
        'ZT1259':str,
        'CZ208':str,
        'ELE_HOGAR_NUEVO':float,
        'TIPO_ENCUESTA':str,
        'A1PER':int,
        'A2PER4':int,
        'B1NVE':int,
        'V1B11TIPO':str,
        'V1B12CARB':str,
        'V1B13EST':str,
        'V2B11TIPO1':str,
        'V2B12CARB1':str,
        'V2B13EST1':str,
        'V3B11TIPO1':str,
        'V3B12CARB1':str,
        'V3B13EST1':str,
        'V4B11TIPO1':str,
        'V4B12CARB1':str,
        'V4B13EST1':str,
        'V5B11TIPO1':str,
        'V5B12CARB1':str,
        'V5B13EST1':str,
        'N_MIEMBROS_POR_HOGAR':int,
        'N_VIAJES_POR_HOGAR':int},
        }