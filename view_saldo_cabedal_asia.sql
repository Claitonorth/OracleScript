CREATE OR REPLACE VIEW VW_CO_SALDO_CABEDAL_CHINA AS
 WITH REMESSAS AS (
             SELECT COD_ITEM
                   ,LISTAGG(LPAD(REMESSA,12,' '), ' || ') WITHIN GROUP (ORDER BY REMESSA)REMESSA
               FROM( 
               SELECT  DISTINCT 
                       (REME.FABR_COD_FABRICA||'-'||REME.PLANO) REMESSA,
                       MACV.ITEM_COD_ITEM_ESTOQUE COD_ITEM
                  FROM REMESSA REME, 
                       LOTE, 
                       ITEM_PEDIDO_TALAO ITPT, 
                       ITEM_PEDIDO_NUMERACAO INUM,
                       PEDIDO PEDI,
                       ITEM_PEDIDO IPED,
                       MERCADO MERC,
                       material_combinacao_venda macv,
                       ITEM_EMPENHO IE
                 WHERE LOTE.REME_COD_REMESSA = REME.COD_REMESSA
                   AND LOTE.FABR_COD_FABRICA_PROGRAMA = REME.FABR_COD_FABRICA
                   AND ITPT.TALO_LOTE_COD_FABRICA = LOTE.FABR_COD_FABRICA_PROGRAMA
                   AND ITPT.TALO_LOTE_COD_LOTE = LOTE.COD_LOTE
                   AND PEDI.COD_PEDIDO = ITPT.ITPE_PEDI_COD_PEDIDO
                   AND PEDI.COD_PEDIDO               = IPED.PEDI_COD_PEDIDO
                   AND ITPT.ITPE_COD_ITEM_PEDIDO     = IPED.COD_ITEM_PEDIDO
                   AND IPED.PEDI_COD_PEDIDO          = INUM.ITPE_PEDI_COD_PEDIDO
                   AND IPED.COD_ITEM_PEDIDO          = INUM.ITPE_COD_ITEM_PEDIDO
                   AND IPED.GRAD_COD_GRADE           = INUM.ITGR_GRAD_COD_GRADE
                   AND REME.ANO                      >= TRUNC(TO_CHAR(SYSDATE,'YYYY'))-1
                   and macv.cove_refe_linh_cod_linha = iped.cove_refe_linh_cod_linha
                   and macv.cove_refe_cod_referencia = iped.cove_refe_cod_referencia
                   and macv.cove_cod_comb_venda      = iped.cove_cod_comb_venda
                   and macv.marc_cod_marca           = iped.marc_cod_marca
                   AND LOTE.MARC_COD_MARCA           = IPED.MARC_COD_MARCA
                   AND MACV.TAMA_COD_TAMANHO         = INUM.ITGR_TAMA_COD_TAMANHO
                   and macv.risco                    = 2
                   AND IPED.GRAD_MERC_COD_MERCADO    = MERC.COD_MERCADO
                   AND MACV.TIPO_MERCADO             = MERC.TIPO
                   AND IPED.PEDI_COD_PEDIDO          = IE.ITPE_PEDI_COD_PEDIDO
                   AND IPED.COD_ITEM_PEDIDO          = IE.ITPE_COD_ITEM_PEDIDO
                   AND IE.ITEM_COD_ITEM              = MACV.ITEM_COD_ITEM_ESTOQUE
                   --AND PEDI.SITUACAO_PEDIDO          != 10
                   AND PEDI.DATA_EMISSAO             > SYSDATE-700
                   AND PEDI.MAEM_COD_MARCA_EMPRESA   IN (1,4)
                   AND IPED.PROGRAMADO = 'S'
                HAVING SUM(INUM.QUANTIDADE)> 0 
                 GROUP BY REME.FABR_COD_FABRICA||'-'||REME.PLANO ,
                          MACV.ITEM_COD_ITEM_ESTOQUE
                ORDER BY 1          
      )GROUP BY COD_ITEM
       ORDER BY 2
       )       
 SELECT TMP.COD_ITEM
       ,TMP.COD_ESTOQUE
       ,TMP.DESCRICAO_ITEM
       ,SUM(TMP.QTD_EMPENHADA)EMPENHADO
       ,MAX(TMP.QTD_COMPRA)COMPRADO
       ,MAX(TMP.QTD_COMPRA) - SUM(TMP.QTD_EMPENHADA) SALDO
       ,MAX(TMP.DATA_OC)MAIOR_DATA_OC_USADA
       ,TO_DATE(':DATA_LIMITE_OC','DD-MM-RRRR')AS DATA_LIMITE_OC
       ,(SELECT LISTAGG(PENT.ITOC_COD_ORDEM_COMPRA||'-'||pent.data_programada, ' || ') WITHIN GROUP (ORDER BY PENT.DATA_PROGRAMADA)
                  FROM item_ordem_compra   ioco,
                       ordem_compra        orco,
                       programacao_entrega pent
                 WHERE ioco.oroc_cod_ordem_compra = orco.cod_ordem_compra
                   AND ioco.oroc_fiem_cod_empresa = orco.fiem_cod_empresa
                   AND ioco.oroc_cod_ordem_compra = pent.itoc_cod_ordem_compra
                   AND ioco.oroc_fiem_cod_empresa = pent.itoc_fiem_cod_empresa
                   AND ioco.item_cod_item = pent.itoc_item_cod_item
                   AND ioco.sequencia = pent.itoc_sequencia
                   AND ioco.item_cod_item = TMP.cod_item
                   AND TO_DATE(PENT.DATA_PROGRAMADA,'DD-MM-RRRR') <= TO_DATE(':DATA_LIMITE_OC','DD-MM-RRRR') -- Filtra pela data limite
                   AND orco.situacao NOT IN ('C', 'X') 
                   and not exists( select 1 
                                    from   item_nf_ordem info -- Não considerar OCs de transferência
                                    ,      nota_fiscal nofi
                                    ,      filial_empresa fiem
                                    where  nofi.fiem_cod_empresa = info.itnf_nofi_fiem_cod_empresa
                                    and    nofi.sequencia_nf = info.itnf_nofi_sequencia_nf
                                    and    fiem.pess_cod_pessoa_filial = nofi.pess_cod_pessoa
                                    and    info.itoc_oroc_cod_ordem_compra = ioco.oroc_cod_ordem_compra
                                    and    info.itoc_oroc_fiem_cod_empresa = ioco.oroc_fiem_cod_empresa
                                    and    info.itoc_item_cod_item = ioco.item_cod_item )
                   AND ioco.situacao NOT IN ('X')--,'A')
                   )DATAS_OC
        ,RR.REMESSA
        ,(   SELECT LISTAGG(LINHA||'-'||REFERENCIA||'-'||COMB, ' || ') WITHIN GROUP (ORDER BY 1,2,3)
               FROM(
                   SELECT DISTINCT
                          MCV.COVE_REFE_LINH_COD_LINHA LINHA
                         ,MCV.COVE_REFE_COD_REFERENCIA REFERENCIA
                         ,MCV.COVE_COD_COMB_VENDA COMB
                         ,MCV.ITEM_COD_ITEM_ESTOQUE ITEM_CAB
                     FROM MATERIAL_COMBINACAO_VENDA MCV
                    WHERE MCV.MARC_COD_MARCA = 0
                      AND MCV.TIPO_MERCADO   = 1
                    ORDER BY 1,2,3  
                   )
              WHERE ITEM_CAB = TMP.COD_ITEM
              )LINHAS_REFERENCIA_USADO
  FROM (
        SELECT x.*,
               (SELECT SUM(pent.quantidade_programada)
                  FROM item_ordem_compra   ioco,
                       ordem_compra        orco,
                       programacao_entrega pent
                 WHERE ioco.oroc_cod_ordem_compra = orco.cod_ordem_compra
                   AND ioco.oroc_fiem_cod_empresa = orco.fiem_cod_empresa
                   AND ioco.oroc_cod_ordem_compra = pent.itoc_cod_ordem_compra
                   AND ioco.oroc_fiem_cod_empresa = pent.itoc_fiem_cod_empresa
                   AND ioco.item_cod_item = pent.itoc_item_cod_item
                   AND ioco.sequencia = pent.itoc_sequencia
                   AND ioco.item_cod_item = x.cod_item
                   AND TO_DATE(pent.data_programada,'DD-MM-RRRR') <= TO_DATE(':DATA_LIMITE_OC','DD-MM-RRRR') -- Filtra pela data limite
                   AND orco.situacao NOT IN ('C', 'X') -- nao pode estar como cadastrada ou cancelada
                   and not exists( select 1 
                                    from   item_nf_ordem info -- Não considerar OCs de transferência
                                    ,      nota_fiscal nofi
                                    ,      filial_empresa fiem
                                    where  nofi.fiem_cod_empresa = info.itnf_nofi_fiem_cod_empresa
                                    and    nofi.sequencia_nf = info.itnf_nofi_sequencia_nf
                                    and    fiem.pess_cod_pessoa_filial = nofi.pess_cod_pessoa
                                    and    info.itoc_oroc_cod_ordem_compra = ioco.oroc_cod_ordem_compra
                                    and    info.itoc_oroc_fiem_cod_empresa = ioco.oroc_fiem_cod_empresa
                                    and    info.itoc_item_cod_item = ioco.item_cod_item )
                   AND ioco.situacao != 'X') qtd_compra
          FROM (SELECT cod_item,
                       cod_estoque,
                       i.descricao_item,
                       ie.data_oc,
                       SUM(ie.qtd_empenho) qtd_empENHADA
                  FROM item_empenho ie, item i
                 WHERE ie.item_cod_item = i.cod_item
                 GROUP BY cod_item, cod_estoque, i.descricao_item, ie.data_oc ) x
        ORDER BY 2
       )TMP
       ,REMESSAS RR
  WHERE TMP.COD_ITEM = RR.COD_ITEM(+)
    --AND TMP.COD_ESTOQUE IN ('CAB0000994')     
    --AND TMP.DATA_LIMITE_OC = '31JAN2025'
  GROUP BY
        TMP.COD_ESTOQUE
       ,TMP.COD_ITEM
       ,TMP.DESCRICAO_ITEM
       ,RR.REMESSA
  ORDER BY 1
/  
