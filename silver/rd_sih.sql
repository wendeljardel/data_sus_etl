-- Databricks notebook source
DROP TABLE IF EXISTS curso_databricks_uc.silver_datasus.sihsus;

CREATE TABLE IF NOT EXISTS curso_databricks_uc.silver_datasus.sihsus AS


-- tb_cid AS (
--   select *
--   from curso_databricks_uc.silver_datasus.cid
--   where codCID not like '%-%'
--   and codCID10 not like '%.%'
--   qualify row_number() over (partition by codCID10dst order by codCID desc) = 1
-- )


SELECT 
    
    t1.ANO_CMPT,
    t1.MES_CMPT,
    t2._c1 AS descCaraterInternacao,
    t4._c1 AS descEspecialidade,
    t1.CGC_HOSP,
    t1.N_AIH,
    t1.CEP,
    t1.MUNIC_RES,
    t3._c1 AS descSexo,
    t1.UTI_MES_IN,
    t1.UTI_MES_AN,
    t1.UTI_MES_AL,
    t1.UTI_MES_TO,
    t1.UTI_INT_IN,
    t1.UTI_INT_AN,
    t1.UTI_INT_AL,
    t1.UTI_INT_TO,
    t1.DIAR_ACOM,
    t1.QT_DIARIAS,
    t1.PROC_SOLIC,
    t1.PROC_REA,
    FLOAT(REPLACE(t1.VAL_SH, ',', '.')) AS vlSH,
    FLOAT(REPLACE(t1.VAL_SP, ',', '.')) AS vlSP,
    FLOAT(REPLACE(t1.VAL_SADT, ',', '.')) AS vlSADT,
    FLOAT(REPLACE(t1.VAL_RN, ',', '.')) AS vlRN,
    FLOAT(REPLACE(t1.VAL_ACOMP, ',', '.')) AS vlACOMP,
    FLOAT(REPLACE(t1.VAL_ORTP, ',', '.')) AS vlORTP,
    FLOAT(REPLACE(t1.VAL_SANGUE, ',', '.')) AS vlSANGUE,
    FLOAT(REPLACE(t1.VAL_SADTSR, ',', '.')) AS vlSADTSR,
    FLOAT(REPLACE(t1.VAL_TRANSP, ',', '.')) AS vlTRANSP,
    FLOAT(REPLACE(t1.VAL_OBSANG, ',', '.')) AS vlOBSANG,
    FLOAT(REPLACE(t1.VAL_PED1AC, ',', '.')) AS vlPED1AC,
    FLOAT(REPLACE(t1.VAL_TOT, ',', '.')) AS vlTOT,
    FLOAT(REPLACE(t1.VAL_UTI, ',', '.')) AS vlUTI,
    FLOAT(REPLACE(t1.US_TOT, ',', '.')) AS vlUSTOT,
    TO_DATE(t1.DT_INTER, 'yyyymmdd') AS DtInternacao,
    TO_DATE(t1.DT_SAIDA, 'yyyymmdd') AS DtSaida,
    t1.DIAG_PRINC,
    t1.DIAG_SECUN,
    t1.GESTAO,
    t1.IND_VDRL,
    t1.MUNIC_MOV,
    t1.COD_IDADE,
    t1.IDADE,
    t1.DIAS_PERM,
    t1.MORTE,
    t1.TOT_PT_SP,
    t1.CPF_AUT,
    t1.HOMONIMO,
    int(t1.NUM_FILHOS) AS nrFilhos,
    t5._c1 AS descGrauInstrucao,
    t1.CID_NOTIF,
    t1.GESTRISCO,
    t1.INSC_PN,
    t1.CBOR,
    t1.CNAER,
    t1.GESTOR_COD,
    t1.GESTOR_CPF,
    t1.GESTOR_DT,
    t1.CNES,
    t1.CNPJ_MANT,
    t1.INFEHOSP,
    t1.CID_ASSO,
    t1.CID_MORTE,
    t1.COMPLEX,
    t1.ETNIA,
    t1.SEQUENCIA,
    t1.REMESSA,
    t1.AUD_JUST,
    t1.SIS_JUST,
    FLOAT(REPLACE(t1.VAL_SH_FED, ',','.')) AS vlSHFED,
    FLOAT(REPLACE(t1.VAL_SP_FED, ',','.')) AS vlSPFED,
    FLOAT(REPLACE(t1.VAL_SH_GES, ',','.')) AS vlSHGES,
    FLOAT(REPLACE(t1.VAL_SP_GES, ',','.')) AS vlSPGES,
    FLOAT(REPLACE(t1.VAL_UCI, ',','.')) AS vlUCI

FROM curso_databricks_uc.bronze_datasus.sihsus AS t1

LEFT JOIN curso_databricks_uc.bronze_datasus.caraterinternacao AS t2
ON t1.CAR_INT = t2._c0

LEFT JOIN curso_databricks_uc.bronze_datasus.sexo AS t3
ON t1.SEXO = t3._c0

LEFT JOIN curso_databricks_uc.bronze_datasus.especialidades AS t4
ON t1.ESPEC = t4._c0

LEFT JOIN curso_databricks_uc.bronze_datasus.grauinstrucao as t5
ON t1.INSTRU = t5._c0
;
