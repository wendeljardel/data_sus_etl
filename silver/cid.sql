-- Databricks notebook source
DROP TABLE IF EXISTS curso_databricks_uc.silver_datasus.cid;

CREATE TABLE IF NOT EXISTS curso_databricks_uc.silver_datasus.cid AS (
  SELECT *
  FROM curso_databricks_uc.bronze_datasus.cid
);
