# Databricks notebook source
# MAGIC %md
# MAGIC ### First create a catalog named 'ahs-corporation' within Databricks
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS 'ahs-corporation' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG 'ahs-corporation' ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC CREATE SCHEMA IF NOT EXISTS `ahs-corporation`.bronze ;
# MAGIC CREATE SCHEMA IF NOT EXISTS `ahs-corporation`.silver ;
# MAGIC CREATE SCHEMA IF NOT EXISTS `ahs-corporation`.gold ;

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC SHOW SCHEMAS IN `ahs-corporation`;