# Databricks notebook source
# DBTITLE 1,Propósito do Notebook
# ==============================================================================
# NOTEBOOK: 99 - Funções de Suporte para Sales Order
# ==============================================================================
#
# PROPÓSITO:
#   Utilitário de manutenção manual, fora do pipeline automático. Exclui um
#   ANO/MES inteiro de raw_sales_order — para quando um mês foi carregado
#   errado e precisa ser removido antes de reprocessar.
#
# ARQUITETURA:
#   ┌─────────────────────────────────────────────────────────────────────┐
#   │ INPUT/OUTPUT: parts_hdbk_sandbox.dt_sales_orders.raw_sales_order    │
#   │   (mesma tabela, lida para validar e depois DELETE)                 │
#   └─────────────────────────────────────────────────────────────────────┘
#
# CONVENÇÕES:
#   • Célula 1: apenas visualiza quantos registros seriam removidos
#   • Célula 2: executa o DELETE de fato
#
# DEPENDÊNCIAS:
#   • nenhuma além de pyspark
#
# EXECUÇÃO:
#   ⚠️ NÃO faz parte de nenhum job — é ad-hoc, rodado manualmente.
#   1. Ajustar ANO e MES no topo da primeira célula
#   2. Rodar a célula 1 e conferir a contagem antes de prosseguir
#   3. Só então rodar a célula 2 (DELETE, irreversível sem backup)
#
# AUTOR: Andre Causs - Honda Peças - Planejamento
# ÚLTIMA ATUALIZAÇÃO: 2026-08-09
# ==============================================================================

# =====================================================================================
# André Causs - 25/07/2026 : Exclusão de um mês inteiro da tabela Delta
# Utilizar para remover dados carregados incorretamente antes de um novo processamento.
# =====================================================================================

from pyspark.sql import functions as F

# Parâmetros
TABELA = "parts_hdbk_sandbox.dt_sales_orders.raw_sales_order"
ANO = 2026
MES = 6

# Visualiza quantidade de registros que serão removidos
df_validacao = spark.sql(f"""
SELECT *
FROM {TABELA}
WHERE YEAR(data) = {ANO}
  AND MONTH(data) = {MES}
""")

print(f"Registros encontrados: {df_validacao.count():,}")

# COMMAND ----------

# =====================================================================================
# André Causs - 25/07/2026 : DELETE do mês selecionado
# =====================================================================================

spark.sql(f"""
DELETE FROM {TABELA}
WHERE YEAR(data) = {ANO}
  AND MONTH(data) = {MES}
""")

print("Exclusão concluída.")