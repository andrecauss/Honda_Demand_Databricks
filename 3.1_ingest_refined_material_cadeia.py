# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Cabecalho do Notebook
# Databricks notebook source

# =============================================================================
# Projeto........: Parts Handbook
# Notebook.......: 03_ingest_refined_material_cadeia
# Camada.........: refined
# Objetivo.......: Extrair a relação material <-> item principal da cadeia de
#                  substituição a partir da tabela raw material_cadastrao,
#                  aplicar regras de limpeza de sufixo e persistir na tabela
#                  material_cadeia.
#
# Desenvolvedor..: André Causs
# Criado em......: 04/08/2026
#
# Premissas
# -----------------------------------------------------------------------------
# • Fonte: parts_hdbk_sandbox.pr_cadastrao.material_cadastrao (camada raw).
# • Colunas selecionadas: empresa, material, item_principal_cadeia.
# • Se item_principal_cadeia for NULL, vazio ou "sim", assume o próprio material.
# • Limpeza de sufixo no item_principal_cadeia:
#     - Empresa 0200: remove " C" (espaço(s) + C) no final.
#     - Empresa 0500: remove " C" ou " G" (espaço(s) + C ou G) no final.
# • Chave primária composta: (empresa, material).
#
# Tabela Fonte
# -----------------------------------------------------------------------------
# parts_hdbk_sandbox.pr_cadastrao.material_cadastrao
#
# Tabela Destino
# -----------------------------------------------------------------------------
# parts_hdbk_sandbox.pr_cadastrao.material_cadeia
#
# Histórico de Alterações
# -----------------------------------------------------------------------------
# Data       Autor          Versão  Descrição
# ---------- -------------- ------- --------------------------------------------
# 04/08/2026 André Causs    1.0.0   Criação da estrutura inicial.
# =============================================================================

# COMMAND ----------

# DBTITLE 1,Imports e funcao auxiliar
from pyspark.sql import functions as F


def clean_material_suffix(col, empresa_col):
    """Remove sufixo de letra (C/G) precedido de espaços conforme regra por empresa.

    Regras:
      - Empresa 0200: se o campo termina em " C" (espaço(s) + C), remove o sufixo.
      - Empresa 0500: se o campo termina em " C" ou " G" (espaço(s) + C/G), remove o sufixo.
    """
    return (
        F.when(
            (empresa_col == "0200") & col.rlike(r".*\s+C$"),
            F.rtrim(F.regexp_replace(col, r"\s+C$", ""))
        ).when(
            (empresa_col == "0500") & col.rlike(r".*\s+[CG]$"),
            F.rtrim(F.regexp_replace(col, r"\s+[CG]$", ""))
        ).otherwise(col)
    )

# COMMAND ----------

# DBTITLE 1,Montar tabela completa a partir da raw
# Selecionar colunas da tabela fonte e tratar item_principal_cadeia
# Regra: se NULL, vazio ou "sim", assume o próprio material
df_cadeia = (
    spark.table("parts_hdbk_sandbox.pr_cadastrao.material_cadastrao")
    .select("empresa", "material", "item_principal_cadeia")
    .withColumn(
        "item_principal_cadeia",
        F.when(
            (F.col("item_principal_cadeia").isNull()) |
            (F.trim(F.col("item_principal_cadeia")) == "") |
            (F.lower(F.trim(F.col("item_principal_cadeia"))) == "sim"),
            F.col("material")
        ).otherwise(F.col("item_principal_cadeia"))
    )
)

print(f"Tabela montada: {df_cadeia.count()} linhas")

# COMMAND ----------

# DBTITLE 1,Limpeza de sufixo no item_principal_cadeia
# Aplicar limpeza de sufixo (" C" / " G") apenas no item_principal_cadeia
df_cadeia = df_cadeia.withColumn(
    "item_principal_cadeia",
    clean_material_suffix(F.col("item_principal_cadeia"), F.col("empresa"))
)

print(f"Limpeza aplicada. Registros com cadeia diferente do material:")
print(f"  {df_cadeia.filter(F.col('material') != F.col('item_principal_cadeia')).count()} linhas")
print("\nAmostra:")
display(
    df_cadeia.filter(
        F.col("material") != F.col("item_principal_cadeia")
    ).limit(10)
)

# COMMAND ----------

# DBTITLE 1,Persistencia na tabela Delta
# Dropar tabela antiga se existir
spark.sql("DROP TABLE IF EXISTS parts_hdbk_sandbox.pr_cadastrao.material_cadeia")

# Criar tabela Delta
df_cadeia.write.saveAsTable("parts_hdbk_sandbox.pr_cadastrao.material_cadeia")

print(f"Tabela parts_hdbk_sandbox.pr_cadastrao.material_cadeia criada com sucesso.")
print(f"Total de registros: {df_cadeia.count()}")

# COMMAND ----------

# DBTITLE 1,Definir PK composta (empresa, material)
# MAGIC %sql
# MAGIC -- Definir colunas NOT NULL (requisito para PK)
# MAGIC ALTER TABLE parts_hdbk_sandbox.pr_cadastrao.material_cadeia
# MAGIC ALTER COLUMN empresa SET NOT NULL;
# MAGIC
# MAGIC ALTER TABLE parts_hdbk_sandbox.pr_cadastrao.material_cadeia
# MAGIC ALTER COLUMN material SET NOT NULL;
# MAGIC
# MAGIC -- Adicionar constraint de chave primária composta
# MAGIC ALTER TABLE parts_hdbk_sandbox.pr_cadastrao.material_cadeia
# MAGIC ADD CONSTRAINT pk_material_cadeia PRIMARY KEY (empresa, material);

# COMMAND ----------

# DBTITLE 1,Comentarios e metadados da tabela
# =============================================================================
# COMENTÁRIOS E METADADOS
# =============================================================================
REFINED_TABLE = "parts_hdbk_sandbox.pr_cadastrao.material_cadeia"

# Comentário da tabela
TABLE_COMMENT = """
Camada Refined da cadeia de substituicao de materiais.
Extraida da tabela raw material_cadastrao com tratamento de sufixo.

Chave Primaria: empresa + material
Atualizacao: Derivada da carga da material_cadastrao

Regras de negocio aplicadas:
  - item_principal_cadeia NULL/vazio/sim -> assume o proprio material
  - Empresa 0200: sufixo ' C' removido do item_principal_cadeia
  - Empresa 0500: sufixo ' C' ou ' G' removido do item_principal_cadeia

Relacionamentos:
  - material -> parts_hdbk_sandbox.pr_cadastrao.material_cadastrao (FK)
  - item_principal_cadeia -> material principal na cadeia de substituicao
"""

spark.sql(f"""
    COMMENT ON TABLE {REFINED_TABLE} IS '{TABLE_COMMENT.replace(chr(39), chr(39)+chr(39))}'
""")

# Comentários nas colunas
COLUMN_COMMENTS = {
    "empresa": "Código da empresa SAP (ex: 0200=2W, 0500=4W). Parte da chave primária.",
    "material": "Código único do material/peça (partnumber SAP). Parte da chave primária.",
    "item_principal_cadeia": "Material principal na cadeia de substituição. Quando NULL/vazio/sim na origem, assume o próprio material. Sufixos ' C'/' G' removidos conforme regra por empresa.",
}

for column_name, comment in COLUMN_COMMENTS.items():
    escaped_comment = comment.replace("'", "''")
    spark.sql(f"COMMENT ON COLUMN {REFINED_TABLE}.{column_name} IS '{escaped_comment}'")

# Tags do Unity Catalog
spark.sql(f"""
    ALTER TABLE {REFINED_TABLE} SET TAGS (
        'domain' = 'materials',
        'layer' = 'refined',
        'source' = 'sap',
        'data_classification' = 'internal'
    )
""")

spark.sql(f"ALTER TABLE {REFINED_TABLE} ALTER COLUMN empresa SET TAGS ('business_key' = 'true')")
spark.sql(f"ALTER TABLE {REFINED_TABLE} ALTER COLUMN material SET TAGS ('business_key' = 'true', 'joins_to' = 'material_cadastrao')")
spark.sql(f"ALTER TABLE {REFINED_TABLE} ALTER COLUMN item_principal_cadeia SET TAGS ('business_key' = 'true')")

# Propriedades customizadas
spark.sql(f"""
    ALTER TABLE {REFINED_TABLE} SET TBLPROPERTIES (
        'business_owner' = 'Demand Planning',
        'technical_owner' = 'Andre Causs',
        'data_domain' = 'Material',
        'source_system' = 'SAP',
        'source_table' = 'parts_hdbk_sandbox.pr_cadastrao.material_cadastrao',
        'refresh_frequency' = 'derived_from_refined',
        'primary_key' = 'empresa, material'
    )
""")

print(f"Metadados completos aplicados a tabela {REFINED_TABLE}")