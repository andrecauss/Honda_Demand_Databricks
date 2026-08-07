# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Sales Orders com Cadeia e Centro Original
# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMP VIEW vw_sales_orders AS
# MAGIC SELECT
# MAGIC   rso.numero_ov,
# MAGIC   rso.data,
# MAGIC   rso.tipo_ov,
# MAGIC   rso.org_vendas,
# MAGIC   rso.canal_dist,
# MAGIC   rso.emissor_da_ordem,
# MAGIC   rso.centro,
# MAGIC   rso.material,  
# MAGIC   rso.quantidade,
# MAGIC   COALESCE(mc.item_principal_cadeia, rso.material) AS item_principal_cadeia,
# MAGIC   k.cen AS centro_original,
# MAGIC   kna.razao_social,
# MAGIC   kna.estado,
# MAGIC   kna.pais
# MAGIC FROM parts_hdbk_sandbox.dt_sales_orders.raw_sales_order rso
# MAGIC LEFT JOIN parts_hdbk_sandbox.pr_cadastrao.material_cadeia mc
# MAGIC   ON rso.material = mc.material
# MAGIC   AND rso.org_vendas = mc.empresa
# MAGIC LEFT JOIN parts_hdbk_sandbox.dm_customers.knvv_sap k
# MAGIC   ON rso.emissor_da_ordem = k.cliente
# MAGIC   AND rso.org_vendas = k.orgv
# MAGIC   AND rso.canal_dist = k.cdst
# MAGIC   AND rso.setor_ativ = k.sa
# MAGIC LEFT JOIN parts_hdbk_sandbox.dm_customers.kna1_sap kna
# MAGIC   ON rso.emissor_da_ordem = kna.cliente
# MAGIC WHERE rso.data >= '${data_minima}'

# COMMAND ----------

# DBTITLE 1,Função gerar_layout_pivotado
from pyspark.sql.functions import date_format, lit
from functools import reduce

# ---------------------------------------------------------------------------
# Parâmetros globais (widgets)
# ---------------------------------------------------------------------------
dbutils.widgets.text("data_minima", "2025-01-01", "1 Data Mínima")
dbutils.widgets.text("ano", "2026", "2 Ano Referência")
dbutils.widgets.text("mes", "7", "3 Mês Referência")

data_minima = dbutils.widgets.get("data_minima")
mes = int(dbutils.widgets.get("mes"))
ano = int(dbutils.widgets.get("ano"))

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------
MESES_PT = {1: "Jan", 2: "Fev", 3: "Mar", 4: "Abr", 5: "Mai", 6: "Jun",
            7: "Jul", 8: "Ago", 9: "Set", 10: "Out", 11: "Nov", 12: "Dez"}
PREFIXOS = {"0200": "HDA", "0500": "HAB"}
ABAS_2W = ["TTL", "0203", "0209", "0232"]
ABAS_4W = ["TTL", "0503", "0505"]
SCHEMA = "parts_hdbk_sandbox.pr_demand"

# ---------------------------------------------------------------------------
# DataFrame base
# ---------------------------------------------------------------------------
df = spark.table("vw_sales_orders")

# ---------------------------------------------------------------------------
# Funções auxiliares
# ---------------------------------------------------------------------------
def _nome_arquivo(org_vendas, sufixo):
    """Gera nome do arquivo: ex. 'HDA 2026 Jul Demanda Fechada'."""
    prefixo = PREFIXOS.get(org_vendas, "OUT")
    mes_abrev = MESES_PT.get(mes, "")
    return f"{prefixo} {ano} {mes_abrev} {sufixo}"


def _pivot(df_filtrado, colunas_grupo, operacao="soma"):
    """Aplica pivot por data (yyyy/MM) com soma ou contagem."""
    df_fmt = df_filtrado.withColumn("data_aaaa_mm", date_format("data", "yyyy/MM"))
    if operacao == "soma":
        df_pivot = df_fmt.groupBy(colunas_grupo).pivot("data_aaaa_mm").agg({"quantidade": "sum"})
    else:
        df_pivot = df_fmt.groupBy(colunas_grupo).pivot("data_aaaa_mm").agg({"quantidade": "count"})
    return df_pivot.fillna(0)


TABELAS = [
    "refined_demand_fechada_novos_modelos",
    "refined_demand_aberta",
    "refined_demand_linha",
    "refined_demand_mi",
    "refined_demand_me",
    "refined_demand_zpug",
    "refined_demand_zpug_cliente",
    "refined_demand_distribuicao",
]


def _limpar_tabelas():
    """Dropa todas as tabelas de saída para recriação limpa."""
    for t in TABELAS:
        spark.sql(f"DROP TABLE IF EXISTS {SCHEMA}.{t}")
        print(f"✖ {SCHEMA}.{t} removida")
    print("\n✓ Todas as tabelas limpas. Pronto para append.")


def _append(df_resultado, tabela):
    """Insere DataFrame em tabela Delta. Cria com column mapping se não existir."""
    full_name = f"{SCHEMA}.{tabela}"
    if not spark.catalog.tableExists(full_name):
        df_resultado.createOrReplaceTempView("_tmp_write")
        spark.sql(f"""
            CREATE TABLE {full_name}
            TBLPROPERTIES ('delta.columnMapping.mode' = 'name')
            AS SELECT * FROM _tmp_write
        """)
        print(f"✓ {full_name} criada ({df_resultado.count()} linhas)")
    else:
        df_resultado.write.mode("append").option("mergeSchema", "true").saveAsTable(full_name)
        print(f"✓ {full_name} append ({df_resultado.count()} linhas)")


def _union_abas(df_base, abas, arquivo, colunas_grupo, operacao="soma"):
    """Itera sobre abas, aplica pivot em cada e faz union."""
    resultados = []
    for aba in abas:
        df_aba = df_base if aba == "TTL" else df_base.filter(df_base.centro_original == aba)
        df_pivot = _pivot(df_aba, colunas_grupo, operacao)
        df_pivot = df_pivot.withColumn("file", lit(arquivo)).withColumn("sheet", lit(aba))
        resultados.append(df_pivot)
    return reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), resultados)


def _reordenar(df_resultado, cols_id):
    """Reordena: file, sheet, cols_id, datas..."""
    fixas = ["file", "sheet"] + cols_id
    datas = sorted([c for c in df_resultado.columns if c not in fixas])
    return df_resultado.select(fixas + datas)


print("Auxiliares carregadas.")

# COMMAND ----------

# DBTITLE 1,Limpar tabelas de saída
# Limpa todas as tabelas de saída antes de regravar HDA + HAB
_limpar_tabelas()

# COMMAND ----------

# DBTITLE 1,Segmento 2W HDA
# MAGIC %md
# MAGIC ## 🏍️ Segmento 2W – HDA (org_vendas = 0200)
# MAGIC Centros: `0203` (Sumaré), `0209` (Jaboatão), `0232` (Manaus)

# COMMAND ----------

# DBTITLE 1,HDA Demanda Fechada
# [2W HDA] Demanda Fechada: soma por item_principal_cadeia, todas as abas 2W
org_vendas = "0200"  # 2W Motos
arquivo = _nome_arquivo(org_vendas, "Demanda Fechada")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov != "ZESP"))

resultado = _union_abas(df_base, ABAS_2W, arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_fechada_novos_modelos")

# COMMAND ----------

# DBTITLE 1,HDA Demanda Aberta
# [2W HDA] Demanda Aberta: soma por material, todas as abas 2W
org_vendas = "0200"  # 2W Motos
arquivo = _nome_arquivo(org_vendas, "Demanda Aberta")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

resultado = _union_abas(df_base, ABAS_2W, arquivo, ["material"])
resultado = resultado.withColumnRenamed("material", "Material")
resultado = _reordenar(resultado, ["Material"])

_append(resultado, "refined_demand_aberta")

# COMMAND ----------

# DBTITLE 1,HDA Demanda Linha
# [2W HDA] Demanda Linha: contagem por item_principal_cadeia, todas as abas 2W
org_vendas = "0200"  # 2W Motos
arquivo = _nome_arquivo(org_vendas, "Demanda Linha")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

resultado = _union_abas(df_base, ABAS_2W, arquivo, ["item_principal_cadeia"], operacao="contagem")
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_linha")

# COMMAND ----------

# DBTITLE 1,HDA Demanda MI
# [2W HDA] Demanda MI (Mercado Interno): soma por item_principal_cadeia, canal_dist=01, abas 2W
org_vendas = "0200"  # 2W Motos
arquivo = _nome_arquivo(org_vendas, "Demanda MI")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.canal_dist == "01")
)

resultado = _union_abas(df_base, ABAS_2W, arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_mi")

# COMMAND ----------

# DBTITLE 1,HDA Demanda ME
# [2W HDA] Demanda ME (Mercado Externo): soma por item_principal_cadeia, canal_dist=02, aba TTL
org_vendas = "0200"  # 2W Motos
arquivo = _nome_arquivo(org_vendas, "Demanda ME")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.canal_dist == "02")
)

resultado = _union_abas(df_base, ["TTL"], arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_me")

# COMMAND ----------

# DBTITLE 1,HDA Pedido ZPUG
# [2W HDA] Pedido ZPUG: soma por item_principal_cadeia, tipo_ov=ZPUG, aba TTL
org_vendas = "0200"  # 2W Motos
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

resultado = _union_abas(df_base, ["TTL"], arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_zpug")

# COMMAND ----------

# DBTITLE 1,HDA Pedido ZPUG por Cliente
# [2W HDA] Pedido ZPUG/Cliente: soma por item_principal_cadeia + emissor_da_ordem, tipo_ov=ZPUG, aba TTL
org_vendas = "0200"  # 2W Motos
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG Cliente")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

resultado = _union_abas(df_base, ["TTL"], arquivo, ["emissor_da_ordem", "item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = resultado.withColumnRenamed("emissor_da_ordem", "Cliente")
resultado = _reordenar(resultado, ["Item Principal Cadeia", "Cliente"])

_append(resultado, "refined_demand_zpug_cliente")

# COMMAND ----------

# DBTITLE 1,Segmento 4W HAB
# MAGIC %md
# MAGIC ## 🚗 Segmento 4W – HAB (org_vendas = 0500)
# MAGIC Centros: `0503` (Sumaré), `0505` (Jaboatão)

# COMMAND ----------

# DBTITLE 1,HAB Demanda Fechada
# [4W HAB] Demanda Fechada: soma por item_principal_cadeia, todas as abas 4W
org_vendas = "0500"  # 4W Autos
arquivo = _nome_arquivo(org_vendas, "Demanda Fechada")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov != "ZESP"))

resultado = _union_abas(df_base, ABAS_4W, arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_fechada_novos_modelos")

# COMMAND ----------

# DBTITLE 1,HAB Demanda Aberta
# [4W HAB] Demanda Aberta: soma por material, todas as abas 4W
org_vendas = "0500"  # 4W Autos
arquivo = _nome_arquivo(org_vendas, "Demanda Aberta")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

resultado = _union_abas(df_base, ABAS_4W, arquivo, ["material"])
resultado = resultado.withColumnRenamed("material", "Material")
resultado = _reordenar(resultado, ["Material"])

_append(resultado, "refined_demand_aberta")

# COMMAND ----------

# DBTITLE 1,HAB Demanda Linha
# [4W HAB] Demanda Linha: contagem por item_principal_cadeia, todas as abas 4W
org_vendas = "0500"  # 4W Autos
arquivo = _nome_arquivo(org_vendas, "Demanda Linha")

df_base = df.filter((df.data >= data_minima) & (df.org_vendas == org_vendas))

resultado = _union_abas(df_base, ABAS_4W, arquivo, ["item_principal_cadeia"], operacao="contagem")
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_linha")

# COMMAND ----------

# DBTITLE 1,HAB Demanda MI
# [4W HAB] Demanda MI (Mercado Interno): soma por item_principal_cadeia, canal_dist=01, abas 4W
org_vendas = "0500"  # 4W Autos
arquivo = _nome_arquivo(org_vendas, "Demanda MI")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.canal_dist == "01")
)

resultado = _union_abas(df_base, ABAS_4W, arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_mi")

# COMMAND ----------

# DBTITLE 1,HAB Demanda ME
# [4W HAB] Demanda ME (Mercado Externo): soma por item_principal_cadeia, canal_dist=02, aba TTL
org_vendas = "0500"  # 4W Autos
arquivo = _nome_arquivo(org_vendas, "Demanda ME")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.canal_dist == "02")
)

resultado = _union_abas(df_base, ["TTL"], arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_me")

# COMMAND ----------

# DBTITLE 1,HAB Pedido ZPUG
# [4W HAB] Pedido ZPUG: soma por item_principal_cadeia, tipo_ov=ZPUG, aba TTL
org_vendas = "0500"  # 4W Autos
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

resultado = _union_abas(df_base, ["TTL"], arquivo, ["item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = _reordenar(resultado, ["Item Principal Cadeia"])

_append(resultado, "refined_demand_zpug")

# COMMAND ----------

# DBTITLE 1,HAB Pedido ZPUG por Cliente
# [4W HAB] Pedido ZPUG/Cliente: soma por item_principal_cadeia + emissor_da_ordem, tipo_ov=ZPUG, aba TTL
org_vendas = "0500"  # 4W Autos
arquivo = _nome_arquivo(org_vendas, "Pedido ZPUG Cliente")

df_base = df.filter(
    (df.data >= data_minima) & (df.org_vendas == org_vendas) & (df.tipo_ov == "ZPUG")
)

resultado = _union_abas(df_base, ["TTL"], arquivo, ["emissor_da_ordem", "item_principal_cadeia"])
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
resultado = resultado.withColumnRenamed("emissor_da_ordem", "Cliente")
resultado = _reordenar(resultado, ["Item Principal Cadeia", "Cliente"])

_append(resultado, "refined_demand_zpug_cliente")

# COMMAND ----------

# DBTITLE 1,Distribuição por Centro e Mercado (HDA + HAB)
from pyspark.sql.functions import sum as _sum, col, round as _round, when
from operator import add
from datetime import date
from dateutil.relativedelta import relativedelta

# ---------------------------------------------------------------------------
# Distribuição por Centro e Mercado – Unificada HDA + HAB
# Colunas: Centros HDA | % HDA | Centros HAB | % HAB | Mercado
# ---------------------------------------------------------------------------
ref_date = date(ano, mes, 1)
data_fim = ref_date + relativedelta(months=1) - relativedelta(days=1)
data_6m = (ref_date - relativedelta(months=5)).strftime("%Y-%m-%d")
data_12m = (ref_date - relativedelta(months=11)).strftime("%Y-%m-%d")
data_fim_str = data_fim.strftime("%Y-%m-%d")

print(f"Centro: {data_6m} a {data_fim_str} (6 meses)")
print(f"Mercado: {data_12m} a {data_fim_str} (12 meses)")


def _calc_centro(df_seg, periodo_inicio, periodo_fim):
    """Pivot de centro com % para um segmento."""
    df_f = df_seg.filter((df_seg.data >= periodo_inicio) & (df_seg.data <= periodo_fim))
    df_pivot = (
        df_f.groupBy("item_principal_cadeia")
        .pivot("centro_original")
        .agg(_sum("quantidade"))
        .fillna(0)
    )
    cols = sorted([c for c in df_pivot.columns if c != "item_principal_cadeia"])
    total = reduce(add, [col(c) for c in cols])
    df_pivot = df_pivot.withColumn("_total", total)
    for c in cols:
        df_pivot = df_pivot.withColumn(
            f"%_{c}",
            _round(when(col("_total") > 0, col(c) / col("_total") * 100).otherwise(0), 2)
        )
        df_pivot = df_pivot.withColumnRenamed(c, f"DMD_{c}")
    return df_pivot.drop("_total"), cols


def _calc_mercado(df_seg, periodo_inicio, periodo_fim):
    """Pivot de mercado (MI/ME) com %."""
    df_f = df_seg.filter((df_seg.data >= periodo_inicio) & (df_seg.data <= periodo_fim))
    df_pivot = (
        df_f.groupBy("item_principal_cadeia")
        .pivot("canal_dist")
        .agg(_sum("quantidade"))
        .fillna(0)
    )
    mercado_map = {"01": "MI", "02": "ME"}
    for old_name, new_name in mercado_map.items():
        if old_name in df_pivot.columns:
            df_pivot = df_pivot.withColumnRenamed(old_name, new_name)
    cols = sorted([c for c in df_pivot.columns if c != "item_principal_cadeia"])
    total = reduce(add, [col(c) for c in cols])
    df_pivot = df_pivot.withColumn("_total", total)
    for c in cols:
        df_pivot = df_pivot.withColumn(
            f"%_{c}",
            _round(when(col("_total") > 0, col(c) / col("_total") * 100).otherwise(0), 2)
        )
        df_pivot = df_pivot.withColumnRenamed(c, f"DMD_{c}")
    return df_pivot.drop("_total"), cols


# --- HDA (2W) ---
df_hda = df.filter(df.org_vendas == "0200")
df_centro_hda, centro_hda_cols = _calc_centro(df_hda, data_6m, data_fim_str)

# --- HAB (4W) ---
df_hab = df.filter(df.org_vendas == "0500")
df_centro_hab, centro_hab_cols = _calc_centro(df_hab, data_6m, data_fim_str)

# --- Mercado (ambos) ---
df_mercado_total, mercado_cols = _calc_mercado(df, data_12m, data_fim_str)

# --- JOIN ---
resultado = (
    df_centro_hda
    .join(df_centro_hab, "item_principal_cadeia", "full")
    .join(df_mercado_total, "item_principal_cadeia", "full")
    .fillna(0)
)
resultado = resultado.withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")

# --- Ordem: Centros HDA | % HDA | Centros HAB | % HAB | Mercado ---
dmd_hda = [f"DMD_{c}" for c in centro_hda_cols]
pct_hda = [f"%_{c}" for c in centro_hda_cols]
dmd_hab = [f"DMD_{c}" for c in centro_hab_cols]
pct_hab = [f"%_{c}" for c in centro_hab_cols]
dmd_mercado = [f"DMD_{c}" for c in mercado_cols]
pct_mercado = [f"%_{c}" for c in mercado_cols]

arquivo = f"HDA HAB {ano} {MESES_PT.get(mes, '')} Distribuição"
resultado = resultado.withColumn("file", lit(arquivo)).withColumn("sheet", lit("TTL"))

cols_final = ["file", "sheet", "Item Principal Cadeia"] + dmd_hda + pct_hda + dmd_hab + pct_hab + dmd_mercado + pct_mercado
resultado = resultado.select(cols_final)

_append(resultado, "refined_demand_distribuicao")

# COMMAND ----------

# DBTITLE 1,Scratch - Consultas ad-hoc (não executa automaticamente)
# =============================================================================
# CÉLULA DE SUPORTE – NÃO EXECUTAR NO RUN ALL
# Use para consultas ad-hoc com filtros diferentes.
# =============================================================================
# Descomente e ajuste conforme necessário:

# --- Exemplo 1: Filtrar por cliente específico ---
# display(
#     df.filter(df.emissor_da_ordem == 11010026)
#       .select("numero_ov", "data", "material", "quantidade", "razao_social", "estado")
#       .orderBy("data", ascending=False)
# )

# --- Exemplo 2: Filtrar por estado com pivot mensal (segmento + estado) ---
from pyspark.sql.functions import when as _when

#_df_estado = (
#    df.filter(df.estado == "TO")
#      .withColumn("segmento", _when(df.org_vendas == "0200", "HDA").otherwise("HAB"))
#)
#display(
#    _pivot(_df_estado, ["segmento", "item_principal_cadeia", "estado"])
#      .withColumnRenamed("segmento", "Segmento")
#      .withColumnRenamed("item_principal_cadeia", "Item Principal Cadeia")
#      .withColumnRenamed("estado", "Estado")
#      .orderBy("Segmento", "Item Principal Cadeia")
#)

# --- Exemplo 3: Filtrar por período customizado ---
# display(
#     df.filter((df.data >= "2026-06-01") & (df.data <= "2026-06-30") & (df.org_vendas == "0200"))
#       .groupBy("razao_social", "estado")
#       .agg({"quantidade": "sum"})
#       .orderBy("sum(quantidade)", ascending=False)
# )

# --- Exemplo 4: Consultar tabelas refined já gravadas ---
# display(spark.table("parts_hdbk_sandbox.pr_demand.refined_demand_zpug_cliente").limit(20))