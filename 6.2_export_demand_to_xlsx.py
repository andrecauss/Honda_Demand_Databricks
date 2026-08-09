# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# dependencies = [
#   "openpyxl",
#   "xlsxwriter",
# ]
# ///
# DBTITLE 1,Export Demand Tables to CSV
# MAGIC %md
# MAGIC # Exportação de Tabelas de Demanda para Excel
# MAGIC
# MAGIC **Camada**: Export  
# MAGIC **Objetivo**: Exportar todas as tabelas refinadas de demanda do schema `pr_demand` como arquivos Excel (XLSX) para o volume de exportação.
# MAGIC
# MAGIC ## Tabelas Exportadas
# MAGIC
# MAGIC * Demandas Fechadas: Novos Modelos
# MAGIC * Demandas Gerais: Aberta e Linha (contagem de SKUs)
# MAGIC * Demandas por Mercado: MI (Mercado Interno) e ME (Mercado Externo)
# MAGIC * Pedidos ZPUG: Agregados e por Cliente
# MAGIC * Distribuição: HDA + HAB por Centro e Mercado
# MAGIC
# MAGIC ## Lógica de Exportação
# MAGIC
# MAGIC * Tabelas com colunas `file`/`sheet`: uma aba por sheet, agrupadas por file
# MAGIC * Tabelas sem essas colunas: um arquivo Excel simples por tabela
# MAGIC
# MAGIC ## Configuração
# MAGIC
# MAGIC **Schema Fonte**: `parts_hdbk_sandbox.pr_demand`  
# MAGIC **Volume Destino**: `/Volumes/parts_hdbk_sandbox/pr_demand/demand_refined_exportfiles`

# COMMAND ----------

# DBTITLE 1,List tables in schema
# ---------------------------------------------------------------------------
# Lista todas as tabelas do schema de demanda para exportação
# ---------------------------------------------------------------------------
tables = spark.sql("SHOW TABLES IN parts_hdbk_sandbox.pr_demand").collect()
table_names = [row.tableName for row in tables]

# Tabelas listadas

# COMMAND ----------

# DBTITLE 1,Install openpyxl library
# ---------------------------------------------------------------------------
# Instala bibliotecas necessárias para exportação em formato Excel
# - openpyxl: Leitura/escrita de arquivos .xlsx
# - xlsxwriter: Engine otimizado para pandas.to_excel()
# ---------------------------------------------------------------------------
%pip install openpyxl xlsxwriter

# COMMAND ----------

# DBTITLE 1,Funções Auxiliares de Exportação
# ---------------------------------------------------------------------------
# Funções auxiliares para exportação de tabelas para Excel
# ---------------------------------------------------------------------------
import pandas as pd
import tempfile
import os

# Caminho do volume de destino
VOLUME_PATH = "/Volumes/parts_hdbk_sandbox/pr_demand/demand_refined_exportfiles"

# Diretório temporário local para criação dos arquivos
LOCAL_TMP = tempfile.mkdtemp()

print(f"Volume de destino: {VOLUME_PATH}")
print(f"Diretorio temporario: {LOCAL_TMP}")


def export_table_to_excel(table_name, schema="parts_hdbk_sandbox.pr_demand"):
    """
    Exporta uma tabela do schema para Excel.
    
    Lógica:
    - Se a tabela possui colunas 'file' e 'sheet':
      -> Cria um arquivo Excel por valor de 'file'
      -> Cada valor de 'sheet' vira uma aba no arquivo
    - Caso contrário:
      -> Cria um arquivo Excel simples com o nome da tabela
    
    Args:
        table_name (str): Nome da tabela a ser exportada
        schema (str): Schema completo (catalog.schema)
    """
    try:
        full_table_name = f"{schema}.{table_name}"
        df = spark.table(full_table_name)
        
        # Cache schema check uma vez antes da conversão (otimização Spark Connect)
        columns_list = df.columns
        has_file_sheet = "file" in columns_list and "sheet" in columns_list
        
        # Converte para pandas (ação que dispara a execução Spark)
        pdf = df.toPandas()

        if has_file_sheet:
            # Tabelas com file/sheet: um workbook por 'file', uma aba por 'sheet'
            for file_name, file_group in pdf.groupby("file"):
                local_path = os.path.join(LOCAL_TMP, f"{file_name}.xlsx")
                with pd.ExcelWriter(local_path, engine='xlsxwriter') as writer:
                    for sheet_name, sheet_group in file_group.groupby("sheet"):
                        # Remove colunas de controle antes de exportar
                        data = sheet_group.drop(columns=["file", "sheet"])
                        # Excel permite no máximo 31 caracteres em nomes de aba
                        data.to_excel(writer, sheet_name=str(sheet_name)[:31], index=False)

                output_path = f"{VOLUME_PATH}/{file_name}.xlsx"
                dbutils.fs.cp(f"file:{local_path}", output_path)
                n_sheets = file_group["sheet"].nunique()
                print(f"[OK] {table_name} -> {file_name}.xlsx ({len(file_group)} linhas, {n_sheets} abas)")
        else:
            # Tabelas simples: um workbook, uma aba
            local_path = os.path.join(LOCAL_TMP, f"{table_name}.xlsx")
            pdf.to_excel(local_path, index=False, engine='xlsxwriter')

            output_path = f"{VOLUME_PATH}/{table_name}.xlsx"
            dbutils.fs.cp(f"file:{local_path}", output_path)
            print(f"[OK] {table_name}.xlsx: {len(pdf)} linhas")

    except Exception as e:
        print(f"[ERRO] Erro ao exportar {table_name}: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Limpar Volume de Exportação
# ---------------------------------------------------------------------------
# Limpa completamente o volume de exportação antes de iniciar
# ---------------------------------------------------------------------------
print("\n" + "="*70)
print("LIMPANDO VOLUME DE EXPORTACAO")
print("="*70)

try:
    # Lista todos os arquivos no volume
    files = dbutils.fs.ls(VOLUME_PATH)
    
    if len(files) == 0:
        print("Volume ja esta vazio.")
    else:
        print(f"\nEncontrados {len(files)} arquivos no volume.")
        print("Removendo todos os arquivos...\n")
        
        # Remove cada arquivo
        for file_info in files:
            file_path = file_info.path
            dbutils.fs.rm(file_path)
            print(f"[REMOVIDO] {file_info.name}")
        
        print(f"\n[OK] Volume limpo com sucesso! {len(files)} arquivos removidos.")
        
except Exception as e:
    print(f"[AVISO] Erro ao limpar volume: {str(e)}")
    print("Continuando com a exportacao...")

# COMMAND ----------

# DBTITLE 1,Bloco: Demandas Gerais
# ===========================================================================
# BLOCO 1: DEMANDAS FECHADAS - NOVOS MODELOS
# ===========================================================================
# Exporta tabelas de demanda fechada para novos modelos
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 1: DEMANDAS FECHADAS - NOVOS MODELOS")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Demanda Fechada
# ---------------------------------------------------------------------------
# [Demandas Fechadas - Novos Modelos] Demanda Fechada (Novos Modelos)
# Tabela: refined_demand_fechada_novos_modelos
# Conteúdo: Tabelas com colunas file/sheet organizadas por arquivo e aba
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_fechada_novos_modelos")

# COMMAND ----------

# DBTITLE 1,Bloco: Demandas Gerais
# ===========================================================================
# BLOCO 2: DEMANDAS GERAIS
# ===========================================================================
# Exporta tabelas de demanda aberta e linha (contagem de SKUs)
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 2: DEMANDAS GERAIS")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Demanda Aberta
# ---------------------------------------------------------------------------
# [Demandas Gerais] Demanda Aberta
# Tabela: refined_demand_aberta
# Conteúdo: Tabelas com colunas file/sheet organizadas por arquivo e aba
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_aberta")

# COMMAND ----------

# DBTITLE 1,Exportar Demanda Linha
# ---------------------------------------------------------------------------
# [Demandas Gerais] Demanda Linha (Contagem de SKUs)
# Tabela: refined_demand_linha
# Conteúdo: Tabelas com colunas file/sheet organizadas por arquivo e aba
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_linha")

# COMMAND ----------

# DBTITLE 1,Bloco: Demandas por Mercado
# ===========================================================================
# BLOCO 3: DEMANDAS POR MERCADO
# ===========================================================================
# Exporta tabelas segmentadas por Mercado Interno (MI) e Mercado Externo (ME)
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 3: DEMANDAS POR MERCADO")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Demanda MI
# ---------------------------------------------------------------------------
# [Demandas por Mercado] Demanda MI (Mercado Interno)
# Tabela: refined_demand_mi
# Filtro: canal_dist = '01'
# Conteúdo: Tabelas com colunas file/sheet organizadas por arquivo e aba
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_mi")

# COMMAND ----------

# DBTITLE 1,Exportar Demanda ME
# ---------------------------------------------------------------------------
# [Demandas por Mercado] Demanda ME (Mercado Externo)
# Tabela: refined_demand_me
# Filtro: canal_dist = '02'
# Conteúdo: Tabelas com colunas file/sheet organizadas por arquivo e aba
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_me")

# COMMAND ----------

# DBTITLE 1,Bloco: Pedidos ZPUG
# ===========================================================================
# BLOCO 4: PEDIDOS ZPUG
# ===========================================================================
# Exporta pedidos do tipo ZPUG (tipo_ov = 'ZPUG'), agregados e por cliente
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 4: PEDIDOS ZPUG")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Pedidos ZPUG
# ---------------------------------------------------------------------------
# [Pedidos ZPUG] Pedidos ZPUG Agregados
# Tabela: refined_demand_zpug
# Filtro: tipo_ov = 'ZPUG'
# Conteúdo: Tabelas com colunas file/sheet organizadas por arquivo e aba
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_zpug")

# COMMAND ----------

# DBTITLE 1,Exportar Pedidos ZPUG por Cliente
# ---------------------------------------------------------------------------
# [Pedidos ZPUG] Pedidos ZPUG por Cliente
# Tabela: refined_demand_zpug_cliente
# Filtro: tipo_ov = 'ZPUG'
# Agrupamento: item_principal_cadeia + emissor_da_ordem (Cliente)
# Conteúdo: Tabelas com colunas file/sheet organizadas por arquivo e aba
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_zpug_cliente")

# COMMAND ----------

# DBTITLE 1,Bloco: Distribuição
# ===========================================================================
# BLOCO 5: DISTRIBUICAO POR CENTRO E MERCADO
# ===========================================================================
# Exporta tabela de distribuicao unificada HDA + HAB com percentuais por
# centro e mercado
# ===========================================================================

print("\n" + "="*70)
print("BLOCO 5: DISTRIBUICAO POR CENTRO E MERCADO")
print("="*70)

# COMMAND ----------

# DBTITLE 1,Exportar Distribuição
# ---------------------------------------------------------------------------
# [Distribuição] Distribuição por Centro e Mercado (HDA + HAB)
# Tabela: refined_demand_distribuicao
# Conteúdo:
#   - Colunas de centros HDA (0203, 0209, 0232) com percentuais
#   - Colunas de centros HAB (0503, 0505) com percentuais
#   - Colunas de mercado (MI/ME) com percentuais
# ---------------------------------------------------------------------------
export_table_to_excel("refined_demand_distribuicao")

# COMMAND ----------

# DBTITLE 1,Resumo da Exportação
# ---------------------------------------------------------------------------
# Resumo final da exportação
# ---------------------------------------------------------------------------
print("\n" + "="*70)
print("EXPORTACAO CONCLUIDA COM SUCESSO!")
print("="*70)
print(f"\nArquivos salvos em: {VOLUME_PATH}")
print(f"Total de tabelas exportadas: {len(table_names)}")