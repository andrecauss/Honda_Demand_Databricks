# ==============================================================================
# MODULE: notebook_meta.py
# ==============================================================================
# Módulo compartilhado para metadata estruturado de notebooks.
# Importável por qualquer notebook do projeto Honda Demand Databricks.
#
# Uso:
#   import sys
#   sys.path.insert(0, "/Workspace/Users/andre_causs@honda.com.br/Honda_Demand_Databricks")
#   from utils.notebook_meta import exibir_metadata, validar_metadata
# ==============================================================================

from typing import Any


# ==============================================================================
# CONSTANTES
# ==============================================================================

# Chaves obrigatórias que todo NOTEBOOK_META deve conter
CHAVES_OBRIGATORIAS = {
    "notebook",
    "proposito",
    "inputs",
    "outputs",
    "autor",
    "atualizado",
}

# Chaves opcionais reconhecidas
CHAVES_OPCIONAIS = {
    "transformacoes",
    "dimensoes",
    "dependencias",
    "execucao",
    "convencoes",
    "notas",
}


# ==============================================================================
# VALIDAÇÃO
# ==============================================================================

def validar_metadata(meta: dict[str, Any]) -> list[str]:
    """
    Valida se o dicionário de metadata possui todas as chaves obrigatórias
    e não contém chaves desconhecidas.

    Args:
        meta: Dicionário NOTEBOOK_META a ser validado.

    Returns:
        Lista de mensagens de erro. Vazia se tudo estiver válido.

    Example:
        >>> erros = validar_metadata(NOTEBOOK_META)
        >>> if erros:
        ...     for e in erros:
        ...         print(f"⚠️ {e}")
        ... else:
        ...     print("✅ Metadata válido.")
    """
    erros: list[str] = []
    chaves_presentes = set(meta.keys())
    chaves_reconhecidas = CHAVES_OBRIGATORIAS | CHAVES_OPCIONAIS

    # Chaves ausentes
    faltantes = CHAVES_OBRIGATORIAS - chaves_presentes
    if faltantes:
        erros.append(f"Chaves obrigatórias ausentes: {', '.join(sorted(faltantes))}")

    # Chaves desconhecidas
    extras = chaves_presentes - chaves_reconhecidas
    if extras:
        erros.append(f"Chaves não reconhecidas: {', '.join(sorted(extras))}")

    # Validações de tipo
    for chave in ("inputs", "outputs"):
        if chave in meta and not isinstance(meta[chave], list):
            erros.append(f"'{chave}' deve ser uma lista, recebeu {type(meta[chave]).__name__}")

    if "dimensoes" in meta and not isinstance(meta["dimensoes"], dict):
        erros.append(f"'dimensoes' deve ser um dict, recebeu {type(meta['dimensoes']).__name__}")

    return erros


# ==============================================================================
# EXIBIÇÃO FORMATADA
# ==============================================================================

def exibir_metadata(meta: dict[str, Any], *, validar: bool = True) -> None:
    """
    Exibe o dicionário de metadata do notebook de forma legível no console.

    Opcionalmente valida o metadata antes de exibir. Se houver erros de
    validação, exibe os avisos antes do conteúdo.

    Args:
        meta: Dicionário NOTEBOOK_META com as chaves padrão.
        validar: Se True (default), executa validar_metadata() antes de exibir.

    Side Effects:
        Imprime o metadata formatado no console via print().

    Example:
        >>> NOTEBOOK_META = {
        ...     "notebook": "5.1 - Demand Refinement",
        ...     "proposito": "Agrega dados de demanda...",
        ...     "inputs": ["raw_sales_order"],
        ...     "outputs": ["refined_demand_*"],
        ...     "autor": "André Causs",
        ...     "atualizado": "2026-08-09",
        ... }
        >>> exibir_metadata(NOTEBOOK_META)
    """
    largura = 70
    sep = "=" * largura

    # --- Validação opcional ---
    if validar:
        erros = validar_metadata(meta)
        if erros:
            print(f"\n{'⚠' * 35}")
            print("  AVISOS DE METADATA:")
            for e in erros:
                print(f"    ⚠️ {e}")
            print(f"{'⚠' * 35}\n")

    # --- Cabeçalho ---
    print(f"\n{sep}")
    print(f"  📘 {meta.get('notebook', 'Sem nome')}")
    print(sep)

    # --- Propósito ---
    print(f"\n  PROPÓSITO:")
    print(f"    {meta.get('proposito', '—')}")

    # --- Fluxo visual INPUT → TRANSFORMAÇÃO → OUTPUT ---
    print(f"\n  ARQUITETURA:")
    print(f"    INPUT:  {', '.join(meta.get('inputs', []))}")

    if meta.get("transformacoes"):
        print(f"      │")
        print(f"      ▼")
        for t in meta["transformacoes"]:
            print(f"    • {t}")

    print(f"      │")
    print(f"      ▼")
    print(f"    OUTPUT: {', '.join(meta.get('outputs', []))}")

    # --- Dimensões ---
    if meta.get("dimensoes"):
        print(f"\n  DIMENSÕES DE ANÁLISE:")
        for chave, desc in meta["dimensoes"].items():
            print(f"    • {chave}: {desc}")

    # --- Convenções ---
    if meta.get("convencoes"):
        print(f"\n  CONVENÇÕES:")
        for conv in meta["convencoes"]:
            print(f"    • {conv}")

    # --- Dependências ---
    if meta.get("dependencias"):
        print(f"\n  DEPENDÊNCIAS:")
        for dep in meta["dependencias"]:
            print(f"    • {dep}")

    # --- Execução ---
    if meta.get("execucao"):
        print(f"\n  EXECUÇÃO:")
        print(f"    {meta['execucao']}")

    # --- Notas ---
    if meta.get("notas"):
        print(f"\n  NOTAS:")
        if isinstance(meta["notas"], list):
            for nota in meta["notas"]:
                print(f"    • {nota}")
        else:
            print(f"    {meta['notas']}")

    # --- Rodapé ---
    print(f"\n  AUTOR: {meta.get('autor', '—')}")
    print(f"  ATUALIZADO: {meta.get('atualizado', '—')}")
    print(f"{sep}\n")


# ==============================================================================
# ARQUITETO: Template para novos notebooks
# ==============================================================================

def criar_metadata_template(
    notebook: str = "",
    proposito: str = "",
    inputs: list[str] | None = None,
    outputs: list[str] | None = None,
    autor: str = "André Causs - Demand Planning - Honda Parts Division",
) -> dict[str, Any]:
    """
    Gera um dicionário NOTEBOOK_META pré-preenchido com valores padrão,
    pronto para ser customizado. Evita copiar/colar templates manualmente.

    Args:
        notebook: Nome do notebook (ex: "5.1 - Demand Refinement").
        proposito: Descrição do propósito do notebook.
        inputs: Lista de tabelas/fontes de entrada.
        outputs: Lista de tabelas/artefatos de saída.
        autor: Nome do autor (default: André Causs).

    Returns:
        Dicionário NOTEBOOK_META completo com todas as chaves.

    Example:
        >>> meta = criar_metadata_template(
        ...     notebook="6.1 - Export Demand to CSV",
        ...     proposito="Exporta tabelas refined para CSV no volume.",
        ...     inputs=["refined_demand_fechada_mi", "refined_demand_aberta_mi"],
        ...     outputs=["/Volumes/.../demand_fechada.csv"],
        ... )
        >>> exibir_metadata(meta)
    """
    from datetime import date

    return {
        "notebook": notebook,
        "proposito": proposito,
        "inputs": inputs or [],
        "outputs": outputs or [],
        "transformacoes": [],
        "dimensoes": {},
        "dependencias": [],
        "execucao": "Executar Run All.",
        "convencoes": [],
        "notas": [],
        "autor": autor,
        "atualizado": date.today().isoformat(),
    }


# ==============================================================================
# SERIALIZAÇÃO (para catálogo / linhagem futura)
# ==============================================================================

def metadata_to_dict(meta: dict[str, Any]) -> dict[str, Any]:
    """
    Retorna uma cópia normalizada do metadata, útil para serializar em JSON
    ou persistir como propriedade de tabela Unity Catalog.

    Args:
        meta: Dicionário NOTEBOOK_META.

    Returns:
        Cópia do dict com listas e dicts garantidos nos campos esperados.
    """
    copia = dict(meta)
    copia.setdefault("inputs", [])
    copia.setdefault("outputs", [])
    copia.setdefault("transformacoes", [])
    copia.setdefault("dimensoes", {})
    copia.setdefault("dependencias", [])
    return copia
