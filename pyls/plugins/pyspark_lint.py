# Copyright 2017 Palantir Technologies, Inc.
import ast
import logging
from pyls import hookimpl, lsp

log = logging.getLogger(__name__)

@hookimpl
def pyls_settings():
    # Default pydocstyle to disabled
    return {'plugins': {'pyspark': {'enabled': False}}}

@hookimpl
def pyls_lint(config, document):
    config.plugin_settings('pyspark')
    log.debug("Running pyspark lint.")

    try:
        tree = ast.parse(document.source, document.path)
    except SyntaxError:
        return None

    ast.fix_missing_locations(tree)
    diags = []
    spark_methods_require_shuffle = [
        'repartition',
        'coalesce',
        'cogroup',
        'sortByKey',
        'aggregateByKey',
        'reduceByKey',
        'groupByKey',
        'join',
    ]
    for node in ast.walk(tree):
        if isinstance(node, ast.Attribute):
            if hasattr(node, 'attr') and node.attr in spark_methods_require_shuffle:
                diags.append({
                    'source': 'pyspark',
                    'range': {
                        'start': {'line': node.lineno - 1, 'character': node.col_offset},
                        'end': {'line': node.lineno - 1, 'character': node.col_offset + 14},
                    },
                    'message': 'Repartition is expensive.',
                    'severity': lsp.DiagnosticSeverity.Warning
                })

    return diags
