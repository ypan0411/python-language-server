# Copyright 2017 Palantir Technologies, Inc.
from pyls import lsp, uris
from pyls.workspace import Document
from pyls.plugins import pyspark_lint

DOC_URI = uris.from_fs_path(__file__)
DOC = """from pyspark import SparkContext

sc = SparkContext("local", "App Name", pyFiles=['MyFile.py', 'lib.zip', 'app.egg'])
rdd = sc.parallelize([1, 2, 3, 4, 5, 6, 7], 4)
rdd.repartition(2)

"""


def test_pyspark(config):
    doc = Document(DOC_URI, DOC)
    diags = pyspark_lint.pyls_lint(config, doc)

    warnings = [d for d in diags if d['source'] == 'pyspark']

    assert len(warnings) == 1
    assert warnings[0]['severity'] == lsp.DiagnosticSeverity.Warning
    assert warnings[0]['message'] == 'Repartition is expensive.'
