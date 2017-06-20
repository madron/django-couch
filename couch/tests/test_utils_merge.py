from django.test import SimpleTestCase
from ..utils import merge_schema


class UtilsTest(SimpleTestCase):
    def test_merge_schema(self):
        schema = dict(
            couchtest=dict(
                default=dict(
                    emptydb=dict(),
                    db=dict(
                        designs=dict(
                            testdesigndoc1=dict(
                                views=dict(
                                    view1=dict(
                                        map='// couchtest db doc1 view1 map',
                                        reduce='// couchtest db doc1 view1 reduce',
                                    ),
                                    view2=dict(
                                        map='// couchtest db doc1 view2 map',
                                    ),
                                ),
                                language='javascript',
                            ),
                            testdesigndoc2=dict(
                                views=dict(
                                    view1=dict(
                                        map='// couchtest db doc2 view1 map',
                                    ),
                                ),
                            ),
                        ),
                        index=dict(
                            testindexdoc=dict(
                                index1=dict(fields=['document_type']),
                            ),
                        ),
                    ),
                    anotherdb=dict(
                        designs=dict(
                            testdesigndoc=dict(
                                language='javascript',
                                views=dict(
                                    view=dict(
                                        map='// couchtest anotherdb doc view map',
                                    ),
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        expected_schema = dict(
            default=dict(
                emptydb=dict(),
                db=dict(
                    designs=dict(
                        couchtest_testdesigndoc1=dict(
                            views=dict(
                                view1=dict(
                                    map='// couchtest db doc1 view1 map',
                                    reduce='// couchtest db doc1 view1 reduce',
                                ),
                                view2=dict(
                                    map='// couchtest db doc1 view2 map',
                                ),
                            ),
                            language='javascript',
                        ),
                        couchtest_testdesigndoc2=dict(
                            views=dict(
                                view1=dict(
                                    map='// couchtest db doc2 view1 map',
                                ),
                            ),
                        ),
                    ),
                    index=dict(
                        couchtest_testindexdoc=dict(
                            index1=dict(fields=['document_type']),
                        ),
                    ),
                ),
                anotherdb=dict(
                    designs=dict(
                        couchtest_testdesigndoc=dict(
                            language='javascript',
                            views=dict(
                                view=dict(
                                    map='// couchtest anotherdb doc view map',
                                ),
                            ),
                        ),
                    ),
                ),
            ),
        )
        merged_schema = merge_schema(schema)
        self.assertEqual(merged_schema, expected_schema)

    def test_merge_schema_multiple_apps(self):
        schema = dict(
            couchtest1=dict(
                default=dict(
                    db=dict(
                        designs=dict(testdesigndoc=dict(views=dict(view1=dict()))),
                    ),
                ),
            ),
            couchtest2=dict(
                default=dict(
                    db=dict(
                        designs=dict(testdesigndoc=dict(views=dict(view1=dict()))),
                    ),
                ),
            ),
        )
        expected_schema = dict(
            default=dict(
                db=dict(
                    designs=dict(
                        couchtest1_testdesigndoc=dict(views=dict(view1=dict())),
                        couchtest2_testdesigndoc=dict(views=dict(view1=dict())),
                    ),
                ),
            ),
        )
        merged_schema = merge_schema(schema)
        self.assertEqual(merged_schema, expected_schema)

    def test_merge_schema_multiple_servers(self):
        schema = dict(
            app1=dict(
                server1=dict(
                    db1=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                    db2=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                ),
                server2=dict(
                    db1=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                    db2=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                ),
            ),
            app2=dict(
                server1=dict(
                    db1=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                    db2=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                ),
                server2=dict(
                    db1=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                    db2=dict(
                        designs=dict(
                            doc1=dict(views=dict(view1=dict())),
                            doc2=dict(views=dict(view1=dict())),
                        ),
                    ),
                ),
            ),
        )
        expected_schema = dict(
            server1=dict(
                db1=dict(
                    designs=dict(
                        app1_doc1=dict(views=dict(view1=dict())),
                        app1_doc2=dict(views=dict(view1=dict())),
                        app2_doc1=dict(views=dict(view1=dict())),
                        app2_doc2=dict(views=dict(view1=dict())),
                    ),
                ),
                db2=dict(
                    designs=dict(
                        app1_doc1=dict(views=dict(view1=dict())),
                        app1_doc2=dict(views=dict(view1=dict())),
                        app2_doc1=dict(views=dict(view1=dict())),
                        app2_doc2=dict(views=dict(view1=dict())),
                    ),
                ),
            ),
            server2=dict(
                db1=dict(
                    designs=dict(
                        app1_doc1=dict(views=dict(view1=dict())),
                        app1_doc2=dict(views=dict(view1=dict())),
                        app2_doc1=dict(views=dict(view1=dict())),
                        app2_doc2=dict(views=dict(view1=dict())),
                    ),
                ),
                db2=dict(
                    designs=dict(
                        app1_doc1=dict(views=dict(view1=dict())),
                        app1_doc2=dict(views=dict(view1=dict())),
                        app2_doc1=dict(views=dict(view1=dict())),
                        app2_doc2=dict(views=dict(view1=dict())),
                    ),

                ),
            ),
        )
        merged_schema = merge_schema(schema)
        self.assertEqual(merged_schema, expected_schema)
