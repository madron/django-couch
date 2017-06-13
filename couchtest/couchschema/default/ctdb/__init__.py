designs = dict(
    testdesigndoc=dict(
        views=dict(
            view1=dict(
                map='view1_map.js',
                reduce='view1_reduce.js',
            ),
            view2=dict(
                map='view2_map.js',
            ),
        ),
        language='javascript',
    ),
    testdesigndoc2=dict(views=dict(view1=dict(map='view.js'))),
)

index = dict(
    testindexdoc=dict(
        index1=dict(fields=['document_type']))
)
