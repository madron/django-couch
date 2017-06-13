def server_setup(sender, **kwargs):
    from django.conf import settings
    from . import Server
    for alias in settings.COUCH_SERVERS.keys():
        server = Server(alias=alias)
        server.single_node_setup()
