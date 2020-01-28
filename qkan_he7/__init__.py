# -*- coding: utf-8 -*-
import logging

import qgis
from qgis.utils import unloadPlugin

# Aufsetzen des Logging-Systems
logger = logging.getLogger("QKan.qkan_he7.init")


def classFactory(iface: qgis.gui.QgisInterface):  # pylint: disable=invalid-name
    try:
        from qkan import QKan as MainQKan

        if not MainQKan.instance:  # QKan isn't loaded
            raise Exception(
                "The QKan main plugin has to be loaded before loading this extension."
            )

        qkan = QKan(iface, MainQKan.instance)
        return qkan
    except ImportError:
        import traceback

        traceback.print_exc()
        unloadPlugin(__name__)
        raise Exception(
            "The QKan main plugin has to be installed for this extension to work."
        )


class QKan:
    instance = None
    name = __name__

    def __init__(self, iface: qgis.gui.QgisInterface, main):
        # noinspection PyUnresolvedReferences
        self.main: "MainQKan.QKan" = main
        self.actions = []

        QKan.config = main.config

        from .importhe import ImportFromHE
        from .exporthe import ExportToHE
        from .ganglinienhe import GanglinienHE

        self.plugins = [ImportFromHE(iface), ExportToHE(iface), GanglinienHE(iface)]
        QKan.instance = self

        # Register self
        self.main.register(self)

    def initGui(self):
        # Calls initGui on all known QKan plugins
        for plugin in self.plugins:
            plugin.initGui()

        self.main.sort_actions()

    def unload(self):
        # Call unload on all loaded plugins
        for plugin in self.plugins:
            plugin.unload()

        # Unload in main instance
        # Remove entries from menu
        for action in self.actions:
            self.main.menu.removeAction(action)
            self.main.toolbar.removeAction(action)

    def add_action(
        self,
        icon_path,
        text,
        callback,
        enabled_flag=True,
        add_to_menu=True,
        add_to_toolbar=True,
        status_tip=None,
        whats_this=None,
        parent=None,
    ):
        action = self.main.add_action(
            icon_path,
            text,
            callback,
            enabled_flag,
            add_to_menu,
            add_to_toolbar,
            status_tip,
            whats_this,
            parent,
        )
        self.actions.append(action)
        return action
