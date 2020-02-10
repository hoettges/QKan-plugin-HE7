# -*- coding: utf-8 -*-

"""

  QGis-Plugin
  ===========

  Definition der Formularklasse

  | Dateiname            : application.py
  | Date                 : October 2016
  | Copyright            : (C) 2016 by Joerg Hoettges
  | Email                : hoettges@fh-aachen.de
  | git sha              : $Format:%H$

  This program is free software; you can redistribute it and/or modify  
  it under the terms of the GNU General Public License as published by  
  the Free Software Foundation; either version 2 of the License, or     
  (at your option) any later version.                                  

"""
import logging
import os

from qgis.core import QgsCoordinateReferenceSystem, QgsProject
from qgis.PyQt.QtCore import QCoreApplication, QSettings, QTranslator, qVersion
from qgis.PyQt.QtWidgets import QFileDialog
from qkan import QKan, enums
from qkan.database.qkan_utils import fehlermeldung, get_database_QKan

# noinspection PyUnresolvedReferences
from . import resources
from .application_dialog import ImportFromHEDialog, ResultsFromHEDialog
from .import_from_he import importKanaldaten
from .results_from_he import importResults

# noinspection PyUnresolvedReferences

logger = logging.getLogger("QKan.importhe.application")


class ImportFromHE:
    """QGIS Plugin Implementation."""

    def __init__(self, iface):
        """Constructor.

        :param iface: An interface instance that will be passed to this class
            which provides the hook by which you can manipulate the QGIS
            application at run time.
        :type iface: QgsInterface
        """
        # Save reference to the QGIS interface
        self.iface = iface
        # initialize plugin directory
        self.plugin_dir = os.path.dirname(__file__)
        # initialize locale
        locale = QSettings().value("locale/userLocale")[0:2]
        locale_path = os.path.join(
            self.plugin_dir, "i18n", "ImportFromHE_{}.qm".format(locale)
        )

        if os.path.exists(locale_path):
            self.translator = QTranslator()
            self.translator.load(locale_path)

            if qVersion() > "4.3.3":
                QCoreApplication.installTranslator(self.translator)

        # Create the dialog (after translation) and keep reference
        self.dlg_he = ImportFromHEDialog()
        self.dlg_lz = ResultsFromHEDialog()

        # Anfang Eigene Funktionen -------------------------------------------------
        # (jh, 09.10.2016)

        # Standard für Suchverzeichnis festlegen
        project = QgsProject.instance()
        self.default_dir = os.path.dirname(project.fileName())

        # Formularereignisse run_import()
        self.dlg_he.pb_selectqkanDB.clicked.connect(self.selectFile_qkanDBHE)
        self.dlg_he.pb_selectHeDB.clicked.connect(self.selectFile_HeDB)
        self.dlg_he.pb_selectProjectFile.clicked.connect(self.selectProjectFile)

        # Formularereignisse run_results()

        self.dlg_lz.pb_selectqmlfile.clicked.connect(self.selectqmlfileResults)

        # Klick auf eine Option zum Layerstil aktiviert/deaktiviert das Textfeld und die Schaltfläche
        self.dlg_lz.pb_selectHeDB.clicked.connect(self.selectFile_HeErgDB)
        self.dlg_lz.rb_userqml.clicked.connect(self.enable_tf_qmlfile)
        self.dlg_lz.rb_uebh.clicked.connect(self.disable_tf_qmlfile)
        self.dlg_lz.rb_uebvol.clicked.connect(self.disable_tf_qmlfile)
        self.dlg_lz.rb_none.clicked.connect(self.disable_tf_qmlfile)

    # noinspection PyMethodMayBeStatic
    def tr(self, message):
        """Get the translation for a string using Qt translation API.

        We implement this ourselves since we do not inherit QObject.

        :param message: String for translation.
        :type message: str, QString

        :returns: Translated version of message.
        :rtype: QString
        """
        # noinspection PyTypeChecker,PyArgumentList,PyCallByClass
        return QCoreApplication.translate("ImportFromHE", message)

    def initGui(self):
        """Create the menu entries and toolbar icons inside the QGIS GUI."""

        icon_import_path = ":/plugins/qkan/importhe/res/icon_import.png"
        QKan.instance.add_action(
            icon_import_path,
            text=self.tr("Import aus Hystem-Extran"),
            callback=self.run_import,
            parent=self.iface.mainWindow(),
        )

        icon_results_path = ":/plugins/qkan/importhe/res/icon_results.png"
        QKan.instance.add_action(
            icon_results_path,
            text=self.tr("Ergebnisse aus Hystem-Extran einlesen"),
            callback=self.run_results,
            parent=self.iface.mainWindow(),
        )

    def unload(self):
        pass

    # Formularfunktionen zum HE-Import -------------------------------------------------

    def selectFile_HeDB(self):
        """Datenbankverbindung zur HE-Datenbank (Firebird) auswaehlen"""

        filename, __ = QFileDialog.getOpenFileName(
            self.dlg_he,
            "Dateinamen der zu lesenden HE-Datenbank auswählen",
            self.default_dir,
            "*.idbf",
        )
        if os.path.dirname(filename) != "":
            os.chdir(os.path.dirname(filename))
        self.dlg_he.tf_heDB.setText(filename)

    def selectFile_qkanDBHE(self):
        """Datenbankverbindung zur QKan-Datenbank (SpatiaLite) auswaehlen"""

        filename, __ = QFileDialog.getSaveFileName(
            self.dlg_he,
            "Dateinamen der zu erstellenden SpatiaLite-Datenbank eingeben",
            self.default_dir,
            "*.sqlite",
        )
        if os.path.dirname(filename) != "":
            os.chdir(os.path.dirname(filename))
        self.dlg_he.tf_qkanDB.setText(filename)

    def selectProjectFile(self):
        """Zu erzeugende Projektdatei festlegen, falls ausgewählt."""

        filename, __ = QFileDialog.getSaveFileName(
            self.dlg_he,
            "Dateinamen der zu erstellenden Projektdatei eingeben",
            self.default_dir,
            "*.qgs",
        )
        if os.path.dirname(filename) != "":
            os.chdir(os.path.dirname(filename))
        self.dlg_he.tf_projectFile.setText(filename)

    def selectFile_HELZ(self):
        """Datenbankverbindung zur HE-Datenbank (Firebird) auswaehlen"""

        filename, __, = QFileDialog.getOpenFileName(
            self.dlg_lz,
            "Dateinamen der HE-Datenbank mit den LZ-Ergebnissen auswählen",
            self.default_dir,
            "*.idbf",
        )
        if os.path.dirname(filename) != "":
            os.chdir(os.path.dirname(filename))
        self.dlg_lz.tf_heDB.setText(filename)

        # Ende Eigene Funktionen ---------------------------------------------------

    def run_import(self):
        """Öffnen des Formulars zum Import aus HE"""

        self.dlg_he.tf_qkanDB.setText(QKan.config.database.qkan)
        self.dlg_he.tf_heDB.setText(QKan.config.he.database)
        self.dlg_he.qsw_epsg.setCrs(
            QgsCoordinateReferenceSystem.fromEpsgId(QKan.config.epsg)
        )
        self.dlg_he.tf_projectFile.setText(QKan.config.project.file)

        # Ende Eigene Funktionen ---------------------------------------------------

        # show the dialog
        self.dlg_he.show()
        # Run the dialog event loop
        result = self.dlg_he.exec_()
        # See if OK was pressed
        if result:
            # Do something useful here - delete the line containing pass and
            # substitute with your code.
            # pass

            # (jh, 09.10.2016)

            # Namen der Datenbanken uebernehmen

            database_HE = self.dlg_he.tf_heDB.text()
            database_QKan = self.dlg_he.tf_qkanDB.text()
            projectfile = self.dlg_he.tf_projectFile.text()
            self.epsg = self.dlg_he.qsw_epsg.crs().postgisSrid()

            # Konfigurationsdaten schreiben
            QKan.config.database.qkan = database_QKan
            QKan.config.epsg = self.epsg
            QKan.config.he.database = database_HE
            QKan.config.project.file = projectfile

            QKan.config.save()

            # Start der Verarbeitung

            # Modulaufruf in Logdatei schreiben
            logger.debug(f"""QKan-Modul Aufruf
                importKanaldaten(
                    "{database_HE}", 
                    "{database_QKan}", 
                    "{projectfile}", 
                    {self.epsg},
                )""")

            importKanaldaten(
                database_HE, 
                database_QKan, 
                projectfile, 
                self.epsg)

    # Formularfunktionen -------------------------------------------------------

    def selectFile_HeErgDB(self):
        """Datenbankverbindung zur HE-Ergebnisdatenbank (Firebird) auswaehlen"""

        filename, _ = QFileDialog.getOpenFileName(
            self.dlg_lz,
            "Dateinamen der zu lesenden HE-Ergebnisdatenbank auswählen",
            self.default_dir,
            "*.idbf",
        )
        if os.path.dirname(filename) != "":
            os.chdir(os.path.dirname(filename))
        self.dlg_lz.tf_heDB.setText(filename)

    def enable_tf_qmlfile(self):
        """aktiviert das Textfeld für die qml-Stildatei"""
        self.dlg_lz.tf_qmlfile.setEnabled(True)
        self.dlg_lz.pb_selectqmlfile.setEnabled(True)

    def disable_tf_qmlfile(self):
        """deaktiviert das Textfeld für die qml-Stildatei"""
        self.dlg_lz.tf_qmlfile.setEnabled(False)
        self.dlg_lz.pb_selectqmlfile.setEnabled(False)

    def selectqmlfileResults(self):
        """qml-Stildatei auswählen"""

        filename, __ = QFileDialog.getOpenFileName(
            self.dlg_lz,
            "Dateinamen der einzulesenen Stildatei auswählen",
            self.default_dir,
            "*.qml",
        )
        if os.path.dirname(filename) != "":
            os.chdir(os.path.dirname(filename))
        self.dlg_lz.tf_qmlfile.setText(filename)

    # Ende Formularfunktionen --------------------------------------------------

    def run_results(self):
        """Öffnen des Formulars zum Einlesen der Simulationsergebnisse aus HE"""

        database_QKan, epsg = get_database_QKan()

        # Auswahl der HE-Ergebnisdatenbank zum Laden
        database_ErgHE = QKan.config.he.database_erg
        self.dlg_lz.tf_heDB.setText(database_ErgHE)

        # Option für Stildatei
        qml_choice = QKan.config.he.qml_choice

        # Standard: User-qml-File ist deaktiviert
        self.disable_tf_qmlfile()

        if qml_choice == enums.QmlChoice.UEBH:
            self.dlg_lz.rb_uebh.setChecked(True)
        elif qml_choice == enums.QmlChoice.UEBVOL:
            self.dlg_lz.rb_uebvol.setChecked(True)
        elif qml_choice == enums.QmlChoice.USERQML:
            self.dlg_lz.rb_userqml.setChecked(True)
            # User-qml-File ist aktivieren
            self.enable_tf_qmlfile()
        elif qml_choice == enums.QmlChoice.NONE:
            self.dlg_lz.rb_none.setChecked(True)
        else:
            fehlermeldung("Fehler im Programmcode (1)", "Nicht definierte Option")
            return False

        # Individuelle Stildatei
        qml_file_results = QKan.config.he.qml_file_results
        self.dlg_lz.tf_qmlfile.setText(qml_file_results)

        # show the dialog
        self.dlg_lz.show()
        # Run the dialog event loop
        result = self.dlg_lz.exec_()
        # See if OK was pressed
        if result:

            # Daten aus Formular übernehmen
            database_ErgHE = self.dlg_lz.tf_heDB.text()
            qml_file_results = self.dlg_lz.tf_qmlfile.text()

            if self.dlg_lz.rb_uebh.isChecked():
                qml_choice = enums.QmlChoice.UEBH
            elif self.dlg_lz.rb_uebvol.isChecked():
                qml_choice = enums.QmlChoice.UEBVOL
            elif self.dlg_lz.rb_userqml.isChecked():
                qml_choice = enums.QmlChoice.USERQML
            elif self.dlg_lz.rb_none.isChecked():
                qml_choice = enums.QmlChoice.NONE
            else:
                fehlermeldung("Fehler im Programmcode (2)", "Nicht definierte Option")
                return False
            # Konfigurationsdaten schreiben

            QKan.config.database.qkan = database_QKan
            QKan.config.he.database_erg = database_ErgHE
            QKan.config.he.qml_choice = qml_choice
            QKan.config.he.qml_file_results = qml_file_results

            QKan.config.save()

            # Start der Verarbeitung

            # Modulaufruf in Logdatei schreiben
            logger.debug(f"""QKan-Modul Aufruf
                importResults(
                    "{database_ErgHE}", 
                    "{database_QKan}", 
                    {qml_choice}, 
                    {qml_file_results}, 
                    {epsg},
            )""")

            importResults(
                database_ErgHE, 
                database_QKan, 
                qml_choice, 
                qml_file_results, 
                epsg
            )
