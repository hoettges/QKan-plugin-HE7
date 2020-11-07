# -*- coding: utf-8 -*-

"""

  Import from HE
  ==============

  Aus einer Hystem-Extran-Datenbank im Firebird-Format werden Kanaldaten
  in die QKan-Datenbank importiert. Dazu wird eine Projektdatei erstellt,
  die verschiedene thematische Layer erzeugt, u.a. eine Klassifizierung
  der Schachttypen.

  | Dateiname            : import_from_he.py
  | Date                 : September 2016
  | Copyright            : (C) 2016 by Joerg Hoettges
  | Email                : hoettges@fh-aachen.de
  | git sha              : $Format:%H$

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; either version 2 of the License, or
  (at your option) any later version.

"""

__author__ = "Joerg Hoettges"
__date__ = "September 2016"
__copyright__ = "(C) 2016, Joerg Hoettges"

import logging
import os
import xml.etree.ElementTree as ET

from pathlib import Path
from qgis.core import QgsCoordinateReferenceSystem, QgsProject
from qgis.PyQt.QtCore import QFileInfo
from qgis.utils import pluginDirectory
from qkan.database.dbfunc import DBConnection
from qkan.database.fbfunc import FBConnection
from qkan.database.qkan_utils import eval_node_types, fehlermeldung
# from qkan import QKan, enums
from qkan.tools.k_qgsadapt import qgsadapt

logger = logging.getLogger("QKan.importhe.import_from_he")


# ------------------------------------------------------------------------------
# Hauptprogramm


def importKanaldaten(
    database_HE, database_QKan, projectfile, epsg
):
    """Import der Kanaldaten aus einer HE-Firebird-Datenbank und Schreiben in eine QKan-SpatiaLite-Datenbank.

    :database_HE:   Datenbankobjekt, das die Verknüpfung zur HE-Firebird-Datenbank verwaltet
    :type database: DBConnection (geerbt von firebirdsql...)

    :database_QKan: Datenbankobjekt, das die Verknüpfung zur QKan-SpatiaLite-Datenbank verwaltet.
    :type database: DBConnection (geerbt von dbapi...)

    :returns: void
    """

    # ------------------------------------------------------------------------------
    # Datenbankverbindungen

    dbHE = FBConnection(database_HE)  # Datenbankobjekt der HE-Datenbank zum Lesen

    if dbHE is None:
        fehlermeldung(
            "Fehler in QKan_Import_from_HE",
            "ITWH-Datenbank {:s} wurde nicht gefunden!\nAbbruch!".format(database_HE),
        )
        return False

    dbQK = DBConnection(
        dbname=database_QKan, epsg=epsg
    )  # Datenbankobjekt der QKan-Datenbank zum Schreiben
    if not dbQK.connected:
        return False

    if dbQK is None:
        fehlermeldung(
            "Fehler in QKan_Import_from_HE",
            "QKan-Datenbank {:s} wurde nicht gefunden!\nAbbruch!".format(
                database_QKan
            ),
        )
        return False

    # Referenztabellen laden.

    # Entwässerungssystem. Attribut [bezeichnung] enthält die Bezeichnung des Benutzers.
    ref_entwart = {}
    sql = "SELECT he_nr, bezeichnung FROM entwaesserungsarten"
    if not dbQK.sql(sql, "importkanaldaten_he (1)"):
        del dbQK
        del dbHE
        return False
    daten = dbQK.fetchall()
    for el in daten:
        ref_entwart[el[0]] = el[1]

    # Pumpentypen. Attribut [bezeichnung] enthält die Bezeichnung des Benutzers.
    ref_pumpentyp = {}
    sql = "SELECT he_nr, bezeichnung FROM pumpentypen"
    if not dbQK.sql(sql, "importkanaldaten_he (2)"):
        del dbQK
        del dbHE
        return False
    daten = dbQK.fetchall()
    for el in daten:
        ref_pumpentyp[el[0]] = el[1]

    # Profile. Attribut [profilnam] enthält die Bezeichnung des Benutzers. Dies kann auch ein Kürzel sein.
    ref_profil = {}
    sql = "SELECT he_nr, profilnam FROM profile"
    if not dbQK.sql(sql, "importkanaldaten_he (3)"):
        del dbQK
        del dbHE
        return False
    daten = dbQK.fetchall()
    for el in daten:
        ref_profil[el[0]] = el[1]

    # Auslasstypen.
    ref_auslasstypen = {}
    sql = "SELECT he_nr, bezeichnung FROM auslasstypen"
    if not dbQK.sql(sql, "importkanaldaten_he (4)"):
        del dbQK
        del dbHE
        return False
    daten = dbQK.fetchall()
    for el in daten:
        ref_auslasstypen[el[0]] = el[1]

    # Simulationsstatus
    ref_simulationsstatus = {}
    sql = "SELECT he_nr, bezeichnung FROM simulationsstatus"
    if not dbQK.sql(sql, "importkanaldaten_he (5)"):
        del dbQK
        del dbHE
        return False
    daten = dbQK.fetchall()
    for el in daten:
        ref_simulationsstatus[el[0]] = el[1]

    # ------------------------------------------------------------------------------
    # Schachtdaten
    # Das Feld [KANALART] enthält das Entwasserungssystem (Schmutz-, Regen- oder Mischwasser)
    # Das Feld [ART] enthält die Information, ob es sich um einen Startknoten oder einen Inneren Knoten handelt.
    # oder: um was für eine #Verzweigung es sich handelt (Wunsch von Herrn Wippermann)...???

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM schaechte'
    # if not dbQK.sql(sql, u'importkanaldaten_he (11)'):
    # return None

    # Daten aus ITWH-Datenbank abfragen
    sql = """
    SELECT 
        NAME AS schnam,
        XKOORDINATE AS xsch, 
        YKOORDINATE AS ysch, 
        SOHLHOEHE AS sohlhoehe, 
        DECKELHOEHE AS deckelhoehe, 
        DURCHMESSER AS durchm, 
        DRUCKDICHTERDECKEL AS druckdicht, 
        KANALART AS entwaesserungsart_he, 
        PLANUNGSSTATUS AS simstat_he, 
        KOMMENTAR AS kommentar, 
        LASTMODIFIED AS createdat
        FROM SCHACHT"""

    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Schachtdaten aufbereiten und in die QKan-DB schreiben

    for attr in daten:
        (
            schnam,
            xsch,
            ysch,
            sohlhoehe,
            deckelhoehe,
            durchm,
            druckdicht,
            entwaesserungsart_he,
            simstat_he,
            kommentar,
            createdat,
        ) = ["NULL" if el is None else el for el in attr]

        # (schnam, kommentar) = [tt.decode('iso-8859-1') for tt in (schnam_ansi, kommentar_ansi)]

        # Entwasserungsarten
        if entwaesserungsart_he in ref_entwart:
            entwart = ref_entwart[entwaesserungsart_he]
        else:
            # Noch nicht in Tabelle [entwaesserungsarten] enthalten, also ergänzen
            sql = "INSERT INTO entwaesserungsarten (bezeichnung, he_nr) Values ('({0:})', {0:d})".format(
                entwaesserungsart_he
            )
            entwart = "({:})".format(entwaesserungsart_he)
            if not dbQK.sql(sql, "importkanaldaten_he (12)"):
                del dbQK
                del dbHE
                return False

        # Simstatus-Nr aus HE ersetzten
        if simstat_he in ref_simulationsstatus:
            simstatus = ref_simulationsstatus[simstat_he]
        else:
            # Noch nicht in Tabelle [simulationsstatus] enthalten, also ergqenzen
            simstatus = "({}_he)".format(simstat_he)
            sql = "INSERT INTO simulationsstatus (bezeichnung, he_nr) Values ('{simstatus}', {he_nr})".format(
                simstatus=simstatus, he_nr=simstat_he
            )
            ref_simulationsstatus[simstat_he] = simstatus
            if not dbQK.sql(sql, "importkanaldaten_he (13)"):
                del dbQK
                del dbHE
                return False

        # Geo-Objekte erzeugen

        # if QKan.config.database.type == enums.QKanDBChoice.SPATIALITE:
        #     geop = "MakePoint({0:},{1:},{2:})".format(xsch, ysch, epsg)
        #     geom = "CastToMultiPolygon(MakePolygon(MakeCircle({0:},{1:},{2:},{3:})))".format(
        #         xsch, ysch, (1.0 if durchm == "NULL" else durchm / 1000.0), epsg
        #     )
        # elif QKan.config.database.type == enums.QKanDBChoice.POSTGIS:
        #     geop = "ST_SetSRID(ST_MakePoint({0:},{1:}),{2:})".format(xsch, ysch, epsg)
        # else:
        #     fehlermeldung(
        #         "Programmfehler!",
        #         "Datenbanktyp ist fehlerhaft: {}!\nAbbruch!".format(
        #             QKan.config.database.type
        #         ),
        #     )

        # Datensatz in die QKan-DB schreiben

        try:
            sql = f"""INSERT INTO schaechte_data (schnam, xsch, ysch, 
                        sohlhoehe, deckelhoehe, durchm, druckdicht, entwart, 
                        schachttyp, simstatus, kommentar, createdat)
            VALUES ('{schnam}', {xsch}, {ysch}, {sohlhoehe}, {deckelhoehe}, {durchm}/1000, {druckdicht}, '{entwart}', 
                     'Schacht', '{simstatus}', '{kommentar}', '{createdat}')"""
            if not dbQK.sql(sql, "importkanaldaten_he (14)"):
                del dbQK
                del dbHE
                return False
        except BaseException as err:
            fehlermeldung("SQL-Fehler", repr(err))
            fehlermeldung(
                "Fehler in QKan_Import_from_HE (14)",
                "\nSchächte: in sql: \n" + sql + "\n\n",
            )

    if not dbQK.sql("UPDATE schaechte SET (geom, geop) = (geom, geop) ",
                    "importkanaldaten_he (14a)"):
        del dbQK
        del dbHE
        return False

    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Speicherschachtdaten

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM speicherschaechte'
    # if not dbQK.sql(sql, u'importkanaldaten_he (15)'):
    # return None

    # Daten aus ITWH-Datenbank abfragen
    sql = """
    SELECT NAME AS schnam, 
        GELAENDEHOEHE AS deckelhoehe, 
        SOHLHOEHE AS sohlhoehe, 
        XKOORDINATE AS xsch, 
        YKOORDINATE AS ysch, 
        UEBERSTAUFLAECHE AS ueberstauflaeche, 
        PLANUNGSSTATUS AS simstat_he, 
        KOMMENTAR AS kommentar, 
        LASTMODIFIED AS createdat 
        FROM SPEICHERSCHACHT"""

    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Speicherschachtdaten aufbereiten und in die QKan-DB schreiben

    logger.debug("simstatus[0]: {}".format(ref_simulationsstatus[0]))
    for attr in daten:
        (
            schnam,
            deckelhoehe,
            sohlhoehe,
            xsch,
            ysch,
            ueberstauflaeche,
            simstat_he,
            kommentar,
            createdat,
        ) = ["NULL" if el is None else el for el in attr]

        # (schnam, kommentar) = [tt.decode('iso-8859-1') for tt in (schnam_ansi, kommentar_ansi)]

        # Simstatus-Nr aus HE ersetzten
        if simstat_he in ref_simulationsstatus:
            simstatus = ref_simulationsstatus[simstat_he]
        else:
            # Noch nicht in Tabelle [simulationsstatus] enthalten, also ergqenzen
            simstatus = "({}_he)".format(simstat_he)
            sql = "INSERT INTO simulationsstatus (bezeichnung, he_nr) Values ('{simstatus}', {he_nr})".format(
                simstatus=simstatus, he_nr=simstat_he
            )
            ref_simulationsstatus[simstat_he] = simstatus
            if not dbQK.sql(sql, "importkanaldaten_he (16)"):
                del dbQK
                del dbHE
                return False

        # Geo-Objekte erzeugen

        # if QKan.config.database.type == enums.QKanDBChoice.SPATIALITE:
        #     geop = "MakePoint({0:},{1:},{2:})".format(xsch, ysch, epsg)
        #     geom = "CastToMultiPolygon(MakePolygon(MakeCircle({0:},{1:},{2:},{3:})))".format(
        #         xsch, ysch, (1.0 if durchm == "NULL" else durchm / 1000.0), epsg
        #     )
        # elif QKan.config.database.type == enums.QKanDBChoice.POSTGIS:
        #     geop = "ST_SetSRID(ST_MakePoint({0:},{1:}),{2:})".format(xsch, ysch, epsg)
        # else:
        #     fehlermeldung(
        #         "Programmfehler!",
        #         "Datenbanktyp ist fehlerhaft {}!\nAbbruch!".format(
        #             QKan.config.database.type
        #         ),
        #     )

        # Datensatz in die QKan-DB schreiben

        sql = f"""INSERT INTO schaechte_data (schnam, xsch, ysch, sohlhoehe, deckelhoehe, ueberstauflaeche, 
                    schachttyp, simstatus, kommentar, createdat)
            VALUES ('{schnam}', {xsch}, {ysch}, {sohlhoehe}, {deckelhoehe}, {ueberstauflaeche}, 
                    'Speicher', '{simstatus}', '{kommentar}', '{createdat}')"""

        if not dbQK.sql(sql, "importkanaldaten_he (17)"):
            del dbQK
            del dbHE
            return False

    if not dbQK.sql("UPDATE schaechte SET (geom, geop) = (geom, geop) ",
                    "importkanaldaten_he (17a)"):
        del dbQK
        del dbHE
        return False

    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Auslässe
    # Das Feld [TYP] enthält den Auslasstyp (0=Frei, 1=Normal, 2= Konstant, 3=Tide, 4=Zeitreihe)

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM auslaesse'
    # if not dbQK.sql(sql, u'importkanaldaten_he (18)'):
    # return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
    SELECT NAME AS schnam, 
        XKOORDINATE AS xsch, 
        YKOORDINATE AS ysch, 
        SOHLHOEHE AS sohlhoehe, 
        GELAENDEHOEHE AS deckelhoehe, 
        TYP AS typ_he, 
        PLANUNGSSTATUS AS simstat_he, 
        KOMMENTAR AS kommentar, 
        LASTMODIFIED AS createdat 
        FROM AUSLASS"""

    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Daten aufbereiten und in die QKan-DB schreiben

    for attr in daten:
        (
            schnam,
            xsch,
            ysch,
            sohlhoehe,
            deckelhoehe,
            typ_he,
            simstat_he,
            kommentar,
            createdat,
        ) = ["NULL" if el is None else el for el in attr]

        # (schnam, kommentar) = [tt.decode('iso-8859-1') for tt in (schnam_ansi, kommentar_ansi)]

        # Auslasstyp-Nr aus HE ersetzten
        if typ_he in ref_auslasstypen:
            auslasstyp = ref_auslasstypen[typ_he]
        else:
            # Noch nicht in Tabelle [auslasstypen] enthalten, also ergqenzen
            auslasstyp = "({}_he)".format(typ_he)
            sql = "INSERT INTO auslasstypen (bezeichnung, he_nr) Values ('{auslasstyp}', {he_nr})".format(
                auslasstyp=auslasstyp, he_nr=typ_he
            )
            ref_auslasstypen[typ_he] = auslasstyp
            if not dbQK.sql(sql, "importkanaldaten_he (19)"):
                del dbQK
                del dbHE
                return False

        # Simstatus-Nr aus HE ersetzten
        if simstat_he in ref_simulationsstatus:
            simstatus = ref_simulationsstatus[simstat_he]
        else:
            # Noch nicht in Tabelle [simulationsstatus] enthalten, also ergqenzen
            simstatus = "({}_he)".format(simstat_he)
            sql = "INSERT INTO simulationsstatus (bezeichnung, he_nr) Values ('{simstatus}', {he_nr})".format(
                simstatus=simstatus, he_nr=simstat_he
            )
            ref_simulationsstatus[simstat_he] = simstatus
            if not dbQK.sql(sql, "importkanaldaten_he (20)"):
                del dbQK
                del dbHE
                return False

        # Geo-Objekte erzeugen

        # if QKan.config.database.type == enums.QKanDBChoice.SPATIALITE:
        #     geop = "MakePoint({0:},{1:},{2:})".format(xsch, ysch, epsg)
        #     geom = "CastToMultiPolygon(MakePolygon(MakeCircle({0:},{1:},{2:},{3:})))".format(
        #         xsch, ysch, 1.0, epsg
        #     )
        # elif QKan.config.database.type == enums.QKanDBChoice.POSTGIS:
        #     geop = "ST_SetSRID(ST_MakePoint({0:},{1:}),{2:})".format(xsch, ysch, epsg)
        # else:
        #     fehlermeldung(
        #         "Programmfehler!",
        #         "Datenbanktyp ist fehlerhaft: {}!\nAbbruch!".format(
        #             QKan.config.database.type
        #         ),
        #     )

        # Datensatz in die QKan-DB schreiben

        sql = f"""INSERT INTO schaechte_data (schnam, xsch, ysch, sohlhoehe, deckelhoehe, 
                    auslasstyp, schachttyp, simstatus, kommentar, createdat)
            VALUES ('{schnam}', {xsch}, {ysch}, {sohlhoehe}, {deckelhoehe}, '{auslasstyp}', 
                    'Auslass', '{simstatus}', '{kommentar}', 
                    '{createdat}')"""
        if not dbQK.sql(sql, "importkanaldaten_he (21)"):
            del dbQK
            del dbHE
            return False

    if not dbQK.sql("UPDATE schaechte SET (geom, geop) = (geom, geop) ",
                    "importkanaldaten_he (21a)"):
        del dbQK
        del dbHE
        return False

    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Haltungsdaten
    # Feld [abflussart] entspricht dem Eingabefeld "System", das in einem Nachschlagefeld die
    # Werte 'Freispiegel', 'Druckabfluss', 'Abfluss im offenen Profil' anbietet

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM haltungen'
    # if not dbQK.sql(sql, u'importkanaldaten_he (6)'):
    # return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
    SELECT 
        ROHR.NAME AS haltnam, 
        ROHR.SCHACHTOBEN AS schoben, 
        ROHR.SCHACHTUNTEN AS schunten, 
        ROHR.GEOMETRIE1 AS hoehe, 
        ROHR.GEOMETRIE2 AS breite, 
        ROHR.LAENGE AS laenge, 
        ROHR.SOHLHOEHEOBEN AS sohleoben, 
        ROHR.SOHLHOEHEUNTEN AS sohleunten, 
        SO.DECKELHOEHE AS deckeloben, 
        SU.DECKELHOEHE AS deckelunten, 
        ROHR.TEILEINZUGSGEBIET AS teilgebiet, 
        ROHR.PROFILTYP AS profiltyp_he, 
        ROHR.SONDERPROFILBEZEICHNUNG AS profilnam, 
        ROHR.KANALART AS entwaesserungsart_he, 
        ROHR.RAUIGKEITSBEIWERT AS ks, 
        ROHR.PLANUNGSSTATUS AS simstat_he, 
        ROHR.KOMMENTAR AS kommentar, 
        ROHR.LASTMODIFIED AS createdat, 
        SO.XKOORDINATE AS xob, 
        SO.YKOORDINATE AS yob, 
        SU.XKOORDINATE AS xun, 
        SU.YKOORDINATE AS yun
    FROM ROHR 
    INNER JOIN (SELECT NAME, DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SCHACHT
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SPEICHERSCHACHT) AS SO ON ROHR.SCHACHTOBEN = SO.NAME 
    INNER JOIN (SELECT NAME, DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SCHACHT
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM AUSLASS
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SPEICHERSCHACHT) AS SU
    ON ROHR.SCHACHTUNTEN = SU.NAME"""
    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Haltungsdaten in die QKan-DB schreiben

    for attr in daten:
        (
            haltnam,
            schoben,
            schunten,
            hoehe,
            breite,
            laenge,
            sohleoben,
            sohleunten,
            deckeloben,
            deckelunten,
            teilgebiet,
            profiltyp_he,
            profilnam,
            entwaesserungsart_he,
            ks,
            simstat_he,
            kommentar,
            createdat,
            xob,
            yob,
            xun,
            yun,
        ) = ["NULL" if el is None else el for el in attr]

        # (haltnam, schoben, schunten, profilnam, kommentar) = \
        # [tt.decode('iso-8859-1') for tt in (haltnam_ansi, schoben_ansi, schunten_ansi,
        # profilnam_ansi, kommentar_ansi)]

        # Anwendung der Referenzlisten HE -> QKan

        # Rohrprofile. In HE werden primär Profilnummern verwendet. Bei Sonderprofilen ist die Profilnummer = 68
        # und zur eindeutigen Identifikation dient stattdessen der Profilname.
        # In QKan wird ausschließlich der Profilname verwendet, so dass sichergestellt sein muss, dass die
        # Standardbezeichnungen für die HE-Profile nicht auch als Namen für ein Sonderprofil verwendet werden.

        if profiltyp_he in ref_profil:
            profilnam = ref_profil[profiltyp_he]
        else:
            # Noch nicht in Tabelle [profile] enthalten, also ergqenzen
            if profilnam == "NULL":
                # In HE ist nur die Profilnummer enthalten. Dann muss ein Profilname erzeugt werden, z.B. (12)
                profilnam = "({profiltyp_he})".format(profiltyp_he=profiltyp_he)

            # In Referenztabelle in dieser Funktion sowie in der QKan-Tabelle profile einfügen
            ref_profil[profiltyp_he] = profilnam
            sql = "INSERT INTO profile (profilnam, he_nr) Values ('{profilnam}', {profiltyp_he})".format(
                profilnam=profilnam, profiltyp_he=profiltyp_he
            )
            if not dbQK.sql(sql, "importkanaldaten_he (7)"):
                del dbQK
                del dbHE
                return False

        # Entwasserungsarten. Hier ist es einfacher als bei den Profilen...
        if entwaesserungsart_he in ref_entwart:
            entwart = ref_entwart[entwaesserungsart_he]
        else:
            # Noch nicht in Tabelle [entwaesserungsarten] enthalten, also ergqenzen
            entwart = "({})".format(entwaesserungsart_he)
            sql = "INSERT INTO entwaesserungsarten (bezeichnung, he_nr) Values ('{entwart}', {he_nr})".format(
                entwart=entwart, he_nr=entwaesserungsart_he
            )
            ref_entwart[entwaesserungsart_he] = entwart
            if not dbQK.sql(sql, "importkanaldaten_he (8)"):
                del dbQK
                del dbHE
                return False

        # Simstatus-Nr aus HE ersetzten
        if simstat_he in ref_simulationsstatus:
            simstatus = ref_simulationsstatus[simstat_he]
        else:
            # Noch nicht in Tabelle [simulationsstatus] enthalten, also ergqenzen
            simstatus = "({}_he)".format(simstat_he)
            sql = "INSERT INTO simulationsstatus (bezeichnung, he_nr) Values ('{simstatus}', {he_nr})".format(
                simstatus=simstatus, he_nr=simstat_he
            )
            ref_simulationsstatus[simstat_he] = simstatus
            if not dbQK.sql(sql, "importkanaldaten_he (9)"):
                del dbQK
                del dbHE
                return False

        # Geo-Objekt erzeugen
        # if QKan.config.database.type == enums.QKanDBChoice.SPATIALITE:
        #     geom = f"MakeLine(MakePoint({xob},{yob},{epsg}),MakePoint({xun},{yun},{epsg}))"
        # elif QKan.config.database.type == enums.QKanDBChoice.POSTGIS:
        #     geom = f"ST_MakeLine(ST_SetSRID(ST_MakePoint({xob},{yob}),{epsg}),ST_SetSRID(ST_MakePoint({xun},{yun}),{epsg}))"
        # else:
        #     fehlermeldung(
        #         "Programmfehler!",
        #         "Datenbanktyp ist fehlerhaft: {}!\nAbbruch!".format(
        #             QKan.config.database.type
        #         ),
        #     )

        # Datensatz aufbereiten in die QKan-DB schreiben

        try:
            sql = """INSERT INTO haltungen_data 
                (haltnam, schoben, schunten, 
                hoehe, breite, laenge, sohleoben, sohleunten, 
                deckeloben, deckelunten, teilgebiet, profilnam, entwart, ks, simstatus, kommentar, createdat) VALUES (
                '{haltnam}', '{schoben}', '{schunten}', {hoehe}, {breite}, {laenge}, 
                {sohleoben}, {sohleunten}, {deckeloben}, {deckelunten}, '{teilgebiet}', '{profilnam}', 
                '{entwart}', {ks}, '{simstatus}', '{kommentar}', '{createdat}')""".format(
                haltnam=haltnam,
                schoben=schoben,
                schunten=schunten,
                hoehe=hoehe,
                breite=breite,
                laenge=laenge,
                sohleoben=sohleoben,
                sohleunten=sohleunten,
                deckeloben=deckeloben,
                deckelunten=deckelunten,
                teilgebiet=teilgebiet,
                profilnam=profilnam,
                entwart=entwart,
                ks=ks,
                simstatus=simstatus,
                kommentar=kommentar,
                createdat=createdat,
            )
        except BaseException as err:
            fehlermeldung("SQL-Fehler", repr(err))
            fehlermeldung(
                "Fehler in QKan_Import_from_HE",
                "\nFehler in sql INSERT INTO haltungen: \n"
                + str(
                    (
                        haltnam,
                        schoben,
                        schunten,
                        hoehe,
                        breite,
                        laenge,
                        sohleoben,
                        sohleunten,
                        deckeloben,
                        deckelunten,
                        teilgebiet,
                        profilnam,
                        entwart,
                        ks,
                        simstatus,
                    )
                )
                + "\n\n",
            )

        if not dbQK.sql(sql, "importkanaldaten_he (10)"):
            del dbQK
            del dbHE
            return False

    if not dbQK.sql("UPDATE haltungen SET geom = geom", "importkanaldaten_he (10a)"):
        del dbQK
        del dbHE
        return False

    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Pumpen

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM pumpen'
    # if not dbQK.sql(sql, u'importkanaldaten_he (22)'):
    # return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
    SELECT 
        PUMPE.NAME AS pnam, 
        PUMPE.SCHACHTOBEN AS schoben, 
        PUMPE.SCHACHTUNTEN AS schunten, 
        PUMPE.TYP AS typ_he, 
        PUMPE.STEUERSCHACHT AS steuersch, 
        PUMPE.EINSCHALTHOEHE AS einschalthoehe, 
        PUMPE.AUSSCHALTHOEHE AS ausschalthoehe,
        SO.XKOORDINATE AS xob, 
        SO.YKOORDINATE AS yob, 
        SU.XKOORDINATE AS xun, 
        SU.YKOORDINATE AS yun, 
        PUMPE.PLANUNGSSTATUS AS simstat_he, 
        PUMPE.KOMMENTAR AS kommentar, 
        PUMPE.LASTMODIFIED AS createdat
    FROM PUMPE
    LEFT JOIN (SELECT NAME, DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SCHACHT
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SPEICHERSCHACHT) AS SO ON PUMPE.SCHACHTOBEN = SO.NAME 
    LEFT JOIN (SELECT NAME, DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SCHACHT
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM AUSLASS
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SPEICHERSCHACHT) AS SU
    ON PUMPE.SCHACHTUNTEN = SU.NAME"""
    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Pumpendaten in die QKan-DB schreiben

    for attr in daten:
        (
            pnam,
            schoben,
            schunten,
            typ_he,
            steuersch,
            einschalthoehe,
            ausschalthoehe,
            xob,
            yob,
            xun,
            yun,
            simstat_he,
            kommentar,
            createdat,
        ) = ["NULL" if el is None else el for el in attr]

        # (pnam, schoben, schunten, kommentar) = [tt.decode('iso-8859-1') for tt in (pnam_ansi, schoben_ansi,
        # schunten_ansi, kommentar_ansi)]

        # Pumpentyp-Nr aus HE ersetzten
        if typ_he in ref_pumpentyp:
            pumpentyp = ref_pumpentyp[typ_he]
        else:
            # Noch nicht in Tabelle [pumpentypen] enthalten, also ergqenzen
            pumpentyp = "({}_he)".format(typ_he)
            sql = "INSERT INTO pumpentypen (bezeichnung, he_nr) Values ('{pumpentyp}', {he_nr})".format(
                pumpentyp=pumpentyp, he_nr=typ_he
            )
            ref_pumpentyp[typ_he] = pumpentyp
            if not dbQK.sql(sql, "importkanaldaten_he (23)"):
                del dbQK
                del dbHE
                return False

        # Simstatus-Nr aus HE ersetzten
        if simstat_he in ref_simulationsstatus:
            simstatus = ref_simulationsstatus[simstat_he]
        else:
            # Noch nicht in Tabelle [simulationsstatus] enthalten, also ergqenzen
            simstatus = "({}_he)".format(simstat_he)
            sql = "INSERT INTO simulationsstatus (bezeichnung, he_nr) Values ('{simstatus}', {he_nr})".format(
                simstatus=simstatus, he_nr=simstat_he
            )
            ref_simulationsstatus[simstat_he] = simstatus
            if not dbQK.sql(sql, "importkanaldaten_he (24)"):
                del dbQK
                del dbHE
                return False

        # Geo-Objekt erzeugen

        # if QKan.config.database.type == enums.QKanDBChoice.SPATIALITE:
        #     geom = f"MakeLine(MakePoint({xob},{yob},{epsg}),MakePoint({xun},{yun},{epsg}))"
        # elif QKan.config.database.type == enums.QKanDBChoice.POSTGIS:
        #     geom = f"ST_MakeLine(ST_SetSRID(ST_MakePoint({xob},{yob}),{epsg}),ST_SetSRID(ST_MakePoint({xun},{yun}),{epsg}))"
        # else:
        #     fehlermeldung(
        #         "Programmfehler!",
        #         "Datenbanktyp ist fehlerhaft: {}!\nAbbruch!".format(
        #             QKan.config.database.type
        #         ),
        #     )

        # Datensatz aufbereiten und in die QKan-DB schreiben

        try:
            sql = """INSERT INTO pumpen_data 
                (pnam, schoben, schunten, pumpentyp, steuersch, einschalthoehe, ausschalthoehe, 
                simstatus, kommentar, createdat) 
                VALUES ('{pnam}', '{schoben}', '{schunten}', '{pumpentyp}', '{steuersch}', 
                {einschalthoehe}, {ausschalthoehe}, '{simstatus}', '{kommentar}', '{createdat}')""".format(
                pnam=pnam,
                schoben=schoben,
                schunten=schunten,
                pumpentyp=pumpentyp,
                steuersch=steuersch,
                einschalthoehe=einschalthoehe,
                ausschalthoehe=ausschalthoehe,
                simstatus=simstatus,
                kommentar=kommentar,
                createdat=createdat,
            )

        except BaseException as err:
            fehlermeldung("SQL-Fehler", repr(err))
            fehlermeldung(
                "Fehler in QKan_Import_from_HE",
                "\nFehler in sql INSERT INTO pumpen: \n"
                + str(
                    (
                        pnam,
                        schoben,
                        schunten,
                        pumpentyp,
                        steuersch,
                        einschalthoehe,
                        ausschalthoehe,
                    )
                )
                + "\n\n",
            )

        if not dbQK.sql(sql, "importkanaldaten_he (25)"):
            del dbQK
            del dbHE
            return False

    if not dbQK.sql("UPDATE pumpen SET geom = geom", "importkanaldaten_he (25a)"):
        del dbQK
        del dbHE
        return False

    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Wehre

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM wehre'
    # if not dbQK.sql(sql, u'importkanaldaten_he (26)'):
    # return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
    SELECT 
        WEHR.NAME AS wnam,
        WEHR.SCHACHTOBEN AS schoben, 
        WEHR.SCHACHTUNTEN AS schunten, 
        WEHR.TYP AS typ_he, 
        WEHR.SCHWELLENHOEHE AS schwellenhoehe, 
        WEHR.GEOMETRIE1 AS kammerhoehe, 
        WEHR.GEOMETRIE2 AS laenge,
        WEHR.UEBERFALLBEIWERT AS uebeiwert,
        SO.XKOORDINATE AS xob, 
        SO.YKOORDINATE AS yob, 
        SU.XKOORDINATE AS xun, 
        SU.YKOORDINATE AS yun, 
        WEHR.PLANUNGSSTATUS AS simstat_he, 
        WEHR.KOMMENTAR AS kommentar, 
        WEHR.LASTMODIFIED AS createdat
    FROM WEHR
    LEFT JOIN (SELECT NAME, DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SCHACHT
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SPEICHERSCHACHT) AS SO ON WEHR.SCHACHTOBEN = SO.NAME 
    LEFT JOIN (SELECT NAME, DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SCHACHT
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM AUSLASS
         UNION SELECT NAME, GELAENDEHOEHE AS DECKELHOEHE, XKOORDINATE, YKOORDINATE FROM SPEICHERSCHACHT) AS SU
    ON WEHR.SCHACHTUNTEN = SU.NAME"""
    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Wehrdaten in die QKan-DB schreiben

    for attr in daten:
        (
            wnam,
            schoben,
            schunten,
            typ_he,
            schwellenhoehe,
            kammerhoehe,
            laenge,
            uebeiwert,
            xob,
            yob,
            xun,
            yun,
            simstat_he,
            kommentar,
            createdat,
        ) = ["NULL" if el is None else el for el in attr]

        # (wnam, schoben, schunten, kommentar) = [tt.decode('iso-8859-1') for tt in (wnam_ansi, schoben_ansi,
        # schunten_ansi, kommentar_ansi)]

        # Simstatus-Nr aus HE ersetzten
        if simstat_he in ref_simulationsstatus:
            simstatus = ref_simulationsstatus[simstat_he]
        else:
            # Noch nicht in Tabelle [simulationsstatus] enthalten, also ergqenzen
            simstatus = "({}_he)".format(simstat_he)
            sql = "INSERT INTO simulationsstatus (bezeichnung, he_nr) Values ('{simstatus}', {he_nr})".format(
                simstatus=simstatus, he_nr=simstat_he
            )
            ref_simulationsstatus[simstat_he] = simstatus
            if not dbQK.sql(sql, "importkanaldaten_he (27)"):
                del dbQK
                del dbHE
                return False

        # Geo-Objekt erzeugen

        # if QKan.config.database.type == enums.QKanDBChoice.SPATIALITE:
        #     geom = f"MakeLine(MakePoint({xob},{yob},{epsg}),MakePoint({xun},{yun},{epsg}))"
        # elif QKan.config.database.type == enums.QKanDBChoice.POSTGIS:
        #     geom = f"ST_MakeLine(ST_SetSRID(ST_MakePoint({xob},{yob}),{epsg}),ST_SetSRID(ST_MakePoint({xun},{yun}),{epsg}))"
        # else:
        #     fehlermeldung(
        #         "Programmfehler!",
        #         "Datenbanktyp ist fehlerhaft: {}!\nAbbruch!".format(
        #             QKan.config.database.type
        #         ),
        #     )

        # Datensatz aufbereiten und in die QKan-DB schreiben

        try:
            sql = """INSERT INTO wehre_data (wnam, schoben, schunten, schwellenhoehe, kammerhoehe,
                 laenge, uebeiwert, simstatus, kommentar, createdat) 
                 VALUES ('{wnam}', '{schoben}', '{schunten}', {schwellenhoehe},
                {kammerhoehe}, {laenge}, {uebeiwert}, '{simstatus}', '{kommentar}', 
                '{createdat}')""".format(
                wnam=wnam,
                schoben=schoben,
                schunten=schunten,
                schwellenhoehe=schwellenhoehe,
                kammerhoehe=kammerhoehe,
                laenge=laenge,
                uebeiwert=uebeiwert,
                simstatus=simstatus,
                kommentar=kommentar,
                createdat=createdat,
            )
            ok = True
        except BaseException as err:
            fehlermeldung("Fehler", repr(err))
            ok = False
            fehlermeldung(
                "Fehler in QKan_Import_from_HE",
                "\nFehler in sql INSERT INTO wehre: \n"
                + str(
                    (
                        wnam,
                        schoben,
                        schunten,
                        schwellenhoehe,
                        kammerhoehe,
                        laenge,
                        uebeiwert,
                    )
                )
                + "\n\n",
            )

        if ok:
            if not dbQK.sql(sql, "importkanaldaten_he (28)"):
                del dbQK
                del dbHE
                return False

    if not dbQK.sql("UPDATE wehre SET geom = geom", "importkanaldaten_he (28a)"):
        del dbQK
        del dbHE
        return False

    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Einzugsgebiete

    # Tabelle in QKan-Datenbank bleibt bestehen, damit gegebenenfalls erstellte
    # Teileinzugsgebiete, deren Geo-Objekte ja in HYSTEM-EXTRAN nicht verwaltet
    # werden können, erhalten bleiben. Deshalb wird beim Import geprüft, ob das
    # jeweilige Objekt schon vorhanden ist.
    # sql = u'DELETE FROM einzugsgebiete'
    # if not dbQK.sql(sql, u'importkanaldaten_he (29)'):
    #     return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
    SELECT 
        NAME AS tgnam,
        EINWOHNERDICHTE AS ewdichte,
        WASSERVERBRAUCH AS wverbrauch,
        STUNDENMITTEL AS stdmittel,
        FREMDWASSERANTEIL AS fremdwas,
        FLAECHE AS flaeche,
        KOMMENTAR AS kommentar,
        LASTMODIFIED AS createdat
    FROM
        teileinzugsgebiet"""

    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Teileinzugsgebietsdaten in die QKan-DB schreiben

    for attr in daten:
        (
            tgnam,
            ewdichte,
            wverbrauch,
            stdmittel,
            fremdwas,
            flaeche,
            kommentar,
            createdat,
        ) = ["NULL" if el is None else el for el in attr]

        # (tgnam, kommentar) = [tt.decode('iso-8859-1') for tt in (tgnam_ansi, kommentar_ansi)]

        # Datensatz aufbereiten und in die QKan-DB schreiben

        try:
            sql = """
              INSERT INTO einzugsgebiete (tgnam, ewdichte, wverbrauch, stdmittel,
                fremdwas, kommentar, createdat) 
              VALUES ('{tgnam}', {ewdichte}, {wverbrauch}, {stdmittel}, {fremdwas},
                '{kommentar}', '{createdat}')
                 """.format(
                tgnam=tgnam,
                ewdichte=ewdichte,
                wverbrauch=wverbrauch,
                stdmittel=stdmittel,
                fremdwas=fremdwas,
                kommentar=kommentar,
                createdat=createdat,
            )
            ok = True
        except BaseException as err:
            fehlermeldung("SQL-Fehler", repr(err))
            fehlermeldung(
                "Fehler in QKan_Import_from_HE",
                "\nFehler in sql INSERT INTO einzugsgebiete: \n"
                + repr(
                    (
                        tgnam,
                        ewdichte,
                        wverbrauch,
                        stdmittel,
                        fremdwas,
                        kommentar,
                        createdat,
                    )
                )
                + "\n\n",
            )
            ok = False

        if ok:
            if not dbQK.sql(sql, "importkanaldaten_he (30)"):
                del dbQK
                del dbHE
                return False
    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Speicherkennlinien

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM speicherkennlinien'
    # if not dbQK.sql(sql, u'importkanaldaten_he (31)'):
    # return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
        SELECT 
            NAME AS schnam, 
            KEYWERT + SOHLHOEHE AS wspiegel, 
            WERT AS oberfl 
        FROM TABELLENINHALTE 
        JOIN SPEICHERSCHACHT 
        ON TABELLENINHALTE.ID = SPEICHERSCHACHT.ID 
        ORDER BY SPEICHERSCHACHT.ID, TABELLENINHALTE.REIHENFOLGE"""

    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Speicherdaten in die QKan-DB schreiben

    for attr in daten:
        (schnam, wspiegel, oberfl) = ["NULL" if el is None else el for el in attr]

        # schnam = schnam_ansi.decode('iso-8859-1')

        # Datensatz aufbereiten und in die QKan-DB schreiben

        sql = """INSERT INTO speicherkennlinien (schnam, wspiegel, oberfl) 
             VALUES ('{schnam}', {wspiegel}, {oberfl})""".format(
            schnam=schnam, wspiegel=wspiegel, oberfl=oberfl
        )

        if not dbQK.sql(sql, "importkanaldaten_he (32)"):
            del dbQK
            del dbHE
            return False
    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Sonderprofildaten

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM profildaten'
    # if not dbQK.sql(sql, u'importkanaldaten_he (33)'):
    # return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
        SELECT 
            NAME AS profilnam, 
            KEYWERT AS wspiegel, 
            WERT AS wbreite 
        FROM TABELLENINHALTE 
        JOIN SONDERPROFIL 
        ON TABELLENINHALTE.ID = SONDERPROFIL.ID 
        ORDER BY SONDERPROFIL.ID, TABELLENINHALTE.REIHENFOLGE"""

    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Profil in die QKan-DB schreiben

    for attr in daten:
        (profilnam, wspiegel, wbreite) = ["NULL" if el is None else el for el in attr]

        # profilnam = profilnam_ansi.decode('iso-8859-1')

        # Datensatz aufbereiten und in die QKan-DB schreiben

        sql = """INSERT INTO profildaten (profilnam, wspiegel, wbreite) 
             VALUES ('{profilnam}', {wspiegel}, {wbreite})""".format(
            profilnam=profilnam, wspiegel=wspiegel, wbreite=wbreite
        )

        if not dbQK.sql(sql, "importkanaldaten_he (34)"):
            del dbQK
            del dbHE
            return False
    dbQK.commit()

    # ------------------------------------------------------------------------------
    # Abflussparameter

    # Tabelle in QKan-Datenbank leeren
    # if check_tabinit:
    # sql = u'DELETE FROM abflussparameter'
    # if not dbQK.sql(sql, u'importkanaldaten_he (35)'):
    # return None

    # Daten aUS ITWH-Datenbank abfragen
    sql = """
        SELECT 
            NAME AS apnam,
            ABFLUSSBEIWERTANFANG AS anfangsabflussbeiwert,
            ABFLUSSBEIWERTENDE AS endabflussbeiwert,
            MULDENVERLUST AS muldenverlust,
            BENETZUNGSVERLUST AS benetzungsverlust,
            BENETZUNGSPEICHERSTART AS benetzung_startwert,
            MULDENAUFFUELLGRADSTART AS mulden_startwert,
            TYP AS aptyp,
            BODENKLASSE AS bodenklasse,
            LASTMODIFIED AS createdat,
            KOMMENTAR AS kommentar
        FROM ABFLUSSPARAMETER"""

    dbHE.sql(sql)
    daten = dbHE.fetchall()

    # Abflussparameter in die QKan-DB schreiben

    # Zuerst sicherstellen, dass die Datensätze nicht schon vorhanden sind. Falls doch, werden sie überschrieben
    sql = "SELECT apnam FROM abflussparameter"
    if not dbQK.sql(sql, "importkanaldaten_he (36)"):
        del dbQK
        del dbHE
        return False
    datqk = [el[0] for el in dbQK.fetchall()]

    for attr in daten:
        (
            apnam,
            anfangsabflussbeiwert,
            endabflussbeiwert,
            muldenverlust,
            benetzungsverlust,
            benetzung_startwert,
            mulden_startwert,
            aptyp,
            bodenklasse,
            createdat,
            kommentar,
        ) = ["NULL" if el is None else el for el in attr]

        # (apnam, bodenklasse, kommentar) = [tt.decode('iso-8859-1') for tt in
        # (apnam_ansi, bodenklasse_ansi, kommentar_ansi)]

        if aptyp == 0:
            bodenklasse = "NULL"  # in QKan default für befestigte Flächen

        # Datensatz in die QKan-DB schreiben

        # Falls Datensatz bereits vorhanden: löschen
        if apnam in datqk:
            sql = "DELETE FROM abflussparameter WHERE apnam = '{}'".format(apnam)
            if not dbQK.sql(sql, "importkanaldaten_he (37)"):
                del dbQK
                del dbHE
                return False

        sql = """INSERT INTO abflussparameter
              ( apnam, anfangsabflussbeiwert, endabflussbeiwert, 
                benetzungsverlust, muldenverlust, 
                benetzung_startwert, mulden_startwert, 
                bodenklasse, kommentar, createdat) 
              VALUES 
              ( '{apnam}', {anfangsabflussbeiwert}, {endabflussbeiwert}, 
                {benetzungsverlust}, {muldenverlust}, 
                {benetzung_startwert}, {mulden_startwert}, 
                '{bodenklasse}', '{kommentar}', '{createdat}')""".format(
            apnam=apnam,
            anfangsabflussbeiwert=anfangsabflussbeiwert,
            endabflussbeiwert=endabflussbeiwert,
            benetzungsverlust=benetzungsverlust,
            muldenverlust=muldenverlust,
            benetzung_startwert=benetzung_startwert,
            mulden_startwert=mulden_startwert,
            bodenklasse=bodenklasse,
            kommentar=kommentar,
            createdat=createdat,
        )

        if not dbQK.sql(sql, "importkanaldaten_he (38)"):
            del dbQK
            del dbHE
            return False
    dbQK.commit()

    # Schachttypen auswerten
    eval_node_types(dbQK)  # in qkan.database.qkan_utils

    # Projektdatei laden und anpassen

    projecttemplate = (
        Path(pluginDirectory("qkan")) / "templates" / "Projekt.qgs"
    )

    qgsadapt(
        projecttemplate,
        database_QKan,
        dbQK,
        projectfile,
        epsg,
    )

    # --------------------------------------------------------------------------
    # Datenbankverbindungen schliessen

    del dbHE
    del dbQK

    # ------------------------------------------------------------------------------
    # Abschluss: Ggfs. Protokoll schreiben und Datenbankverbindungen schliessen

    # iface.mainWindow().statusBar().clearMessage()
    # iface.messageBar().pushMessage("Information", "Datenimport ist fertig!", level=QgsMessageBar.INFO)
    # QgsMessageLog.logMessage(message="\nFertig: Datenimport erfolgreich!", level=Qgis.Info)

    # Importiertes Projekt laden
    project = QgsProject.instance()
    # project.read(QFileInfo(projectfile))
    project.read(projectfile)  # read the new project file


# ----------------------------------------------------------------------------------------------------------------------

# Verzeichnis der Testdaten
pfad = "C:/FHAC/jupiter/hoettges/team_data/Kanalprogramme/k_qkan/k_heqk/beispiele/linges_deng"

database_HE = os.path.join(pfad, "21.04.2017-2pumpen.idbf")
database_QKan = os.path.join(pfad, "netz.sqlite")
projectfile = os.path.join(pfad, "plan.qgs")
epsg = 31466

if __name__ == "__main__":
    importKanaldaten(database_HE, database_QKan, projectfile, epsg)
elif __name__ == "__console__":
    # QMessageBox.information(None, "Info", "Das Programm wurde aus der QGIS-Konsole aufgerufen")
    importKanaldaten(database_HE, database_QKan, projectfile, epsg)
elif __name__ == "__builtin__":
    # QMessageBox.information(None, "Info", "Das Programm wurde aus der QGIS-Toolbox aufgerufen")
    importKanaldaten(database_HE, database_QKan, projectfile, epsg)
# else:
# QMessageBox.information(None, "Info", "Die Variable __name__ enthält: {0:s}".format(__name__))
