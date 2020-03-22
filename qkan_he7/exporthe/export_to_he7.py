# -*- coding: utf-8 -*-

"""
  Export Kanaldaten nach HYSTEM-EXTRAN
  ====================================

  Transfer von Kanaldaten aus einer QKan-Datenbank nach HYSTEM EXTRAN 7.6

  | Dateiname            : export_to_he7.py
  | Date                 : Februar 2017
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
import shutil
import time

from qgis.core import Qgis, QgsMessageLog
from qgis.PyQt.QtWidgets import QProgressBar
from qkan.database.fbfunc import FBConnection
from qkan.database.qkan_database import versionolder
from qkan.database.qkan_utils import checknames, fehlermeldung, fortschritt, meldung
from qkan import enums

# Referenzlisten
from qkan.database.reflists import abflusstypen
from qkan.linkflaechen.updatelinks import updatelinkageb, updatelinkfl, updatelinksw

logger = logging.getLogger("QKan.exporthe.export_to_he7")

progress_bar = None


# Hauptprogramm ---------------------------------------------------------------------------------------------


def exportKanaldaten(
    iface,
    database_HE,
    dbtemplate_HE,
    dbQK,
    liste_teilgebiete,
    autokorrektur,
    fangradius=0.1,
    mindestflaeche=0.5,
    mit_verschneidung=True,
    exportFlaechenHE8=True,
    check_export={},
):
    """Export der Kanaldaten aus einer QKan-SpatiaLite-Datenbank und Schreiben in eine HE-Firebird-Datenbank.

    :database_HE:           Pfad zur HE-Firebird-Datenbank
    :type database_HE:      string

    :dbtemplate_HE:         Vorlage für die zu erstellende Firebird-Datenbank
    :type dbtemplate_HE:    string

    :dbQK:                  Datenbankobjekt, das die Verknüpfung zur QKan-SpatiaLite-Datenbank verwaltet.
    :type dbQK:             DBConnection

    :liste_teilgebiete:     Liste der ausgewählten Teilgebiete
    :type liste_teilgebiete: String

    :autokorrektur:         Option, ob eine automatische Korrektur der Bezeichnungen durchgeführt
                            werden soll. Falls nicht, wird die Bearbeitung mit einer Fehlermeldung
                            abgebrochen.
    :type autokorrektur:    String

    :fangradius:            Suchradius, mit dem an den Enden der Verknüpfungen (linkfl, linksw) eine 
                            Haltung bzw. ein Einleitpunkt zugeordnet wird. 
    :type fangradius:       Float

    :mindestflaeche:        Mindestflächengröße bei Einzelflächen und Teilflächenstücken
    :type mindestflaeche:   float

    :mit_verschneidung:     Flächen werden mit Haltungsflächen verschnitten (abhängig von Attribut "aufteilen")
    :type mit_verschneidung: Boolean

    :check_export:          Liste von Export-Optionen
    :type check_export:     Dictionary

    :returns:               void
    """

    # Statusmeldung in der Anzeige
    global progress_bar
    progress_bar = QProgressBar(iface.messageBar())
    progress_bar.setRange(0, 100)
    status_message = iface.messageBar().createMessage(
        "", "Export in Arbeit. Bitte warten."
    )
    status_message.layout().addWidget(progress_bar)
    iface.messageBar().pushWidget(status_message, Qgis.Info, 10)

    # Referenzliste der Abflusstypen für HYSTEM-EXTRAN
    he_fltyp_ref = abflusstypen("he")

    # ITWH-Datenbank aus gewählter Vorlage kopieren
    if os.path.exists(database_HE):
        try:
            os.remove(database_HE)
        except BaseException as err:
            fehlermeldung(
                "Fehler (33) in QKan_Export",
                "Die HE-Datenbank ist schon vorhanden und kann nicht ersetzt werden: {}".format(
                    repr(err)
                ),
            )
            return False
    try:
        shutil.copyfile(dbtemplate_HE, database_HE)
    except BaseException as err:
        fehlermeldung(
            "Fehler (34) in QKan_Export",
            "Kopieren der Vorlage HE-Datenbank fehlgeschlagen: {}\nVorlage: {}\nZiel: {}\n".format(
                repr(err), dbtemplate_HE, database_HE
            ),
        )
        return False
    fortschritt("Firebird-Datenbank aus Vorlage kopiert...", 0.01)
    progress_bar.setValue(1)

    # Verbindung zur Hystem-Extran-Datenbank

    dbHE = FBConnection(database_HE)  # Datenbankobjekt der HE-Datenbank zum Schreiben

    if dbHE is None:
        fehlermeldung(
            "(1) Fehler",
            "ITWH-Datenbank {:s} wurde nicht gefunden!\nAbbruch!".format(database_HE),
        )
        return None

    # --------------------------------------------------------------------------------------------------
    # Zur Abschaetzung der voraussichtlichen Laufzeit

    # if not dbQK.sql("SELECT count(*) AS n FROM schaechte", "export_to_he7.laufzeit (1)")
    # del dbHE
    # return False
    # anzdata = float(dbQK.fetchone()[0])
    # fortschritt("Anzahl Schächte: {}".format(anzdata))

    # if not dbQK.sql("SELECT count(*) AS n FROM haltungen", "export_to_he7.laufzeit (2)")
    # del dbHE
    # return False
    # anzdata += float(dbQK.fetchone()[0])
    # fortschritt("Anzahl Haltungen: {}".format(anzdata))

    # if not dbQK.sql("SELECT count(*) AS n FROM flaechen", "export_to_he7.laufzeit (3)")
    # del dbHE
    # return False
    # anzdata += float(dbQK.fetchone()[0]) * 2
    # fortschritt("Anzahl Flächen: {}".format(anzdata))

    # --------------------------------------------------------------------------------------------
    # Besonderes Gimmick des ITWH-Programmiers: Die IDs der Tabellen muessen sequentiell
    # vergeben werden!!! Ein Grund ist, dass (u.a.?) die Tabelle "tabelleninhalte" mit verschiedenen
    # Tabellen verknuepft ist und dieser ID eindeutig sein muss.

    if not dbHE.sql("SELECT NEXTID, VERSION FROM ITWH$PROGINFO"):
        del dbHE
        return False
    data = dbHE.fetchone()
    nextid = int(data[0]) + 1
    heDBVersion = data[1].split(".")
    logger.debug("HE IDBF-Version {}".format(heDBVersion))

    # --------------------------------------------------------------------------------------------
    # Export der Schaechte

    logger.debug(f"""check_export:\n{', '.join(check_export.keys())}\n""")

    if check_export["export_schaechte"] or check_export["modify_schaechte"]:

        # Nur Daten fuer ausgewaehlte Teilgebiete
        if len(liste_teilgebiete) != 0:
            auswahl = " AND schaechte.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl = ""

        sql = """
            SELECT
                schaechte.schnam AS schnam,
                schaechte.deckelhoehe AS deckelhoehe,
                schaechte.sohlhoehe AS sohlhoehe,
                schaechte.durchm AS durchmesser,
                schaechte.strasse AS strasse,
                schaechte.xsch AS xsch,
                schaechte.ysch AS ysch, 
                schaechte.createdat
            FROM schaechte
            WHERE schaechte.schachttyp = 'Schacht'{}
            """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_schaechte"):
            del dbHE
            return False

        nr0 = nextid

        fortschritt("Export Schaechte Teil 1...", 0.1)
        progress_bar.setValue(15)

        for attr in dbQK.fetchall():
            # progress_bar.setValue(progress_bar.value() + 1)

            # In allen Feldern None durch NULL ersetzen
            (
                schnam,
                deckelhoehe_t,
                sohlhoehe_t,
                durchmesser_t,
                strasse,
                xsch_t,
                ysch_t,
                createdat_t,
            ) = ("NULL" if el is None else el for el in attr)

            # Formatierung der Zahlen
            (deckelhoehe, sohlhoehe, durchmesser, xsch, ysch) = (
                "NULL" if tt == "NULL" else "{:.3f}".format(float(tt))
                for tt in (deckelhoehe_t, sohlhoehe_t, durchmesser_t, xsch_t, ysch_t)
            )

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Ändern vorhandener Datensätze
            if check_export["modify_schaechte"]:
                sql = """
                    UPDATE SCHACHT SET
                    DECKELHOEHE={deckelhoehe}, KANALART={kanalart}, DRUCKDICHTERDECKEL={druckdichterdeckel},
                    SOHLHOEHE={sohlhoehe}, XKOORDINATE={xkoordinate}, YKOORDINATE={ykoordinate},
                    KONSTANTERZUFLUSS={konstanterzufluss}, GELAENDEHOEHE={gelaendehoehe},
                    ART={art}, ANZAHLKANTEN={anzahlkanten}, SCHEITELHOEHE={scheitelhoehe},
                    PLANUNGSSTATUS='{planungsstatus}', LASTMODIFIED='{lastmodified}',
                    DURCHMESSER={durchmesser}
                    WHERE NAME = '{name}';
                """.format(
                    deckelhoehe=deckelhoehe,
                    kanalart="0",
                    druckdichterdeckel="0",
                    sohlhoehe=sohlhoehe,
                    xkoordinate=xsch,
                    ykoordinate=ysch,
                    konstanterzufluss="0",
                    gelaendehoehe=deckelhoehe,
                    art="1",
                    anzahlkanten="0",
                    scheitelhoehe="0",
                    planungsstatus="0",
                    name=schnam,
                    lastmodified=createdat,
                    durchmesser=durchmesser,
                )

                if not dbHE.sql(sql, "dbHE: export_schaechte (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_schaechte"]:
                # Trick: In Firebird ist kein SELECT ohne Tabelle möglich. Tabelle "RDB$DATABASE" hat genau 1 Datensatz
                sql = """
                    INSERT INTO SCHACHT
                    ( DECKELHOEHE, KANALART, DRUCKDICHTERDECKEL, SOHLHOEHE, XKOORDINATE, YKOORDINATE,
                      KONSTANTERZUFLUSS, GELAENDEHOEHE, ART, ANZAHLKANTEN, SCHEITELHOEHE,
                      PLANUNGSSTATUS, NAME, LASTMODIFIED, ID, DURCHMESSER)
                    SELECT
                      {deckelhoehe}, {kanalart}, {druckdichterdeckel}, {sohlhoehe}, {xkoordinate},
                      {ykoordinate}, {konstanterzufluss}, {gelaendehoehe}, {art}, {anzahlkanten},
                      {scheitelhoehe}, '{planungsstatus}', '{name}', '{lastmodified}', {id}, {durchmesser}
                    FROM RDB$DATABASE
                    WHERE '{name}' NOT IN (SELECT NAME FROM SCHACHT);
                """.format(
                    deckelhoehe=deckelhoehe,
                    kanalart="0",
                    druckdichterdeckel="0",
                    sohlhoehe=sohlhoehe,
                    xkoordinate=xsch,
                    ykoordinate=ysch,
                    konstanterzufluss="0",
                    gelaendehoehe=deckelhoehe,
                    art="1",
                    anzahlkanten="0",
                    scheitelhoehe="0",
                    planungsstatus="0",
                    name=schnam,
                    lastmodified=createdat,
                    id=nextid,
                    durchmesser=durchmesser,
                )

                if not dbHE.sql(sql, "dbHE: export_schaechte (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Schaechte eingefuegt".format(nextid - nr0), 0.30)
        progress_bar.setValue(30)

    # --------------------------------------------------------------------------------------------
    # Export der Speicherbauwerke
    #
    # Beim Export werden die IDs mitgeschrieben, um bei den Speicherkennlinien
    # wiederverwertet zu werden.

    if check_export["export_speicher"] or check_export["modify_speicher"]:

        # Nur Daten fuer ausgewaehlte Teilgebiete
        if len(liste_teilgebiete) != 0:
            auswahl = " AND schaechte.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl = ""

        sql = """
            SELECT
                schaechte.schnam AS schnam,
                schaechte.deckelhoehe AS deckelhoehe,
                schaechte.sohlhoehe AS sohlhoehe,
                schaechte.durchm AS durchmesser,
                schaechte.strasse AS strasse,
                schaechte.xsch AS xsch,
                schaechte.ysch AS ysch,
                kommentar AS kommentar,
                createdat
            FROM schaechte
            WHERE schaechte.schachttyp = 'Speicher'{}
            """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_speicher"):
            del dbHE
            return False

        nr0 = nextid
        refid_speicher = {}

        for attr in dbQK.fetchall():
            fortschritt("Export Speicherschaechte...", 0.35)
            progress_bar.setValue(35)

            # In allen Feldern None durch NULL ersetzen
            (
                schnam,
                deckelhoehe_t,
                sohlhoehe_t,
                durchmesser_t,
                strasse,
                xsch_t,
                ysch_t,
                kommentar,
                createdat_t,
            ) = ("NULL" if el is None else el for el in attr)

            # Formatierung der Zahlen
            (deckelhoehe, sohlhoehe, durchmesser, xsch, ysch) = (
                "NULL" if tt == "NULL" else "{:.3f}".format(float(tt))
                for tt in (deckelhoehe_t, sohlhoehe_t, durchmesser_t, xsch_t, ysch_t)
            )

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Speichern der aktuellen ID zum Speicherbauwerk
            refid_speicher[schnam] = nextid

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_speicher"]:
                sql = """
                    UPDATE SPEICHERSCHACHT SET
                    TYP={typ}, SOHLHOEHE={sohlhoehe},
                      XKOORDINATE={xkoordinate}, YKOORDINATE={ykoordinate},
                      GELAENDEHOEHE={gelaendehoehe}, ART={art}, ANZAHLKANTEN={anzahlkanten},
                      SCHEITELHOEHE={scheitelhoehe}, HOEHEVOLLFUELLUNG={hoehevollfuellung},
                      KONSTANTERZUFLUSS={konstanterzufluss}, ABSETZWIRKUNG={absetzwirkung}, 
                      PLANUNGSSTATUS='{planungsstatus}',
                      LASTMODIFIED='{lastmodified}', KOMMENTAR='{kommentar}'
                      WHERE NAME='{name}';
                """.format(
                    typ="1",
                    sohlhoehe=sohlhoehe,
                    xkoordinate=xsch,
                    ykoordinate=ysch,
                    gelaendehoehe=deckelhoehe,
                    art="1",
                    anzahlkanten="0",
                    scheitelhoehe=deckelhoehe,
                    hoehevollfuellung=deckelhoehe,
                    konstanterzufluss="0",
                    absetzwirkung="0",
                    planungsstatus="0",
                    name=schnam,
                    lastmodified=createdat,
                    kommentar=kommentar,
                    durchmesser=durchmesser,
                )

                if not dbHE.sql(sql, "dbHE: export_speicher (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_speicher"]:
                # Trick: In Firebird ist kein SELECT ohne Tabelle möglich. Tabelle "RDB$DATABASE" hat genau 1 Datensatz
                sql = """
                    INSERT INTO SPEICHERSCHACHT
                    ( ID, TYP, SOHLHOEHE,
                      XKOORDINATE, YKOORDINATE,
                      GELAENDEHOEHE, ART, ANZAHLKANTEN,
                      SCHEITELHOEHE, HOEHEVOLLFUELLUNG,
                      KONSTANTERZUFLUSS, ABSETZWIRKUNG, PLANUNGSSTATUS,
                      NAME, LASTMODIFIED, KOMMENTAR)
                    SELECT
                      {id}, {typ}, {sohlhoehe},
                      {xkoordinate}, {ykoordinate},
                      {gelaendehoehe}, {art}, {anzahlkanten},
                      {scheitelhoehe}, {hoehevollfuellung},
                      {konstanterzufluss}, {absetzwirkung}, '{planungsstatus}',
                      '{name}', '{lastmodified}', '{kommentar}'
                    FROM RDB$DATABASE
                    WHERE '{name}' NOT IN (SELECT NAME FROM SPEICHERSCHACHT);
                """.format(
                    id=nextid,
                    typ="1",
                    sohlhoehe=sohlhoehe,
                    xkoordinate=xsch,
                    ykoordinate=ysch,
                    gelaendehoehe=deckelhoehe,
                    art="1",
                    anzahlkanten="0",
                    scheitelhoehe=deckelhoehe,
                    hoehevollfuellung=deckelhoehe,
                    konstanterzufluss="0",
                    absetzwirkung="0",
                    planungsstatus="0",
                    name=schnam,
                    lastmodified=createdat,
                    kommentar=kommentar,
                    durchmesser=durchmesser,
                )

                if not dbHE.sql(sql, "dbHE: export_speicher (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Speicher eingefuegt".format(nextid - nr0), 0.40)

        # --------------------------------------------------------------------------------------------
        # Export der Kennlinien der Speicherbauwerke - nur wenn auch Speicher exportiert werden

        if (
            check_export["export_speicherkennlinien"]
            or check_export["modify_speicherkennlinien"]
        ):

            sql = """SELECT sl.schnam, sl.wspiegel - sc.sohlhoehe AS wtiefe, sl.oberfl
                      FROM speicherkennlinien AS sl
                      JOIN schaechte AS sc ON sl.schnam = sc.schnam
                      ORDER BY sc.schnam, sl.wspiegel"""

            if not dbQK.sql(sql, "dbQK: export_to_he7.export_speicherkennlinien"):
                del dbHE
                return False

            spnam = None  # Zähler für Speicherkennlinien

            for attr in dbQK.fetchall():

                # In allen Feldern None durch NULL ersetzen
                (schnam, wtiefe, oberfl) = ("NULL" if el is None else el for el in attr)

                # Einfuegen in die Datenbank

                if schnam in refid_speicher:
                    if spnam == "NULL" or schnam != spnam:
                        spnam = schnam
                        reihenfolge = 1
                    else:
                        schnam = spnam
                        reihenfolge += 1

                    # Ändern vorhandener Datensätze entfällt bei Tabellendaten

                    # Einfuegen in die Datenbank
                    if check_export["export_speicherkennlinien"]:
                        # Trick: In Firebird ist kein SELECT ohne Tabelle möglich. Tabelle "RDB$DATABASE" hat genau 1 Datensatz
                        sql = """
                            INSERT INTO TABELLENINHALTE
                            ( KEYWERT, WERT, REIHENFOLGE, ID)
                            SELECT
                              {wtiefe}, {oberfl}, {reihenfolge}, {id}
                            FROM RDB$DATABASE;
                        """.format(
                            wtiefe=wtiefe,
                            oberfl=oberfl,
                            reihenfolge=reihenfolge,
                            id=refid_speicher[schnam],
                        )
                        # print(sql)

                        if not dbHE.sql(sql, "dbHE: export_speicherkennlinien"):
                            del dbHE
                            return False

            dbHE.commit()

            fortschritt("{} Speicher eingefuegt".format(nextid - nr0), 0.40)
    progress_bar.setValue(45)

    # --------------------------------------------------------------------------------------------
    # Export der Auslaesse

    if check_export["export_auslaesse"] or check_export["modify_auslaesse"]:

        # Nur Daten fuer ausgewaehlte Teilgebiete
        if len(liste_teilgebiete) != 0:
            auswahl = " AND schaechte.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl = ""

        sql = """
            SELECT
                schaechte.schnam AS schnam,
                schaechte.deckelhoehe AS deckelhoehe,
                schaechte.sohlhoehe AS sohlhoehe,
                schaechte.durchm AS durchmesser,
                schaechte.xsch AS xsch,
                schaechte.ysch AS ysch,
                kommentar AS kommentar,
                createdat
            FROM schaechte
            WHERE schaechte.schachttyp = 'Auslass'{}
            """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_auslaesse"):
            del dbHE
            return False

        nr0 = nextid

        fortschritt("Export Auslässe...", 0.20)

        for attr in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            (
                schnam,
                deckelhoehe_t,
                sohlhoehe_t,
                durchmesser_t,
                xsch_t,
                ysch_t,
                kommentar,
                createdat_t,
            ) = ("NULL" if el is None else el for el in attr)

            # Formatierung der Zahlen
            (deckelhoehe, sohlhoehe, durchmesser, xsch, ysch) = (
                "NULL" if tt == "NULL" else "{:.3f}".format(float(tt))
                for tt in (deckelhoehe_t, sohlhoehe_t, durchmesser_t, xsch_t, ysch_t)
            )

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_auslaesse"]:
                sql = """
                    UPDATE AUSLASS SET
                    TYP={typ}, RUECKSCHLAGKLAPPE={rueckschlagklappe},
                    SOHLHOEHE={sohlhoehe}, XKOORDINATE={xkoordinate}, YKOORDINATE={ykoordinate},
                    GELAENDEHOEHE={gelaendehoehe}, ART={art}, ANZAHLKANTEN={anzahlkanten},
                    SCHEITELHOEHE={scheitelhoehe}, KONSTANTERZUFLUSS={konstanterzufluss},
                    PLANUNGSSTATUS='{planungsstatus}',
                    LASTMODIFIED='{lastmodified}', KOMMENTAR='{kommentar}'
                    WHERE NAME = '{name}';
                """.format(
                    typ="1",
                    rueckschlagklappe=0,
                    sohlhoehe=sohlhoehe,
                    xkoordinate=xsch,
                    ykoordinate=ysch,
                    gelaendehoehe=deckelhoehe,
                    art="3",
                    anzahlkanten="0",
                    scheitelhoehe=deckelhoehe,
                    konstanterzufluss=0,
                    planungsstatus="0",
                    name=schnam,
                    lastmodified=createdat,
                    kommentar=kommentar,
                    durchmesser=durchmesser,
                )

                if not dbHE.sql(sql, "dbHE: export_auslaesse (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_auslaesse"]:
                sql = """
                    INSERT INTO AUSLASS
                    ( ID, TYP, RUECKSCHLAGKLAPPE, SOHLHOEHE,
                      XKOORDINATE, YKOORDINATE,
                      GELAENDEHOEHE, ART, ANZAHLKANTEN,
                      SCHEITELHOEHE, KONSTANTERZUFLUSS, PLANUNGSSTATUS,
                      NAME, LASTMODIFIED, KOMMENTAR)
                    SELECT
                      {id}, {typ}, {rueckschlagklappe}, {sohlhoehe},
                      {xkoordinate}, {ykoordinate},
                      {gelaendehoehe}, {art}, {anzahlkanten},
                      {scheitelhoehe}, {konstanterzufluss}, '{planungsstatus}',
                      '{name}', '{lastmodified}', '{kommentar}'
                    FROM RDB$DATABASE
                    WHERE '{name}' NOT IN (SELECT NAME FROM AUSLASS);
                """.format(
                    id=nextid,
                    typ="1",
                    rueckschlagklappe=0,
                    sohlhoehe=sohlhoehe,
                    xkoordinate=xsch,
                    ykoordinate=ysch,
                    gelaendehoehe=deckelhoehe,
                    art="3",
                    anzahlkanten="0",
                    scheitelhoehe=deckelhoehe,
                    konstanterzufluss=0,
                    planungsstatus="0",
                    name=schnam,
                    lastmodified=createdat,
                    kommentar=kommentar,
                    durchmesser=durchmesser,
                )

                if not dbHE.sql(sql, "dbHE: export_auslaesse (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Auslässe eingefuegt".format(nextid - nr0), 0.40)
    progress_bar.setValue(50)

    # --------------------------------------------------------------------------------------------
    # Export der Pumpen
    #

    if check_export["export_pumpen"] or check_export["modify_pumpen"]:

        # Nur Daten fuer ausgewaehlte Teilgebiete
        if len(liste_teilgebiete) != 0:
            auswahl = " WHERE pumpen.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl = ""

        sql = """
            SELECT
                pumpen.pnam AS pnam,
                pumpen.schoben AS schoben,
                pumpen.schunten AS schunten,
                pumpentypen.he_nr AS pumpentypnr,
                pumpen.steuersch AS steuersch,
                pumpen.einschalthoehe AS einschalthoehe_t,
                pumpen.ausschalthoehe AS ausschalthoehe_t,
                simulationsstatus.he_nr AS simstatusnr,
                pumpen.kommentar AS kommentar,
                pumpen.createdat AS createdat
            FROM pumpen
            LEFT JOIN pumpentypen
            ON pumpen.pumpentyp = pumpentypen.bezeichnung
            LEFT JOIN simulationsstatus
            ON pumpen.simstatus = simulationsstatus.bezeichnung{}
            """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_pumpen"):
            del dbHE
            return False

        nr0 = nextid

        fortschritt("Export Pumpen...", 0.60)

        for attr in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            (
                pnam,
                schoben,
                schunten,
                pumpentypnr,
                steuersch,
                einschalthoehe_t,
                ausschalthoehe_t,
                simstatusnr,
                kommentar,
                createdat,
            ) = ("NULL" if el is None else el for el in attr)

            # Formatierung der Zahlen
            (einschalthoehe, ausschalthoehe) = (
                "NULL" if tt == "NULL" else "{:.3f}".format(float(tt))
                for tt in (einschalthoehe_t, ausschalthoehe_t)
            )

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_pumpen"]:
                sql = """
                    UPDATE PUMPE SET
                    TYP={typ}, SCHACHTOBEN='{schoben}', SCHACHTUNTEN='{schunten}', 
                    STEUERSCHACHT='{steuersch}', 
                    EINSCHALTHOEHE={einschalthoehe}, 
                    AUSSCHALTHOEHE={ausschalthoehe}, PLANUNGSSTATUS={simstatusnr},
                    LASTMODIFIED='{lastmodified}', KOMMENTAR='{kommentar}'
                    WHERE NAME = '{name}';
                """.format(
                    name=pnam,
                    typ=pumpentypnr,
                    schoben=schoben,
                    schunten=schunten,
                    steuersch=steuersch,
                    einschalthoehe=einschalthoehe,
                    ausschalthoehe=ausschalthoehe,
                    simstatusnr=simstatusnr,
                    lastmodified=createdat,
                    kommentar=kommentar,
                )

                if not dbHE.sql(sql, "dbHE: export_pumpen (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_pumpen"]:
                sql = """
                    INSERT INTO PUMPE
                    ( ID, NAME, TYP, SCHACHTOBEN, SCHACHTUNTEN, 
                      STEUERSCHACHT, EINSCHALTHOEHE, 
                      AUSSCHALTHOEHE, PLANUNGSSTATUS,
                      LASTMODIFIED, KOMMENTAR)
                    SELECT
                      {id}, '{name}', {typ}, '{schoben}', '{schunten}', 
                      '{steuersch}', {einschalthoehe}, {ausschalthoehe}, 
                      {simstatusnr},
                      '{lastmodified}', '{kommentar}'
                    FROM RDB$DATABASE
                    WHERE '{name}' NOT IN (SELECT NAME FROM PUMPE);
                """.format(
                    id=nextid,
                    name=pnam,
                    typ=pumpentypnr,
                    schoben=schoben,
                    schunten=schunten,
                    steuersch=steuersch,
                    einschalthoehe=einschalthoehe,
                    ausschalthoehe=ausschalthoehe,
                    simstatusnr=simstatusnr,
                    lastmodified=createdat,
                    kommentar=kommentar,
                )

                if not dbHE.sql(sql, "dbHE: export_pumpen (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Pumpen eingefuegt".format(nextid - nr0), 0.40)
    progress_bar.setValue(60)

    # --------------------------------------------------------------------------------------------
    # Export der Wehre
    #

    if check_export["export_wehre"] or check_export["modify_wehre"]:

        # Nur Daten fuer ausgewaehlte Teilgebiete
        if len(liste_teilgebiete) != 0:
            auswahl = " WHERE wehre.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl = ""

        sql = """
            SELECT
                wehre.wnam AS wnam,
                wehre.schoben AS schoben,
                wehre.schunten AS schunten,
                coalesce(sob.sohlhoehe, 0) AS sohleoben_t,
                coalesce(sun.sohlhoehe, 0) AS sohleunten_t,
                wehre.schwellenhoehe AS schwellenhoehe_t,
                wehre.kammerhoehe AS kammerhoehe_t,
                wehre.laenge AS laenge_t,
                wehre.uebeiwert AS uebeiwert_t,
                simulationsstatus.he_nr AS simstatusnr,
                wehre.kommentar AS kommentar,
                wehre.createdat AS createdat
            FROM wehre
            LEFT JOIN simulationsstatus
            ON wehre.simstatus = simulationsstatus.bezeichnung
            LEFT JOIN schaechte AS sob 
            ON wehre.schoben = sob.schnam
            LEFT JOIN schaechte AS sun 
            ON wehre.schunten = sun.schnam{}
            """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_wehre"):
            del dbHE
            return False

        nr0 = nextid

        fortschritt("Export Wehre...", 0.65)

        for attr in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            (
                wnam,
                schoben,
                schunten,
                sohleoben_t,
                sohleunten_t,
                schwellenhoehe_t,
                kammerhoehe_t,
                laenge_t,
                uebeiwert_t,
                simstatusnr,
                kommentar,
                createdat,
            ) = ("NULL" if el is None else el for el in attr)

            # Formatierung der Zahlen
            (sohleoben, sohleunten, schwellenhoehe, kammerhoehe, laenge, uebeiwert) = (
                "NULL" if tt == "NULL" else "{:.3f}".format(float(tt))
                for tt in (
                    sohleoben_t,
                    sohleunten_t,
                    schwellenhoehe_t,
                    kammerhoehe_t,
                    laenge_t,
                    uebeiwert_t,
                )
            )

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_wehre"]:
                sql = """
                    UPDATE WEHR SET
                    TYP=1, SCHWELLENHOEHE={schwellenhoehe},	UEBERFALLBEIWERT={uebeiwert},
                    GEOMETRIE1={kammerhoehe}, GEOMETRIE2={laenge},
                    SCHACHTOBEN='{schoben}', SCHACHTUNTEN='{schunten}', 
                    SOHLHOEHEOBEN='{sohleoben}', SOHLHOEHEUNTEN='{sohleunten}', 
                    PLANUNGSSTATUS={simstatusnr},
                    LASTMODIFIED='{lastmodified}', KOMMENTAR='{kommentar}'
                    WHERE NAME = '{name}';
                """.format(
                    name=wnam,
                    typ=1,
                    schoben=schoben,
                    schunten=schunten,
                    sohleoben=sohleoben,
                    sohleunten=sohleunten,
                    schwellenhoehe=schwellenhoehe,
                    kammerhoehe=kammerhoehe,
                    laenge=laenge,
                    uebeiwert=uebeiwert,
                    simstatusnr=simstatusnr,
                    lastmodified=createdat,
                    kommentar=kommentar,
                )

                if not dbHE.sql(sql, "dbHE: export_wehre (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_wehre"]:
                sql = """
                    INSERT INTO WEHR
                    (ID, NAME, TYP, SCHACHTOBEN, SCHACHTUNTEN, 
                     SOHLHOEHEOBEN, SOHLHOEHEUNTEN, 
                     SCHWELLENHOEHE, GEOMETRIE1, 
                     GEOMETRIE2, UEBERFALLBEIWERT, 
                     RUECKSCHLAGKLAPPE, VERFAHRBAR, PROFILTYP, 
                     EREIGNISBILANZIERUNG, EREIGNISGRENZWERTENDE,
                     EREIGNISGRENZWERTANFANG, EREIGNISTRENNDAUER, 
                     EREIGNISINDIVIDUELL, PLANUNGSSTATUS, 
                     LASTMODIFIED, KOMMENTAR)
                    SELECT
                      {id}, '{name}', {typ}, '{schoben}', '{schunten}', 
                      {sohleoben}, {sohleunten}, 
                      {schwellenhoehe}, {kammerhoehe}, 
                      {laenge}, {uebeiwert}, 
                      {rueckschlagklappe}, {verfahrbar}, {profiltyp}, 
                      {ereignisbilanzierung}, {ereignisgrenzwertende},
                      {ereignisgrenzwertanfang}, {ereignistrenndauer}, 
                      {ereignisindividuell}, {simstatusnr},
                      '{lastmodified}', '{kommentar}'
                    FROM RDB$DATABASE
                    WHERE '{name}' NOT IN (SELECT NAME FROM WEHR);
                """.format(
                    id=nextid,
                    name=wnam,
                    typ=1,
                    schoben=schoben,
                    schunten=schunten,
                    sohleoben=sohleoben,
                    sohleunten=sohleunten,
                    schwellenhoehe=schwellenhoehe,
                    kammerhoehe=kammerhoehe,
                    laenge=laenge,
                    uebeiwert=uebeiwert,
                    rueckschlagklappe=0,
                    verfahrbar=0,
                    profiltyp=52,
                    ereignisbilanzierung=0,
                    ereignisgrenzwertende=0,
                    ereignisgrenzwertanfang=0,
                    ereignistrenndauer=0,
                    ereignisindividuell=0,
                    simstatusnr=simstatusnr,
                    lastmodified=createdat,
                    kommentar=kommentar,
                )

                if not dbHE.sql(sql, "dbHE: export_wehre (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Wehre eingefuegt".format(nextid - nr0), 0.40)
    progress_bar.setValue(60)

    # --------------------------------------------------------------------------------------------
    # Export der Haltungen
    #
    # Erläuterung zum Feld "GESAMTFLAECHE":
    # Die Haltungsfläche (area(tezg.geom)) wird in das Feld "GESAMTFLAECHE" eingetragen und erscheint damit
    # in HYSTEM-EXTRAN in der Karteikarte "Haltungen > Trockenwetter". Solange dort kein
    # Siedlungstyp zugeordnet ist, wird diese Fläche nicht wirksam und dient nur der Information!

    if check_export["export_haltungen"] or check_export["modify_haltungen"]:

        # Nur Daten fuer ausgewaehlte Teilgebiete
        if len(liste_teilgebiete) != 0:
            auswahl = " AND haltungen.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl = ""

        sql = """
          SELECT
              haltungen.haltnam AS haltnam, haltungen.schoben AS schoben, haltungen.schunten AS schunten,
              coalesce(haltungen.laenge, glength(haltungen.geom)) AS laenge_t,
              coalesce(haltungen.sohleoben,sob.sohlhoehe) AS sohleoben_t,
              coalesce(haltungen.sohleunten,sun.sohlhoehe) AS sohleunten_t,
              haltungen.profilnam AS profilnam, profile.he_nr AS he_nr, haltungen.hoehe AS hoehe_t, haltungen.breite AS breite_t,
              entwaesserungsarten.he_nr AS entw_nr,
              haltungen.rohrtyp AS rohrtyp, haltungen.ks AS rauheit_t,
              haltungen.teilgebiet AS teilgebiet, haltungen.createdat AS createdat
            FROM
              (haltungen JOIN schaechte AS sob ON haltungen.schoben = sob.schnam)
              JOIN schaechte AS sun ON haltungen.schunten = sun.schnam
              LEFT JOIN profile ON haltungen.profilnam = profile.profilnam
              LEFT JOIN entwaesserungsarten ON haltungen.entwart = entwaesserungsarten.bezeichnung
              LEFT JOIN simulationsstatus AS st ON haltungen.simstatus = st.bezeichnung
              WHERE (st.he_nr IN ('0', '1', '2') or st.he_nr IS NULL){:}
        """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_haltungen"):
            del dbHE
            return False

        fortschritt("Export Haltungen...", 0.35)

        nr0 = nextid

        # Varianten abhängig von HE-Version
        if versionolder(heDBVersion[0:2], ["7", "8"], 2):
            logger.debug("Version vor 7.8 erkannt")
            fieldsnew = ""
            attrsnew = ""
            valuesnew = ""
        elif versionolder(heDBVersion[0:2], ["7", "9"], 2):
            logger.debug("Version vor 7.9 erkannt")
            fieldsnew = ", EINZUGSGEBIET = 0, KONSTANTERZUFLUSSTEZG = 0"
            attrsnew = ", EINZUGSGEBIET, KONSTANTERZUFLUSSTEZG"
            valuesnew = ", 0, 0"
        else:
            logger.debug("Version 7.9 erkannt")
            fieldsnew = ", EINZUGSGEBIET = 0, KONSTANTERZUFLUSSTEZG = 0, BEFESTIGTEFLAECHE = 0, UNBEFESTIGTEFLAECHE = 0"
            attrsnew = ", EINZUGSGEBIET, KONSTANTERZUFLUSSTEZG, BEFESTIGTEFLAECHE, UNBEFESTIGTEFLAECHE"
            valuesnew = ", 0, 0, 0, 0"

        for attr in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            (
                haltnam,
                schoben,
                schunten,
                laenge_t,
                sohleoben_t,
                sohleunten_t,
                profilnam,
                he_nr,
                hoehe_t,
                breite_t,
                entw_nr,
                rohrtyp,
                rauheit_t,
                teilgebiet,
                createdat_t,
            ) = ("NULL" if el is None else el for el in attr)

            # Datenkorrekturen
            (laenge, sohleoben, sohleunten, hoehe, breite) = (
                "NULL" if tt == "NULL" else "{:.4f}".format(float(tt))
                for tt in (laenge_t, sohleoben_t, sohleunten_t, hoehe_t, breite_t)
            )

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            if rauheit_t == "NULL":
                rauheit = "1.5"
            else:
                rauheit = "{:.3f}".format(float(rauheit_t))

                h_profil = he_nr
            if h_profil == "68":
                h_sonderprofil = profilnam
            else:
                h_sonderprofil = ""

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_haltungen"]:
                # Profile < 0 werden nicht uebertragen
                if int(h_profil) > 0:
                    sql = """
                      UPDATE ROHR SET
                      SCHACHTOBEN='{schachtoben}', SCHACHTUNTEN='{schachtunten}',
                      LAENGE={laenge}, SOHLHOEHEOBEN={sohlhoeheoben},
                      SOHLHOEHEUNTEN={sohlhoeheunten}, PROFILTYP='{profiltyp}', 
                      SONDERPROFILBEZEICHNUNG='{sonderprofilbezeichnung}',
                      GEOMETRIE1={geometrie1}, GEOMETRIE2={geometrie2}, KANALART='{kanalart}',
                      RAUIGKEITSBEIWERT={rauigkeitsbeiwert}, ANZAHL={anzahl},
                      TEILEINZUGSGEBIET='{teileinzugsgebiet}', RUECKSCHLAGKLAPPE={rueckschlagklappe},
                      KONSTANTERZUFLUSS={konstanterzufluss},
                      RAUIGKEITSANSATZ={rauigkeitsansatz},
                      GEFAELLE={gefaelle}, GESAMTFLAECHE={gesamtflaeche}, ABFLUSSART={abflussart},
                      INDIVIDUALKONZEPT={individualkonzept}, HYDRAULISCHERRADIUS={hydraulischerradius},
                      RAUHIGKEITANZEIGE={rauhigkeitanzeige}, PLANUNGSSTATUS={planungsstatus},
                      LASTMODIFIED='{lastmodified}', MATERIALART={materialart},
                      EREIGNISBILANZIERUNG={ereignisbilanzierung},
                      EREIGNISGRENZWERTENDE={ereignisgrenzwertende},
                      EREIGNISGRENZWERTANFANG={ereignisgrenzwertanfang},
                      EREIGNISTRENNDAUER={ereignistrenndauer}, 
                      EREIGNISINDIVIDUELL={ereignisindividuell}{fieldsnew}
                      WHERE NAME = '{name}';
                      """.format(
                        name=haltnam,
                        schachtoben=schoben,
                        schachtunten=schunten,
                        laenge=laenge,
                        sohlhoeheoben=sohleoben,
                        sohlhoeheunten=sohleunten,
                        profiltyp=h_profil,
                        sonderprofilbezeichnung=h_sonderprofil,
                        geometrie1=hoehe,
                        geometrie2=breite,
                        kanalart=entw_nr,
                        rauigkeitsbeiwert=1.5,
                        anzahl=1,
                        teileinzugsgebiet="",
                        rueckschlagklappe=0,
                        konstanterzufluss=0,
                        rauigkeitsansatz=1,
                        gefaelle=0,
                        gesamtflaeche=0,
                        abflussart=0,
                        individualkonzept=0,
                        hydraulischerradius=0,
                        rauhigkeitanzeige=1.5,
                        planungsstatus=0,
                        lastmodified=createdat,
                        materialart=28,
                        ereignisbilanzierung=0,
                        ereignisgrenzwertende=0,
                        ereignisgrenzwertanfang=0,
                        ereignistrenndauer=0,
                        ereignisindividuell=0,
                        fieldsnew=fieldsnew,
                    )

                    if not dbHE.sql(sql, "dbHE: export_haltungen (1)"):
                        del dbHE
                        return False

            # Einfuegen in die Datenbank
            if check_export["export_haltungen"]:
                # Profile < 0 werden nicht uebertragen
                if int(h_profil) > 0:
                    sql = """
                      INSERT INTO ROHR
                      ( NAME, SCHACHTOBEN, SCHACHTUNTEN, LAENGE, SOHLHOEHEOBEN,
                        SOHLHOEHEUNTEN, PROFILTYP, SONDERPROFILBEZEICHNUNG, GEOMETRIE1,
                        GEOMETRIE2, KANALART, RAUIGKEITSBEIWERT, ANZAHL, TEILEINZUGSGEBIET,
                        RUECKSCHLAGKLAPPE, KONSTANTERZUFLUSS,
                        RAUIGKEITSANSATZ, GEFAELLE, GESAMTFLAECHE, ABFLUSSART,
                        INDIVIDUALKONZEPT, HYDRAULISCHERRADIUS, RAUHIGKEITANZEIGE, PLANUNGSSTATUS,
                        LASTMODIFIED, MATERIALART, EREIGNISBILANZIERUNG, EREIGNISGRENZWERTENDE,
                        EREIGNISGRENZWERTANFANG, EREIGNISTRENNDAUER, EREIGNISINDIVIDUELL, ID{attrsnew})
                      SELECT
                        '{name}', '{schachtoben}', '{schachtunten}', {laenge}, {sohlhoeheoben},
                        {sohlhoeheunten}, '{profiltyp}', '{sonderprofilbezeichnung}', {geometrie1},
                        {geometrie2}, '{kanalart}', {rauigkeitsbeiwert}, {anzahl}, '{teileinzugsgebiet}',
                        {rueckschlagklappe}, {konstanterzufluss},
                        {rauigkeitsansatz}, {gefaelle}, {gesamtflaeche}, {abflussart}, 
                        {individualkonzept}, {hydraulischerradius}, {rauhigkeitanzeige}, {planungsstatus},
                        '{lastmodified}', {materialart}, {ereignisbilanzierung}, {ereignisgrenzwertende},
                        {ereignisgrenzwertanfang}, {ereignistrenndauer}, 
                        {ereignisindividuell}, {id}{valuesnew}
                      FROM RDB$DATABASE
                      WHERE '{name}' NOT IN (SELECT NAME FROM ROHR);
                      """.format(
                        name=haltnam,
                        schachtoben=schoben,
                        schachtunten=schunten,
                        laenge=laenge,
                        sohlhoeheoben=sohleoben,
                        sohlhoeheunten=sohleunten,
                        profiltyp=h_profil,
                        sonderprofilbezeichnung=h_sonderprofil,
                        geometrie1=hoehe,
                        geometrie2=breite,
                        kanalart=entw_nr,
                        rauigkeitsbeiwert=1.5,
                        anzahl=1,
                        teileinzugsgebiet="",
                        rueckschlagklappe=0,
                        konstanterzufluss=0,
                        rauigkeitsansatz=1,
                        gefaelle=0,
                        gesamtflaeche=0,
                        abflussart=0,
                        befestigte_flaeche=0,
                        unbefestigte_flaeche=0,
                        individualkonzept=0,
                        hydraulischerradius=0,
                        rauhigkeitanzeige=1.5,
                        planungsstatus=0,
                        lastmodified=createdat,
                        materialart=28,
                        ereignisbilanzierung=0,
                        ereignisgrenzwertende=0,
                        ereignisgrenzwertanfang=0,
                        ereignistrenndauer=0,
                        ereignisindividuell=0,
                        id=nextid,
                        attrsnew=attrsnew,
                        valuesnew=valuesnew,
                    )

                    if not dbHE.sql(sql, "dbHE: export_haltungen (2)"):
                        del dbHE
                        return False

                    nextid += 1
        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Haltungen eingefuegt".format(nextid - nr0), 0.60)
    progress_bar.setValue(70)

    # --------------------------------------------------------------------------------------------
    # Export der Bodenklassen

    if check_export["export_bodenklassen"] or check_export["modify_bodenklassen"]:

        sql = """
            SELECT
                bknam AS bknam,
                infiltrationsrateanfang AS infiltrationsrateanfang, 
                infiltrationsrateende AS infiltrationsrateende, 
                infiltrationsratestart AS infiltrationsratestart, 
                rueckgangskonstante AS rueckgangskonstante, 
                regenerationskonstante AS regenerationskonstante, 
                saettigungswassergehalt AS saettigungswassergehalt, 
                createdat AS createdat,
                kommentar AS kommentar
            FROM bodenklassen
            """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_bodenklassen"):
            del dbHE
            return False

        nr0 = nextid

        for attr in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            (
                bknam,
                infiltrationsrateanfang,
                infiltrationsrateende,
                infiltrationsratestart,
                rueckgangskonstante,
                regenerationskonstante,
                saettigungswassergehalt,
                createdat_t,
                kommentar,
            ) = ("NULL" if el is None else el for el in attr)

            # Der leere Satz Bodenklasse ist nur für interne QKan-Zwecke da.
            if bknam == "NULL":
                continue

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_bodenklassen"]:
                sql = """
                    UPDATE BODENKLASSE SET
                    INFILTRATIONSRATEANFANG={infiltrationsrateanfang},
                    INFILTRATIONSRATEENDE={infiltrationsrateende},
                    INFILTRATIONSRATESTART={infiltrationsratestart},
                    RUECKGANGSKONSTANTE={rueckgangskonstante},
                    REGENERATIONSKONSTANTE={regenerationskonstante},
                    SAETTIGUNGSWASSERGEHALT={saettigungswassergehalt},
                    LASTMODIFIED='{lastmodified}',
                    KOMMENTAR='{kommentar}'
                    WHERE NAME = '{name}';
                    """.format(
                    infiltrationsrateanfang=infiltrationsrateanfang,
                    infiltrationsrateende=infiltrationsrateende,
                    infiltrationsratestart=infiltrationsratestart,
                    rueckgangskonstante=rueckgangskonstante,
                    regenerationskonstante=regenerationskonstante,
                    saettigungswassergehalt=saettigungswassergehalt,
                    name=bknam,
                    lastmodified=createdat,
                    kommentar=kommentar,
                )

                if not dbHE.sql(sql, "dbHE: export_bodenklassen (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_bodenklassen"]:
                sql = """
                  INSERT INTO BODENKLASSE
                  ( INFILTRATIONSRATEANFANG, INFILTRATIONSRATEENDE,
                    INFILTRATIONSRATESTART, RUECKGANGSKONSTANTE, REGENERATIONSKONSTANTE,
                    SAETTIGUNGSWASSERGEHALT, NAME, LASTMODIFIED, KOMMENTAR,  ID)
                  SELECT
                    {infiltrationsrateanfang}, {infiltrationsrateende},
                    {infiltrationsratestart}, {rueckgangskonstante}, {regenerationskonstante},
                    {saettigungswassergehalt}, '{name}', '{lastmodified}', '{kommentar}', {id}
                    FROM RDB$DATABASE
                    WHERE '{name}' NOT IN (SELECT NAME FROM BODENKLASSE);
                    """.format(
                    infiltrationsrateanfang=infiltrationsrateanfang,
                    infiltrationsrateende=infiltrationsrateende,
                    infiltrationsratestart=infiltrationsratestart,
                    rueckgangskonstante=rueckgangskonstante,
                    regenerationskonstante=regenerationskonstante,
                    saettigungswassergehalt=saettigungswassergehalt,
                    name=bknam,
                    lastmodified=createdat,
                    kommentar=kommentar,
                    id=nextid,
                )

                if not dbHE.sql(sql, "dbHE: export_bodenklassen (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Bodenklassen eingefuegt".format(nextid - nr0), 0.62)
    progress_bar.setValue(80)

    # --------------------------------------------------------------------------------------------
    # Export der Abflussparameter

    if (
        check_export["export_abflussparameter"]
        or check_export["modify_abflussparameter"]
    ):

        sql = """
            SELECT
                apnam,
                anfangsabflussbeiwert AS anfangsabflussbeiwert_t,
                endabflussbeiwert AS endabflussbeiwert_t,
                benetzungsverlust AS benetzungsverlust_t,
                muldenverlust AS muldenverlust_t,
                benetzung_startwert AS benetzung_startwert_t,
                mulden_startwert AS mulden_startwert_t,
                bodenklasse, kommentar, createdat
            FROM abflussparameter
            """.format(
            auswahl
        )

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_abflussparameter"):
            del dbHE
            return False

        nr0 = nextid

        fortschritt("Export Abflussparameter...", 0.7)

        for attr in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            (
                apnam,
                anfangsabflussbeiwert_t,
                endabflussbeiwert_t,
                benetzungsverlust_t,
                muldenverlust_t,
                benetzung_startwert_t,
                mulden_startwert_t,
                bodenklasse,
                kommentar,
                createdat_t,
            ) = ("NULL" if el is None else el for el in attr)

            # Formatierung der Zahlen
            (
                anfangsabflussbeiwert,
                endabflussbeiwert,
                benetzungsverlust,
                muldenverlust,
                benetzung_startwert,
                mulden_startwert,
            ) = (
                "NULL" if tt == "NULL" else "{:.2f}".format(float(tt))
                for tt in (
                    anfangsabflussbeiwert_t,
                    endabflussbeiwert_t,
                    benetzungsverlust_t,
                    muldenverlust_t,
                    benetzung_startwert_t,
                    mulden_startwert_t,
                )
            )

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            if bodenklasse == "NULL":
                typ = 0  # undurchlässig
                bodenklasse = ""
            else:
                typ = 1  # durchlässig

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_abflussparameter"]:
                sql = """
                  UPDATE ABFLUSSPARAMETER SET
                  ABFLUSSBEIWERTANFANG={anfangsabflussbeiwert},
                  ABFLUSSBEIWERTENDE={endabflussbeiwert}, BENETZUNGSVERLUST={benetzungsverlust},
                  MULDENVERLUST={muldenverlust}, BENETZUNGSPEICHERSTART={benetzung_startwert},
                  MULDENAUFFUELLGRADSTART={mulden_startwert},
                  SPEICHERKONSTANTEKONSTANT={speicherkonstantekonstant},
                  SPEICHERKONSTANTEMIN={speicherkonstantemin},
                  SPEICHERKONSTANTEMAX={speicherkonstantemax},
                  SPEICHERKONSTANTEKONSTANT2={speicherkonstantekonstant2},
                  SPEICHERKONSTANTEMIN2={speicherkonstantemin2}, SPEICHERKONSTANTEMAX2={speicherkonstantemax2},
                  BODENKLASSE='{bodenklasse}', CHARAKTERISTISCHEREGENSPENDE={charakteristischeregenspende},
                  CHARAKTERISTISCHEREGENSPENDE2={charakteristischeregenspende2},
                  TYP={typ}, JAHRESGANGVERLUSTE={jahresgangverluste}, LASTMODIFIED='{createdat}',
                  KOMMENTAR='{kommentar}'
                  WHERE NAME = '{apnam}';
                """.format(
                    apnam=apnam,
                    anfangsabflussbeiwert=anfangsabflussbeiwert,
                    endabflussbeiwert=endabflussbeiwert,
                    benetzungsverlust=benetzungsverlust,
                    muldenverlust=muldenverlust,
                    benetzung_startwert=benetzung_startwert,
                    mulden_startwert=mulden_startwert,
                    speicherkonstantekonstant=1,
                    speicherkonstantemin=0,
                    speicherkonstantemax=0,
                    speicherkonstantekonstant2=1,
                    speicherkonstantemin2=0,
                    speicherkonstantemax2=0,
                    bodenklasse=bodenklasse,
                    charakteristischeregenspende=0,
                    charakteristischeregenspende2=0,
                    typ=typ,
                    jahresgangverluste=0,
                    createdat=createdat,
                    kommentar=kommentar,
                )

                if not dbHE.sql(sql, "dbHE: export_abflussparameter (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_abflussparameter"]:
                sql = """
                  INSERT INTO ABFLUSSPARAMETER
                  ( NAME, ABFLUSSBEIWERTANFANG, ABFLUSSBEIWERTENDE, BENETZUNGSVERLUST,
                    MULDENVERLUST, BENETZUNGSPEICHERSTART, MULDENAUFFUELLGRADSTART, SPEICHERKONSTANTEKONSTANT,
                    SPEICHERKONSTANTEMIN, SPEICHERKONSTANTEMAX, SPEICHERKONSTANTEKONSTANT2,
                    SPEICHERKONSTANTEMIN2, SPEICHERKONSTANTEMAX2,
                    BODENKLASSE, CHARAKTERISTISCHEREGENSPENDE, CHARAKTERISTISCHEREGENSPENDE2,
                    TYP, JAHRESGANGVERLUSTE, LASTMODIFIED, KOMMENTAR, ID)
                  SELECT
                    '{apnam}', {anfangsabflussbeiwert}, {endabflussbeiwert}, {benetzungsverlust},
                    {muldenverlust}, {benetzung_startwert}, {mulden_startwert}, {speicherkonstantekonstant},
                    {speicherkonstantemin}, {speicherkonstantemax}, {speicherkonstantekonstant2},
                    {speicherkonstantemin2}, {speicherkonstantemax2},
                    '{bodenklasse}', {charakteristischeregenspende}, {charakteristischeregenspende2},
                    {typ}, {jahresgangverluste}, '{createdat}', '{kommentar}', {id}
                  FROM RDB$DATABASE
                  WHERE '{apnam}' NOT IN (SELECT NAME FROM ABFLUSSPARAMETER);
                """.format(
                    apnam=apnam,
                    anfangsabflussbeiwert=anfangsabflussbeiwert,
                    endabflussbeiwert=endabflussbeiwert,
                    benetzungsverlust=benetzungsverlust,
                    muldenverlust=muldenverlust,
                    benetzung_startwert=benetzung_startwert,
                    mulden_startwert=mulden_startwert,
                    speicherkonstantekonstant=1,
                    speicherkonstantemin=0,
                    speicherkonstantemax=0,
                    speicherkonstantekonstant2=1,
                    speicherkonstantemin2=0,
                    speicherkonstantemax2=0,
                    bodenklasse=bodenklasse,
                    charakteristischeregenspende=0,
                    charakteristischeregenspende2=0,
                    typ=typ,
                    jahresgangverluste=0,
                    createdat=createdat,
                    kommentar=kommentar,
                    id=nextid,
                )

                if not dbHE.sql(sql, "dbHE: export_abflussparameter (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Abflussparameter eingefuegt".format(nextid - nr0), 0.65)
    progress_bar.setValue(85)

    # ------------------------------------------------------------------------------------------------
    # Export der Regenschreiber
    #
    # Wenn in QKan keine Regenschreiber eingetragen sind, wird als Name "Regenschreiber1" angenommen.

    if check_export["export_regenschreiber"] or check_export["modify_regenschreiber"]:

        # # Pruefung, ob Regenschreiber fuer Export vorhanden
        # if len(liste_teilgebiete) != 0:
        #     auswahl = " AND flaechen.teilgebiet in ('{}')".format("', '".join(liste_teilgebiete))
        # else:
        #     auswahl = ""
        #
        # sql = "SELECT regenschreiber FROM flaechen GROUP BY regenschreiber{}".format(auswahl)

        # Regenschreiber berücksichtigen nicht ausgewählte Teilgebiete
        sql = """SELECT regenschreiber FROM flaechen GROUP BY regenschreiber"""

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_regenschreiber"):
            del dbHE
            return False

        attr = dbQK.fetchall()
        if attr == [(None,)]:
            reglis = tuple(["Regenschreiber1"])
            logger.debug(
                'In QKan war kein Regenschreiber vorhanden. "Regenschreiber1" ergänzt'
            )
        else:
            reglis = tuple([str(el[0]) for el in attr])
            logger.debug(
                "In QKan wurden folgende Regenschreiber referenziert: {}".format(
                    str(reglis)
                )
            )

        logger.debug("Regenschreiber - reglis: {}".format(str(reglis)))

        # Liste der fehlenden Regenschreiber in der Ziel- (*.idbf-) Datenbank
        # Hier muss eine Besonderheit von tuple berücksichtigt werden. Ein Tuple mit einem Element
        # endet mit ",)", z.B. (1,), während ohne oder bei mehr als einem Element alles wie üblich
        # ist: () oder (1,2,3,4)
        if len(reglis) == 1:
            sql = """SELECT NAME FROM REGENSCHREIBER WHERE NAME NOT IN {})""".format(
                str(reglis)[:-2]
            )
        else:
            sql = """SELECT NAME FROM REGENSCHREIBER WHERE NAME NOT IN {}""".format(
                str(reglis)
            )

        if not dbHE.sql(sql, "dbHE: export_regenschreiber (1)"):
            del dbHE
            return False

        attr = dbHE.fetchall()
        logger.debug("Regenschreiber - attr: {}".format(str(attr)))

        nr0 = nextid

        createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())

        regschnr = 1
        for regenschreiber in reglis:
            if regenschreiber not in attr:
                sql = """
                  INSERT INTO REGENSCHREIBER
                  ( NUMMER, STATION,
                    XKOORDINATE, YKOORDINATE, ZKOORDINATE, NAME,
                    FLAECHEGESAMT, FLAECHEDURCHLAESSIG, FLAECHEUNDURCHLAESSIG,
                    ANZAHLHALTUNGEN, INTERNENUMMER,
                    LASTMODIFIED, KOMMENTAR, ID) SELECT
                      {nummer}, '{station}',
                      {xkoordinate}, {ykoordinate}, {zkoordinate}, '{name}',
                      {flaechegesamt}, {flaechedurchlaessig}, {flaecheundurchlaessig},
                      {anzahlhaltungen}, {internenummer},
                      '{lastmodified}', '{kommentar}', {id}
                    FROM RDB$DATABASE
                    WHERE '{name}' NOT IN (SELECT NAME FROM REGENSCHREIBER);
                  """.format(
                    nummer=regschnr,
                    station=10000 + regschnr,
                    xkoordinate=0,
                    ykoordinate=0,
                    zkoordinate=0,
                    name=regenschreiber,
                    flaechegesamt=0,
                    flaechedurchlaessig=0,
                    flaecheundurchlaessig=0,
                    anzahlhaltungen=0,
                    internenummer=0,
                    lastmodified=createdat,
                    kommentar="Ergänzt durch QKan",
                    id=nextid,
                )

                if not dbHE.sql(sql, "dbHE: export_regenschreiber (2)"):
                    del dbHE
                    return False

                logger.debug("In HE folgenden Regenschreiber ergänzt: {}".format(sql))

                nextid += 1
        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Regenschreiber eingefuegt".format(nextid - nr0), 0.68)
    progress_bar.setValue(90)

    # ------------------------------------------------------------------------------------------------
    # Export der Flächen

    if check_export["export_flaechenrw"] or check_export["modify_flaechenrw"]:
        """
        Export der Flaechendaten

        Die Daten werden in max. drei Teilen nach HYSTEM-EXTRAN exportiert:
        1. Befestigte Flächen
        2.2 Unbefestigte Flächen

        Die Abflusseigenschaften werden über die Tabelle "abflussparameter" geregelt. Dort ist 
        im attribut bodenklasse nur bei unbefestigten Flächen ein Eintrag. Dies ist das Kriterium
        zur Unterscheidung

        undurchlässigen Flächen -------------------------------------------------------------------------------

        Es gibt in HYSTEM-EXTRAN 3 Flächentypen (BERECHNUNGSPEICHERKONSTANTE):
        verwendete Parameter:    Anz_Sp  SpKonst.  Fz_SschwP  Fz_Oberfl  Fz_Kanal
        0 - direkt                 x       x
        1 - Fließzeiten                                          x          x
        2 - Schwerpunktfließzeit                       x

        In der QKan-Datenbank stehen diese Parameter in der Tabelle "linkfl" 
        und sind Fz_SschwP und Fz_oberfl zu einem Feld zusammengefasst (fliesszeitflaeche)

        Befestigte Flächen"""

        # Vorbereitung flaechen: Falls flnam leer ist, plausibel ergänzen:
        if not checknames(dbQK, "flaechen", "flnam", "f_", autokorrektur):
            del dbHE
            return False

        if not updatelinkfl(dbQK, fangradius):
            del dbHE  # Im Fehlerfall wird dbQK in updatelinkfl geschlossen.
            fehlermeldung(
                "Fehler beim Update der Flächen-Verknüpfungen",
                "Der logische Cache konnte nicht aktualisiert werden.",
            )
            return False

        # Zu verschneidende zusammen mit nicht zu verschneidene Flächen exportieren

        # Nur Daten fuer ausgewaehlte Teilgebiete
        if len(liste_teilgebiete) != 0:
            auswahl_c = " AND ha.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
            auswahl_a = " WHERE ha.teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl_c = ""
            auswahl_a = ""

        # Verschneidung nur, wenn (mit_verschneidung)
        if mit_verschneidung:
            case_verschneidung = "fl.aufteilen IS NULL or fl.aufteilen <> 'ja'"
            join_verschneidung = """
                LEFT JOIN tezg AS tg
                ON lf.tezgnam = tg.flnam"""
            expr_verschneidung = """CastToMultiPolygon(CollectionExtract(intersection(fl.geom,tg.geom),3))"""
        else:
            case_verschneidung = "1"
            join_verschneidung = ""
            expr_verschneidung = "fl.geom"  # dummy

        # Einfuegen der Flächendaten in die QKan-Datenbank, Tabelle "flaechen_he8"

        if exportFlaechenHE8:

            # Vorbereitung: Leeren der Tabelle "flaechen_he8"

            sql = "DELETE FROM flaechen_he8"
            if not dbQK.sql(sql, "dbQK: export_to_he7.exportFlaechenHE8.delete"):
                del dbHE
                return False

                # Einfügen aller verschnittenen Flächen in Tabelle "flaechen_he8", zusammengefasst
                # nach Haltungen, Regenschreiber, etc.

                sql = """
                WITH flintersect AS (
                  SELECT 
                    substr(printf('%s-%d', fl.flnam, lf.pk),1,30) AS flnam, 
                    ha.haltnam AS haltnam, fl.neigkl AS neigkl,
                    at.he_nr AS abflusstyp, 
                    CASE WHEN ap.bodenklasse IS NULL THEN 0 ELSE 1 END AS typbef, 
                    lf.speicherzahl AS speicherzahl, lf.speicherkonst AS speicherkonst,
                    lf.fliesszeitflaeche AS fliesszeitflaeche, lf.fliesszeitkanal AS fliesszeitkanal,
                    CASE WHEN {case_verschneidung} THEN area(fl.geom)/10000 
                    ELSE area({expr_verschneidung})/10000 
                    END AS flaeche, 
                    fl.regenschreiber AS regenschreiber, ft.he_nr AS flaechentypnr, 
                    fl.abflussparameter AS abflussparameter, fl.createdat AS createdat,
                    fl.kommentar AS kommentar,
                    CASE WHEN {case_verschneidung} THEN fl.geom
                    ELSE {expr_verschneidung} 
                    END AS geom
                  FROM linkfl AS lf
                  INNER JOIN flaechen AS fl
                  ON lf.flnam = fl.flnam
                  INNER JOIN haltungen AS ha
                  ON lf.haltnam = ha.haltnam
                  LEFT JOIN abflusstypen AS at
                  ON lf.abflusstyp = at.abflusstyp
                  LEFT JOIN abflussparameter AS ap
                  ON fl.abflussparameter = ap.apnam
                  LEFT JOIN flaechentypen AS ft
                  ON ap.flaechentyp = ft.bezeichnung{join_verschneidung}{auswahl_a})
                INSERT INTO flaechen_he8 (
                  Name, Haltung, Groesse, Regenschreiber, Flaechentyp, 
                  BerechnungSpeicherkonstante, Typ, AnzahlSpeicher,
                  Speicherkonstante, 
                  Schwerpunktlaufzeit,
                  FliesszeitOberflaeche, LaengsteFliesszeitKanal,
                  Parametersatz, Neigungsklasse, ZuordnUnabhEZG, 
                  IstPolygonalflaeche, ZuordnungGesperrt, 
                  LastModified,
                  Kommentar, 
                  Geometry)
                SELECT 
                  flnam AS Name, haltnam AS Haltung, flaeche AS Groesse, regenschreiber AS Regenschreiber, 
                  flaechentypnr AS Flaechentyp, 
                  abflusstyp AS BerechnungSpeicherkonstante, typbef AS Typ, speicherzahl AS AnzahlSpeicher, 
                  speicherkonst AS Speicherkonstante, 
                  coalesce(fliesszeitflaeche, 0.0) AS Schwerpunktlaufzeit, 
                  fliesszeitflaeche AS FliesszeitOberflaeche, fliesszeitkanal AS LaengsteFliesszeitKanal, 
                  abflussparameter AS Parametersatz, neigkl AS Neigungsklasse, 
                  1 AS IstPolygonalflaeche, 1 AS ZuordnungGesperrt, 0 AS ZuordnUnabhEZG, 
                  strftime('%Y-%m-%d %H:%M:%S', coalesce(createdat, 'now')) AS lastmodified, 
                  kommentar AS Kommentar, 
                  SetSrid(geom, -1) AS Geometry
                FROM flintersect AS fi
                WHERE flaeche*10000 > {mindestflaeche}""".format(
                    mindestflaeche=mindestflaeche,
                    auswahl_a=auswahl_a,
                    case_verschneidung=case_verschneidung,
                    join_verschneidung=join_verschneidung,
                    expr_verschneidung=expr_verschneidung,
                )

            logger.debug(
                "Abfrage zum Export der Flächendaten nach HE8: \n{}".format(sql)
            )

            if not dbQK.sql(sql, "dbQK: export_to_he7.export_flaechenhe8"):
                del dbHE
                return False

        # Abfragen zum Export in die HE-7-Firebird-Datenbank. Da der Datentransfer innerhalb einer
        # Abfrage nicht möglich ist, werden die Inhalte datensatzweise übertragen.

        if check_export["combine_flaechenrw"]:
            sql = """
              WITH flintersect AS (
                SELECT lf.flnam AS flnam, lf.pk AS pl, lf.haltnam AS haltnam, fl.neigkl AS neigkl, lf.abflusstyp AS abflusstyp, 
                  lf.speicherzahl AS speicherzahl, lf.speicherkonst AS speicherkonst,
                  lf.fliesszeitflaeche AS fliesszeitflaeche, lf.fliesszeitkanal AS fliesszeitkanal,
                  fl.regenschreiber AS regenschreiber,
                  fl.abflussparameter AS abflussparameter, fl.createdat AS createdat,
                  fl.kommentar AS kommentar, 
                  CASE WHEN {case_verschneidung} THEN fl.geom 
                  ELSE {expr_verschneidung} END AS geom
                FROM linkfl AS lf
                INNER JOIN flaechen AS fl
                ON lf.flnam = fl.flnam{join_verschneidung})
              SELECT substr(printf('%s-%d', fi.flnam, fi.pl),1,30) AS flnam, 
                ha.haltnam AS haltnam, fi.neigkl AS neigkl,
                fi.abflusstyp AS abflusstyp, fi.speicherzahl AS speicherzahl, avg(fi.speicherkonst) AS speicherkonst,
                max(fi.fliesszeitflaeche) AS fliesszeitflaeche, max(fi.fliesszeitkanal) AS fliesszeitkanal,
                sum(area(fi.geom)/10000) AS flaeche, fi.regenschreiber AS regenschreiber,
                abflussparameter AS abflussparameter, max(fi.createdat) AS createdat,
                max(fi.kommentar) AS kommentar
              FROM flintersect AS fi
              INNER JOIN haltungen AS ha
              ON fi.haltnam = ha.haltnam
              WHERE area(fi.geom) > {mindestflaeche}{auswahl_c}
              GROUP BY ha.haltnam, fi.abflussparameter, fi.regenschreiber, fi.speicherzahl, 
                fi.abflusstyp, fi.neigkl""".format(
                mindestflaeche=mindestflaeche,
                auswahl_c=auswahl_c,
                case_verschneidung=case_verschneidung,
                join_verschneidung=join_verschneidung,
                expr_verschneidung=expr_verschneidung,
            )
            logger.debug("combine_flaechenrw = True")
            logger.debug("Abfrage zum Export der Flächendaten: \n{}".format(sql))
        else:
            sql = """
              WITH flintersect AS (
                SELECT substr(printf('%s-%d', fl.flnam, lf.pk),1,30) AS flnam, 
                  ha.haltnam AS haltnam, fl.neigkl AS neigkl,
                  lf.abflusstyp AS abflusstyp, lf.speicherzahl AS speicherzahl, lf.speicherkonst AS speicherkonst,
                  lf.fliesszeitflaeche AS fliesszeitflaeche, lf.fliesszeitkanal AS fliesszeitkanal,
                  CASE WHEN {case_verschneidung} THEN area(fl.geom)/10000 
                  ELSE area({expr_verschneidung})/10000 END AS flaeche, 
                  fl.regenschreiber AS regenschreiber,
                  fl.abflussparameter AS abflussparameter, fl.createdat AS createdat,
                  fl.kommentar AS kommentar
                FROM linkfl AS lf
                INNER JOIN flaechen AS fl
                ON lf.flnam = fl.flnam
                INNER JOIN haltungen AS ha
                ON lf.haltnam = ha.haltnam{join_verschneidung}{auswahl_a})
              SELECT flnam, haltnam, neigkl, abflusstyp, speicherzahl, speicherkonst, 
              fliesszeitflaeche, fliesszeitkanal, flaeche, regenschreiber, abflussparameter,
              createdat, kommentar
              FROM flintersect AS fi
              WHERE flaeche*10000 > {mindestflaeche}""".format(
                mindestflaeche=mindestflaeche,
                auswahl_a=auswahl_a,
                case_verschneidung=case_verschneidung,
                join_verschneidung=join_verschneidung,
                expr_verschneidung=expr_verschneidung,
            )
            logger.debug("combine_flaechenrw = False")
            logger.debug("Abfrage zum Export der Flächendaten: \n{}".format(sql))

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_flaechenrw (4)"):
            del dbHE
            return False

        fortschritt("Export befestigte Flaechen...", 0.70)

        nr0 = nextid

        fehler_abflusstyp = False  # Um wiederholte Fehlermeldung zu unterdrücken...

        for attr in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            (
                flnam,
                haltnam,
                neigkl,
                abflusstyp,
                speicherzahl,
                speicherkonst,
                fliesszeitflaeche,
                fliesszeitkanal,
                flaeche,
                regenschreiber,
                abflussparameter,
                createdat_t,
                kommentar,
            ) = ("NULL" if el is None else el for el in attr)

            # Datenkorrekturen
            if regenschreiber == "NULL":
                regenschreiber = "Regenschreiber1"

            if abflusstyp in he_fltyp_ref:
                he_typ = he_fltyp_ref[abflusstyp]
            elif abflusstyp == "NULL":
                he_typ = 0  # Flächentyp 'Direkt'
            else:
                if not fehler_abflusstyp:
                    meldung(
                        'Datenfehler in Tabelle "flaechen", Feld "abflusstyp"',
                        "Wert: {}".format(abflusstyp),
                    )
                    he_typ = 0  # Flächentyp 'Direkt'
                    fehler_abflusstyp = True

            if flaeche != "NULL":
                flaeche = "{0:.4f}".format(flaeche)

            if neigkl != "NULL":
                neigkl = "{0:.0f}".format(neigkl)
            else:
                neigkl = 0

            if speicherzahl != "NULL":
                speicherzahl = "{0:.0f}".format(speicherzahl)
            else:
                speicherzahl = "0"

            if speicherkonst != "NULL":
                speicherkonst = "{0:.3f}".format(speicherkonst)
            else:
                speicherkonst = "0"

            if fliesszeitflaeche != "NULL":
                fliesszeitflaeche = "{0:.2f}".format(fliesszeitflaeche)
            else:
                fliesszeitflaeche = "0"

            if fliesszeitkanal != "NULL":
                fliesszeitkanal = "{0:.2f}".format(fliesszeitkanal)
            else:
                fliesszeitkanal = "0"

            # Feld "fliesszeitflaeche" in QKan entspricht je nach he_typ zwei
            # unterschiedlichen Feldern in HE, s.o. Deshalb wird dieser Wert
            # einfach in beide Felder geschrieben (bis Version 3.0.1: getrennt)

            fliesszeitoberfl = fliesszeitflaeche
            fliesszeitschwerp = fliesszeitflaeche

            # Standardwerte, falls keine Vorgaben
            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            if kommentar == "NULL" or kommentar == "":
                kommentar = "eingefuegt von export_to_he7"

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_flaechenrw"]:
                sql = """
                  UPDATE FLAECHE SET
                  GROESSE={flaeche}, REGENSCHREIBER='{regenschreiber}', HALTUNG='{haltnam}',
                  BERECHNUNGSPEICHERKONSTANTE={he_typ}, TYP={fltyp}, ANZAHLSPEICHER={speicherzahl},
                  SPEICHERKONSTANTE={speicherkonst}, SCHWERPUNKTLAUFZEIT={fliesszeitschwerp},
                  FLIESSZEITOBERFLAECHE={fliesszeitoberfl}, LAENGSTEFLIESSZEITKANAL={fliesszeitkanal},
                  PARAMETERSATZ='{abflussparameter}', NEIGUNGSKLASSE={neigkl},
                  LASTMODIFIED='{createdat}',
                  KOMMENTAR='{kommentar}', ZUORDNUNABHEZG={zuordnunabhezg}
                  WHERE NAME = '{flnam}';
                  """.format(
                    flaeche=flaeche,
                    regenschreiber=regenschreiber,
                    haltnam=haltnam,
                    he_typ=he_typ,
                    fltyp=0,
                    speicherzahl=speicherzahl,
                    speicherkonst=speicherkonst,
                    fliesszeitschwerp=fliesszeitschwerp,
                    fliesszeitoberfl=fliesszeitoberfl,
                    fliesszeitkanal=fliesszeitkanal,
                    abflussparameter=abflussparameter,
                    neigkl=neigkl,
                    flnam=flnam,
                    createdat=createdat,
                    kommentar=kommentar,
                    zuordnunabhezg=0,
                )

                if not dbHE.sql(sql, "dbHE: export_flaechenrw (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_flaechenrw"]:
                sql = """
                  INSERT INTO FLAECHE
                  ( GROESSE, REGENSCHREIBER, HALTUNG,
                    BERECHNUNGSPEICHERKONSTANTE, TYP, ANZAHLSPEICHER,
                    SPEICHERKONSTANTE, SCHWERPUNKTLAUFZEIT,
                    FLIESSZEITOBERFLAECHE, LAENGSTEFLIESSZEITKANAL,
                    PARAMETERSATZ, NEIGUNGSKLASSE,
                    NAME, LASTMODIFIED,
                    KOMMENTAR, ID, ZUORDNUNABHEZG)
                  SELECT
                    {flaeche}, '{regenschreiber}', '{haltnam}',
                    {he_typ}, {fltyp}, {speicherzahl},
                    {speicherkonst}, {fliesszeitschwerp},
                    {fliesszeitoberfl}, {fliesszeitkanal},
                    '{abflussparameter}', {neigkl},
                    '{flnam}', '{createdat}',
                    '{kommentar}', {nextid}, {zuordnunabhezg}
                  FROM RDB$DATABASE
                  WHERE '{flnam}' NOT IN (SELECT NAME FROM FLAECHE);
                  """.format(
                    flaeche=flaeche,
                    regenschreiber=regenschreiber,
                    haltnam=haltnam,
                    he_typ=he_typ,
                    fltyp=0,
                    speicherzahl=speicherzahl,
                    speicherkonst=speicherkonst,
                    fliesszeitschwerp=fliesszeitschwerp,
                    fliesszeitoberfl=fliesszeitoberfl,
                    fliesszeitkanal=fliesszeitkanal,
                    abflussparameter=abflussparameter,
                    neigkl=neigkl,
                    flnam=flnam,
                    createdat=createdat,
                    kommentar=kommentar,
                    nextid=nextid,
                    zuordnunabhezg=0,
                )

                # logger.debug('Abfrage zum Export der Flächendaten in die ITWH-DB: \n{}'.format(sql))
                if not dbHE.sql(sql, "dbHE: export_flaechenrw (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Flaechen eingefuegt".format(nextid - nr0), 0.80)
    progress_bar.setValue(90)

    # ------------------------------------------------------------------------------------------------
    # Export der Direkteinleitungen

    if check_export["export_einleitdirekt"] or check_export["modify_einleitdirekt"]:
        # Herkunft = 1 (Direkt) und 3 (Einwohnerbezogen)

        """
        Bearbeitung in QKan: Vervollständigung der Einzugsgebiete

        Prüfung der vorliegenden Einzugsgebiete in QKan
        ============================================
        Zunächst eine grundsätzliche Anmerkung: In HE gibt es keine Einzugsgebiete in der Form, wie sie
        in QKan vorhanden sind. Diese werden (nur) in QKan verwendet, um für die Variante 
        Herkunft = 3 die Grundlagendaten
         - einwohnerspezifischer Schmutzwasseranfall
         - Fremdwasseranteil
         - Stundenmittel
        zu verwalten.

        Aus diesem Grund werden vor dem Export der Einzeleinleiter diese Daten geprüft:

        1 Wenn in QKan keine Einzugsgebiete vorhanden sind, wird zunächst geprüft, ob die
           Einwohnerpunkte einem (noch nicht angelegten) Einzugsgebiet zugeordnet sind.
           1.1 Kein Einwohnerpunkt ist einem Einzugsgebiet zugeordnet. Dann wird ein Einzugsgebiet "Einzugsgebiet1" 
               angelegt und alle Einwohnerpunkte diesem Einzugsgebiet zugeordnet
           1.2 Die Einwohnerpunkte sind einem oder mehreren noch nicht in der Tabelle "einzugsgebiete" vorhandenen 
               Einzugsgebieten zugeordnet. Dann werden entsprechende Einzugsgebiete mit Standardwerten angelegt.
        2 Wenn in QKan Einzugsgebiete vorhanden sind, wird geprüft, ob es auch Einwohnerpunkte gibt, die diesen
           Einzugsgebieten zugeordnet sind.
           2.1 Es gibt keine Einwohnerpunkte, die einem Einzugsgebiet zugeordnet sind.
               2.1.1 Es gibt in QKan genau ein Einzugsgebiet. Dann werden alle Einwohnerpunkte diesem Einzugsgebiet
                     zugeordnet.
               2.1.2 Es gibt in QKan mehrere Einzugsgebiete. Dann werden alle Einwohnerpunkte geographisch dem
                     betreffenden Einzugsgebiet zugeordnet.
           2.2 Es gibt mindestens einen Einwohnerpunkt, der einem Einzugsgebiet zugeordnet ist.
               Dann wird geprüft, ob es noch nicht zugeordnete Einwohnerpunkte gibt, eine Warnung angezeigt und
               diese Einwohnerpunkte aufgelistet.
        """

        sql = "SELECT count(*) AS anz FROM einzugsgebiete"

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (1)"):
            del dbHE
            return False

        anztgb = int(dbQK.fetchone()[0])
        if anztgb == 0:
            # 1 Kein Einzugsgebiet in QKan -----------------------------------------------------------------
            createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())

            sql = """
                SELECT count(*) AS anz FROM einleit WHERE
                (einzugsgebiet is not NULL) AND
                (einzugsgebiet <> 'NULL') AND
                (einzugsgebiet <> '')
            """

            if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (2)"):
                del dbHE
                return False

            anz = int(dbQK.fetchone()[0])
            if anz == 0:
                # 1.1 Kein Einwohnerpunkt mit Einzugsgebiet ----------------------------------------------------
                sql = """
                   INSERT INTO einzugsgebiete
                   ( tgnam, ewdichte, wverbrauch, stdmittel,
                     fremdwas, createdat, kommentar)
                   Values
                   ( 'einzugsgebiet1', 60, 120, 14, 100, '{createdat}',
                     'Automatisch durch  QKan hinzugefuegt')""".format(
                    createdat=createdat
                )

                if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (3)"):
                    del dbHE
                    return False

                dbQK.commit()
            else:
                # 1.2 Einwohnerpunkte mit Einzugsgebiet ----------------------------------------------------
                # Liste der in allen Einwohnerpunkten vorkommenden Einzugsgebiete
                sql = """SELECT einzugsgebiet FROM einleit WHERE einzugsgebiet is not NULL GROUP BY einzugsgebiet"""

                if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (4)"):
                    del dbHE
                    return False

                listeilgeb = dbQK.fetchall()
                for tgb in listeilgeb:
                    sql = """
                       INSERT INTO einzugsgebiete
                       ( tgnam, ewdichte, wverbrauch, stdmittel,
                         fremdwas, createdat, kommentar)
                       Values
                       ( '{tgnam}', 60, 120, 14, 100, '{createdat}',
                         'Hinzugefuegt aus QKan')""".format(
                        tgnam=tgb[0], createdat=createdat
                    )

                    if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (5)"):
                        del dbHE
                        return False

                    dbQK.commit()
                    meldung(
                        "Tabelle 'einzugsgebiete':\n",
                        "Es wurden {} Einzugsgebiete hinzugefügt".format(len(tgb)),
                    )

                # Kontrolle mit Warnung
                sql = """
                    SELECT count(*) AS anz
                    FROM einleit
                    LEFT JOIN einzugsgebiete ON einleit.einzugsgebiet = einzugsgebiete.tgnam
                    WHERE einzugsgebiete.pk IS NULL
                """

                if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (6)"):
                    del dbHE
                    return False

                anz = int(dbQK.fetchone()[0])
                if anz > 0:
                    meldung(
                        "Fehlerhafte Daten in Tabelle 'einleit':",
                        "{} Einleitpunkte sind keinem Einzugsgebiet zugeordnet".format(
                            anz
                        ),
                    )
        else:
            # 2 Einzugsgebiete in QKan ----------------------------------------------------
            sql = """
                SELECT count(*) AS anz
                FROM einleit
                INNER JOIN einzugsgebiete ON einleit.einzugsgebiet = einzugsgebiete.tgnam
            """

            if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (7)"):
                del dbHE
                return False

            anz = int(dbQK.fetchone()[0])
            if anz == 0:
                # 2.1 Keine Einleitpunkte mit Einzugsgebiet ----------------------------------------------------
                if anztgb == 1:
                    # 2.1.1 Es existiert genau ein Einzugsgebiet ---------------------------------------------
                    sql = """UPDATE einleit SET einzugsgebiet = (SELECT tgnam FROM einzugsgebiete GROUP BY tgnam)"""

                    if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (8)"):
                        del dbHE
                        return False

                    dbQK.commit()
                    meldung(
                        "Tabelle 'einleit':\n",
                        "Alle Einleitpunkte in der Tabelle 'einleit' wurden einem Einzugsgebiet zugeordnet",
                    )
                else:
                    # 2.1.2 Es existieren mehrere Einzugsgebiete ------------------------------------------
                    sql = """UPDATE einleit SET einzugsgebiet = (SELECT tgnam FROM einzugsgebiete
                          WHERE within(einleit.geom, einzugsgebiete.geom) 
                              and einleit.geom IS NOT NULL and einzugsgebiete.geom IS NOT NULL)"""

                    if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (9)"):
                        del dbHE
                        return False

                    dbQK.commit()
                    meldung(
                        "Tabelle 'einleit':\n",
                        "Alle Einleitpunkte in der Tabelle 'einleit' wurden dem Einzugsgebiet zugeordnet, in dem sie liegen.",
                    )

                    # Kontrolle mit Warnung
                    sql = """
                        SELECT count(*) AS anz
                        FROM einleit
                        LEFT JOIN einzugsgebiete ON einleit.einzugsgebiet = einzugsgebiete.tgnam
                        WHERE einzugsgebiete.pk IS NULL
                    """
                    if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (10)"):
                        del dbHE
                        return False

                    anz = int(dbQK.fetchone()[0])
                    if anz > 0:
                        meldung(
                            "Fehlerhafte Daten in Tabelle 'einleit':",
                            "{} Einleitpunkte sind keinem Einzugsgebiet zugeordnet".format(
                                anz
                            ),
                        )
            else:
                # 2.2 Es gibt Einleitpunkte mit zugeordnetem Einzugsgebiet
                # Kontrolle mit Warnung
                sql = """
                    SELECT count(*) AS anz
                    FROM einleit
                    LEFT JOIN einzugsgebiete ON einleit.einzugsgebiet = einzugsgebiete.tgnam
                    WHERE einzugsgebiete.pk is NULL
                """

                if not dbQK.sql(sql, "dbQK: export_to_he7.export_einzugsgebiete (11)"):
                    del dbHE
                    return False

                anz = int(dbQK.fetchone()[0])
                if anz > 0:
                    meldung(
                        "Fehlerhafte Daten in Tabelle 'einleit':",
                        "{} Einleitpunkte sind keinem Einzugsgebiet zugeordnet".format(
                            anz
                        ),
                    )

        # --------------------------------------------------------------------------------------------
        # Export der Einzeleinleiter aus Schmutzwasser
        #
        # Referenzlisten (HE 7.8):
        #
        # ABWASSERART (Im Formular "Art"):
        #    0 = Häuslich
        #    1 = Gewerblich
        #    2 = Industriell
        #    5 = Regenwasser
        #
        # HERKUNFT (Im Formular "Herkunft"):
        #    0 = Siedlungstyp
        #    1 = Direkt
        #    2 = Frischwasserverbrauch
        #    3 = Einwohner
        #

        # HERKUNFT = 1 (Direkt) wird aus einer eigenen Tabelle "einleiter" erzeugt und ebenfalls in die
        # HE-Tabelle "Einzeleinleiter" übertragen
        #
        # HERKUNFT = 2 (Frischwasserverbrauch) ist zurzeit nicht realisiert
        #
        # Herkunft = 3 (Einwohner).
        # Nur die Flächen werden berücksichtigt, die einem Einzugsgebiet
        # mit Wasserverbrauch zugeordnet sind.

        # Vorbereitung einleit: Falls elnam leer ist, plausibel ergänzen:

        if not checknames(dbQK, "einleit", "elnam", "e_", autokorrektur):
            del dbHE
            return False

        if not updatelinksw(dbQK, fangradius):
            del dbHE  # Im Fehlerfall wird dbQK in updatelinksw geschlossen.
            fehlermeldung(
                "Fehler beim Update der Einzeleinleiter-Verknüpfungen",
                "Der logische Cache konnte nicht aktualisiert werden.",
            )
            return False

        # Nur Daten fuer ausgewaehlte Teilgebiete

        if len(liste_teilgebiete) != 0:
            auswahl = " and teilgebiet in ('{}')".format("', '".join(liste_teilgebiete))
        else:
            auswahl = ""

        if check_export["combine_einleitdirekt"]:
            sql = """SELECT
              elnam,
              avg(x(geom)) AS xel,
              avg(y(geom)) AS yel,
              haltnam AS haltnam,
              NULL AS wverbrauch, 
              NULL AS stdmittel,
              NULL AS fremdwas, 
              NULL AS einwohner,
              sum(zufluss) AS zuflussdirekt, 
              1 AS herkunft,
              einleit.createdat AS createdat
              FROM einleit
              WHERE zufluss IS NOT NULL {auswahl}
              GROUP BY haltnam
          UNION
              SELECT
              el.elnam AS elnam,
              avg(x(el.geom)) AS xel,
              avg(y(el.geom)) AS yel,
              el.haltnam AS haltnam,
              printf('%.6f',tg.wverbrauch) AS wverbrauch, 
              printf('%.1f',tg.stdmittel) AS stdmittel,
              printf('%.3f',tg.fremdwas) AS fremdwas, 
              printf('%.6f',el.ew) AS einwohner,
              NULL AS zuflussdirekt, 
              3 AS herkunft,
              el.createdat AS createdat
              FROM einleit AS el
              INNER JOIN einzugsgebiete AS tg
              ON el.einzugsgebiet = tg.tgnam
              WHERE zufluss IS NULL {auswahl}
              GROUP BY el.haltnam, 
                printf('%.6f',tg.wverbrauch), 
                printf('%.1f',tg.stdmittel),
                printf('%.3f',tg.fremdwas),
                printf('%.6f',el.ew)
            """.format(
                auswahl=auswahl
            )
        else:
            sql = """SELECT
              elnam,
              x(geom) AS xel,
              y(geom) AS yel,
              haltnam AS haltnam,
              NULL AS wverbrauch, 
              NULL AS stdmittel,
              NULL AS fremdwas, 
              NULL AS einwohner,
              zufluss AS zuflussdirekt, 
              1 AS herkunft,
              einleit.createdat AS createdat
              FROM einleit
              WHERE zufluss IS NOT NULL {auswahl}
          UNION
              SELECT
              el.elnam AS elnam,
              x(el.geom) AS xel,
              y(el.geom) AS yel,
              el.haltnam AS haltnam,
              tg.wverbrauch AS wverbrauch, 
              tg.stdmittel AS stdmittel,
              tg.fremdwas AS fremdwas, 
              el.ew AS einwohner,
              NULL AS zuflussdirekt, 
              3 AS herkunft,
              el.createdat AS createdat
              FROM einleit AS el
              INNER JOIN einzugsgebiete AS tg
              ON el.einzugsgebiet = tg.tgnam 
              WHERE zufluss IS NULL {auswahl}
            """.format(
                auswahl=auswahl
            )

        logger.debug("\nSQL-4e:\n{}\n".format(sql))

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_einleitdirekt (6)"):
            del dbHE
            return False

        nr0 = nextid

        fortschritt("Export Einzeleinleiter (direkt)...", 0.92)

        # Varianten abhängig von HE-Version
        if versionolder(heDBVersion[0:2], ["7", "9"], 2):
            logger.debug("Version vor 7.9 erkannt")
            fieldsnew = ""
            attrsnew = ""
            valuesnew = ""
        else:
            logger.debug("Version 7.9 erkannt")
            fieldsnew = ", ZUFLUSSOBERERSCHACHT = 0"
            attrsnew = ", ZUFLUSSOBERERSCHACHT"
            valuesnew = ", 0"

        for b in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            elnam, xel, yel, haltnam, wverbrauch, stdmittel, fremdwas, einwohner, zuflussdirekt, herkunft, createdat_t = (
                "NULL" if el is None else el for el in b
            )

            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_einleitdirekt"]:
                sql = """
                    UPDATE EINZELEINLEITER SET
                    XKOORDINATE={xel}, YKOORDINATE={yel}, ZUORDNUNGGESPERRT={zuordnunggesperrt}, 
                    ZUORDNUNABHEZG={zuordnunabhezg}, ROHR='{haltnam}',
                    ABWASSERART={abwasserart}, EINWOHNER={einwohner}, WASSERVERBRAUCH={wverbrauch}, 
                    HERKUNFT={herkunft},
                    STUNDENMITTEL={stdmittel}, FREMDWASSERZUSCHLAG={fremdwas}, FAKTOR={faktor}, 
                    GESAMTFLAECHE={flaeche},
                    ZUFLUSSMODELL={zuflussmodell}, ZUFLUSSDIREKT={zuflussdirekt}, 
                    ZUFLUSS={zufluss}, PLANUNGSSTATUS={planungsstatus},
                    ABRECHNUNGSZEITRAUM={abrechnungszeitraum}, ABZUG={abzug},
                    LASTMODIFIED='{createdat}'{fieldsnew}
                    WHERE NAME='{elnam}';
                    """.format(
                    xel=xel,
                    yel=yel,
                    zuordnunggesperrt=0,
                    zuordnunabhezg=1,
                    haltnam=haltnam,
                    abwasserart=0,
                    einwohner=einwohner,
                    wverbrauch=wverbrauch,
                    herkunft=herkunft,
                    stdmittel=stdmittel,
                    fremdwas=fremdwas,
                    faktor=1,
                    flaeche=0,
                    zuflussmodell=0,
                    zuflussdirekt=zuflussdirekt,
                    zufluss=0,
                    planungsstatus=0,
                    elnam=elnam[:27],
                    abrechnungszeitraum=365,
                    abzug=0,
                    createdat=createdat,
                    fieldsnew=fieldsnew,
                )

                if not dbHE.sql(sql, "dbHE: export_einleitdirekt (1)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_einleitdirekt"]:
                sql = """
                  INSERT INTO EINZELEINLEITER
                  ( XKOORDINATE, YKOORDINATE, ZUORDNUNGGESPERRT, ZUORDNUNABHEZG, ROHR,
                    ABWASSERART, EINWOHNER, WASSERVERBRAUCH, HERKUNFT,
                    STUNDENMITTEL, FREMDWASSERZUSCHLAG, FAKTOR, GESAMTFLAECHE,
                    ZUFLUSSMODELL, ZUFLUSSDIREKT, ZUFLUSS, PLANUNGSSTATUS, NAME,
                    ABRECHNUNGSZEITRAUM, ABZUG,
                    LASTMODIFIED, ID{attrsnew}) 
                  SELECT
                    {xel}, {yel}, {zuordnunggesperrt}, {zuordnunabhezg}, '{haltnam}',
                    {abwasserart}, {einwohner}, {wverbrauch}, {herkunft},
                    {stdmittel}, {fremdwas}, {faktor}, {flaeche},
                    {zuflussmodell}, {zuflussdirekt}, {zufluss}, {planungsstatus}, '{elnam}',
                    {abrechnungszeitraum}, {abzug},
                    '{createdat}', {nextid}{valuesnew}
                  FROM RDB$DATABASE
                  WHERE '{elnam}' NOT IN (SELECT NAME FROM EINZELEINLEITER);
              """.format(
                    xel=xel,
                    yel=yel,
                    zuordnunggesperrt=0,
                    zuordnunabhezg=1,
                    haltnam=haltnam,
                    abwasserart=0,
                    einwohner=einwohner,
                    wverbrauch=wverbrauch,
                    herkunft=herkunft,
                    stdmittel=stdmittel,
                    fremdwas=fremdwas,
                    faktor=1,
                    flaeche=0,
                    zuflussmodell=0,
                    zuflussdirekt=zuflussdirekt,
                    zufluss=0,
                    planungsstatus=0,
                    elnam=elnam[:27],
                    abrechnungszeitraum=365,
                    abzug=0,
                    createdat=createdat,
                    nextid=nextid,
                    attrsnew=attrsnew,
                    valuesnew=valuesnew,
                )

                if not dbHE.sql(sql, "dbHE: export_einleitdirekt (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Einzeleinleiter (direkt) eingefuegt".format(nextid - nr0), 0.95)

    # ------------------------------------------------------------------------------------------------
    # Export der Aussengebiete

    if check_export["export_aussengebiete"] or check_export["modify_aussengebiete"]:

        # Aktualisierung der Anbindungen, insbesondere wird der richtige Schacht in die
        # Tabelle "aussengebiete" eingetragen.

        if not updatelinkageb(dbQK, fangradius):
            del dbHE  # Im Fehlerfall wird dbQK in updatelinkageb geschlossen.
            fehlermeldung(
                "Fehler beim Update der Außengebiete-Verknüpfungen",
                "Der logische Cache konnte nicht aktualisiert werden.",
            )
            return False

        # Nur Daten fuer ausgewaehlte Teilgebiete

        if len(liste_teilgebiete) != 0:
            auswahl = " WHERE teilgebiet in ('{}')".format(
                "', '".join(liste_teilgebiete)
            )
        else:
            auswahl = ""

        sql = """SELECT
          gebnam,
          x(centroid(geom)) AS xel,
          y(centroid(geom)) AS yel,
          schnam,
          hoeheob, 
          hoeheun, 
          fliessweg, 
          area(geom)/10000 AS gesflaeche, 
          basisabfluss, 
          cn, 
          regenschreiber, 
          kommentar, 
          createdat
          FROM aussengebiete{auswahl}
        """.format(
            auswahl=auswahl
        )

        logger.debug("\nSQL-4e:\n{}\n".format(sql))

        if not dbQK.sql(sql, "dbQK: export_to_he7.export_aussengebiete (6)"):
            del dbHE
            return False

        nr0 = nextid

        fortschritt("Export Außengebiete...", 0.92)
        for b in dbQK.fetchall():

            # In allen Feldern None durch NULL ersetzen
            gebnam, xel, yel, schnam, hoeheob, hoeheun, fliessweg, gesflaeche, basisabfluss, cn, regenschreiber, kommentar, createdat_t = (
                "NULL" if el is None else el for el in b
            )

            if createdat_t == "NULL":
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", time.localtime())
            else:
                try:
                    if createdat_t.count(":") == 1:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M")
                    else:
                        createdat_s = time.strptime(createdat_t, "%d.%m.%Y %H:%M:%S")
                except:
                    createdat_s = time.localtime()
                createdat = time.strftime("%d.%m.%Y %H:%M:%S", createdat_s)

            # Ändern vorhandener Datensätze (geschickterweise vor dem Einfügen!)
            if check_export["modify_aussengebiete"]:
                sql = """
                    UPDATE AUSSENGEBIET SET
                    NAME='{gebnam}', SCHACHT='{schnam}', HOEHEOBEN={hoeheob}, 
                    HOEHEUNTEN={hoeheun}, XKOORDINATE={xel}, YKOORDINATE={yel}, 
                    GESAMTFLAECHE={gesflaeche}, CNMITTELWERT={cn}, BASISZUFLUSS={basisabfluss}, 
                    FLIESSLAENGE={fliessweg}, VERFAHREN={verfahren}, REGENSCHREIBER='{regenschreiber}', 
                    LASTMODIFIED='{createdat}', KOMMENTAR='{kommentar}'
                    WHERE NAME='{gebnam}';
                    """.format(
                    gebnam=gebnam,
                    schnam=schnam,
                    hoeheob=hoeheob,
                    hoeheun=hoeheun,
                    xel=xel,
                    yel=yel,
                    gesflaeche=gesflaeche,
                    cn=cn,
                    basisabfluss=basisabfluss,
                    fliessweg=fliessweg,
                    verfahren=0,
                    regenschreiber=regenschreiber,
                    createdat=createdat,
                    kommentar=kommentar,
                )

                if not dbHE.sql(sql, "dbHE: export_aussengebiete (1)"):
                    del dbHE
                    return False

                sql = """
                    UPDATE TABELLENINHALTE 
                    SET KEYWERT = {cn}, WERT = {gesflaeche}
                    WHERE ID = (SELECT ID FROM AUSSENGEBIET WHERE NAME = '{gebnam}')
                    """.format(
                    cn=cn, gesflaeche=gesflaeche, gebnam=gebnam
                )

                if not dbHE.sql(sql, "dbHE: export_aussengebiete (2)"):
                    del dbHE
                    return False

            # Einfuegen in die Datenbank
            if check_export["export_aussengebiete"]:
                sql = """
                    INSERT INTO AUSSENGEBIET
                    ( NAME, SCHACHT, HOEHEOBEN, 
                      HOEHEUNTEN, XKOORDINATE, YKOORDINATE, 
                      GESAMTFLAECHE, CNMITTELWERT, BASISZUFLUSS, 
                      FLIESSLAENGE, VERFAHREN, REGENSCHREIBER, 
                      LASTMODIFIED, KOMMENTAR, ID) 
                    SELECT
                      '{gebnam}', '{schnam}', {hoeheob}, 
                      {hoeheun}, {xel}, {yel}, 
                      {gesflaeche}, {cn}, {basisabfluss}, 
                      {fliessweg}, {verfahren}, '{regenschreiber}', 
                      '{createdat}', '{kommentar}', {nextid}
                    FROM RDB$DATABASE
                    WHERE '{gebnam}' NOT IN (SELECT NAME FROM AUSSENGEBIET);
                    """.format(
                    gebnam=gebnam,
                    schnam=schnam,
                    hoeheob=hoeheob,
                    hoeheun=hoeheun,
                    xel=xel,
                    yel=yel,
                    gesflaeche=gesflaeche,
                    cn=cn,
                    basisabfluss=basisabfluss,
                    fliessweg=fliessweg,
                    verfahren=0,
                    regenschreiber=regenschreiber,
                    createdat=createdat,
                    kommentar=kommentar,
                    nextid=nextid,
                )

                if not dbHE.sql(sql, "dbHE: export_aussengebiete (2)"):
                    del dbHE
                    return False

                sql = """
                    INSERT INTO TABELLENINHALTE
                    ( KEYWERT, WERT, REIHENFOLGE, ID)
                    SELECT
                      {cn}, {gesflaeche}, {reihenfolge}, {id}
                    FROM RDB$DATABASE;
                    """.format(
                    cn=cn, gesflaeche=gesflaeche, reihenfolge=1, id=nextid
                )

                if not dbHE.sql(sql, "dbHE: export_aussengebiete (2)"):
                    del dbHE
                    return False

                nextid += 1

        if not dbHE.sql("UPDATE ITWH$PROGINFO SET NEXTID = {:d}".format(nextid)):
            del dbHE
            return False
        dbHE.commit()

        fortschritt("{} Aussengebiete eingefuegt".format(nextid - nr0), 0.98)

    # Zum Schluss: Schließen der Datenbankverbindungen

    del dbHE

    fortschritt("Ende...", 1)
    progress_bar.setValue(100)
    status_message.setText("Datenexport abgeschlossen.")
    # status_message.setLevel(Qgis.Success)

    return True
