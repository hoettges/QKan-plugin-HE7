# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file 'application_dialog_base.ui'
#
# Created by: PyQt4 UI code generator 4.11.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

try:
    _fromUtf8 = QtCore.QString.fromUtf8
except AttributeError:
    def _fromUtf8(s):
        return s

try:
    _encoding = QtGui.QApplication.UnicodeUTF8


    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig, _encoding)
except AttributeError:
    def _translate(context, text, disambig):
        return QtGui.QApplication.translate(context, text, disambig)


class Ui_ImportFromHEDialogBase(object):
    def setupUi(self, ImportFromHEDialogBase):
        ImportFromHEDialogBase.setObjectName(_fromUtf8("ImportFromHEDialogBase"))
        ImportFromHEDialogBase.resize(539, 378)
        self.button_box = QtGui.QDialogButtonBox(ImportFromHEDialogBase)
        self.button_box.setGeometry(QtCore.QRect(140, 320, 251, 32))
        font = QtGui.QFont()
        font.setPointSize(10)
        font.setBold(True)
        font.setWeight(75)
        self.button_box.setFont(font)
        self.button_box.setOrientation(QtCore.Qt.Horizontal)
        self.button_box.setStandardButtons(
            QtGui.QDialogButtonBox.Cancel | QtGui.QDialogButtonBox.Help | QtGui.QDialogButtonBox.Ok)
        self.button_box.setObjectName(_fromUtf8("button_box"))
        self.groupBox = QtGui.QGroupBox(ImportFromHEDialogBase)
        self.groupBox.setGeometry(QtCore.QRect(20, 20, 491, 191))
        font = QtGui.QFont()
        font.setBold(True)
        font.setWeight(75)
        self.groupBox.setFont(font)
        self.groupBox.setToolTip(_fromUtf8(""))
        self.groupBox.setFlat(False)
        self.groupBox.setCheckable(False)
        self.groupBox.setObjectName(_fromUtf8("groupBox"))
        self.label = QtGui.QLabel(self.groupBox)
        self.label.setGeometry(QtCore.QRect(10, 80, 291, 21))
        font = QtGui.QFont()
        font.setBold(False)
        font.setWeight(50)
        self.label.setFont(font)
        self.label.setAlignment(QtCore.Qt.AlignLeading | QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        self.label.setObjectName(_fromUtf8("label"))
        self.tf_qkanDB = QtGui.QLineEdit(self.groupBox)
        self.tf_qkanDB.setGeometry(QtCore.QRect(10, 100, 391, 20))
        self.tf_qkanDB.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignTrailing | QtCore.Qt.AlignVCenter)
        self.tf_qkanDB.setObjectName(_fromUtf8("tf_qkanDB"))
        self.pb_selectQkanDB = QtGui.QPushButton(self.groupBox)
        self.pb_selectQkanDB.setGeometry(QtCore.QRect(410, 100, 31, 21))
        self.pb_selectQkanDB.setObjectName(_fromUtf8("pb_selectQkanDB"))
        self.label_2 = QtGui.QLabel(self.groupBox)
        self.label_2.setGeometry(QtCore.QRect(10, 20, 301, 21))
        font = QtGui.QFont()
        font.setBold(False)
        font.setWeight(50)
        self.label_2.setFont(font)
        self.label_2.setAlignment(QtCore.Qt.AlignLeading | QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        self.label_2.setObjectName(_fromUtf8("label_2"))
        self.pb_selectHeDB = QtGui.QPushButton(self.groupBox)
        self.pb_selectHeDB.setGeometry(QtCore.QRect(410, 40, 31, 21))
        self.pb_selectHeDB.setObjectName(_fromUtf8("pb_selectHeDB"))
        self.tf_heDB = QtGui.QLineEdit(self.groupBox)
        self.tf_heDB.setGeometry(QtCore.QRect(10, 40, 391, 21))
        self.tf_heDB.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignTrailing | QtCore.Qt.AlignVCenter)
        self.tf_heDB.setObjectName(_fromUtf8("tf_heDB"))
        self.tf_epsg = QtGui.QLineEdit(self.groupBox)
        self.tf_epsg.setGeometry(QtCore.QRect(332, 150, 71, 20))
        self.tf_epsg.setObjectName(_fromUtf8("tf_epsg"))
        self.label_5 = QtGui.QLabel(self.groupBox)
        self.label_5.setGeometry(QtCore.QRect(130, 150, 191, 21))
        font = QtGui.QFont()
        font.setBold(False)
        font.setWeight(50)
        self.label_5.setFont(font)
        self.label_5.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignTrailing | QtCore.Qt.AlignVCenter)
        self.label_5.setObjectName(_fromUtf8("label_5"))
        self.pb_selectKBS = QtGui.QPushButton(self.groupBox)
        self.pb_selectKBS.setGeometry(QtCore.QRect(410, 150, 31, 21))
        self.pb_selectKBS.setObjectName(_fromUtf8("pb_selectKBS"))
        self.groupBox_2 = QtGui.QGroupBox(ImportFromHEDialogBase)
        self.groupBox_2.setGeometry(QtCore.QRect(20, 220, 491, 81))
        font = QtGui.QFont()
        font.setBold(True)
        font.setWeight(75)
        self.groupBox_2.setFont(font)
        self.groupBox_2.setToolTip(_fromUtf8(""))
        self.groupBox_2.setFlat(False)
        self.groupBox_2.setCheckable(False)
        self.groupBox_2.setObjectName(_fromUtf8("groupBox_2"))
        self.label_4 = QtGui.QLabel(self.groupBox_2)
        self.label_4.setGeometry(QtCore.QRect(10, 20, 301, 21))
        font = QtGui.QFont()
        font.setBold(False)
        font.setWeight(50)
        self.label_4.setFont(font)
        self.label_4.setAlignment(QtCore.Qt.AlignLeading | QtCore.Qt.AlignLeft | QtCore.Qt.AlignVCenter)
        self.label_4.setObjectName(_fromUtf8("label_4"))
        self.pb_selectProjectFile = QtGui.QPushButton(self.groupBox_2)
        self.pb_selectProjectFile.setGeometry(QtCore.QRect(410, 40, 31, 21))
        self.pb_selectProjectFile.setObjectName(_fromUtf8("pb_selectProjectFile"))
        self.tf_projectFile = QtGui.QLineEdit(self.groupBox_2)
        self.tf_projectFile.setGeometry(QtCore.QRect(10, 40, 391, 21))
        self.tf_projectFile.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignTrailing | QtCore.Qt.AlignVCenter)
        self.tf_projectFile.setObjectName(_fromUtf8("tf_projectFile"))
        self.label_3 = QtGui.QLabel(ImportFromHEDialogBase)
        self.label_3.setGeometry(QtCore.QRect(440, 360, 91, 20))
        self.label_3.setAlignment(QtCore.Qt.AlignRight | QtCore.Qt.AlignTrailing | QtCore.Qt.AlignVCenter)
        self.label_3.setObjectName(_fromUtf8("label_3"))

        self.retranslateUi(ImportFromHEDialogBase)
        QtCore.QObject.connect(self.button_box, QtCore.SIGNAL(_fromUtf8("accepted()")), ImportFromHEDialogBase.accept)
        QtCore.QObject.connect(self.button_box, QtCore.SIGNAL(_fromUtf8("rejected()")), ImportFromHEDialogBase.reject)
        QtCore.QMetaObject.connectSlotsByName(ImportFromHEDialogBase)

    def retranslateUi(self, ImportFromHEDialogBase):
        ImportFromHEDialogBase.setWindowTitle(
            _translate("ImportFromHEDialogBase", "QKan Import from Hystem-Extran", None))
        self.button_box.setToolTip(
            _translate("ImportFromHEDialogBase", "<html><head/><body><p>Datenimport starten ...</p></body></html>",
                       None))
        self.groupBox.setTitle(_translate("ImportFromHEDialogBase", "Datenbank-Verbindungen", None))
        self.label.setText(_translate("ImportFromHEDialogBase", "Datenziel: QKan-Datenbank (*.sqlite):", None))
        self.tf_qkanDB.setToolTip(_translate("ImportFromHEDialogBase",
                                             "<html><head/><body><p>QKan - Zieldatenbank (wird ggfs. neu angelegt)</p></body></html>",
                                             None))
        self.pb_selectQkanDB.setToolTip(_translate("ImportFromHEDialogBase",
                                                   "<html><head/><body><p>Zieldatenbank auswählen und optional erstellen</p></body></html>",
                                                   None))
        self.pb_selectQkanDB.setText(_translate("ImportFromHEDialogBase", "...", None))
        self.label_2.setText(
            _translate("ImportFromHEDialogBase", "Datenquelle: Hystem-Extran-Datenbank (*.idbf):", None))
        self.pb_selectHeDB.setToolTip(_translate("ImportFromHEDialogBase",
                                                 "<html><head/><body><p>ITWH-Quelldatenbank mit Kanalnetzdaten auswählen</p></body></html>",
                                                 None))
        self.pb_selectHeDB.setText(_translate("ImportFromHEDialogBase", "...", None))
        self.tf_heDB.setToolTip(_translate("ImportFromHEDialogBase",
                                           "<html><head/><body><p>Quelldatenbank mit Kanaldaten aus HYSTEM-EXTRAN</p></body></html>",
                                           None))
        self.label_5.setText(_translate("ImportFromHEDialogBase", "EPSG-Code des Projektionssystems:", None))
        self.pb_selectKBS.setToolTip(_translate("ImportFromHEDialogBase",
                                                "<html><head/><body><p>Zieldatenbank auswählen und optional erstellen</p></body></html>",
                                                None))
        self.pb_selectKBS.setText(_translate("ImportFromHEDialogBase", "...", None))
        self.groupBox_2.setTitle(_translate("ImportFromHEDialogBase", "Projektdatei erzeugen (optional)", None))
        self.label_4.setText(_translate("ImportFromHEDialogBase", "Projektdatei (*.qgs):", None))
        self.pb_selectProjectFile.setToolTip(_translate("ImportFromHEDialogBase",
                                                        "<html><head/><body><p>Pfad und Name der Projektdatei festlegen</p></body></html>",
                                                        None))
        self.pb_selectProjectFile.setText(_translate("ImportFromHEDialogBase", "...", None))
        self.tf_projectFile.setToolTip(_translate("ImportFromHEDialogBase",
                                                  "<html><head/><body><p>Pfad der neu erzeugten Projektdatei (optional)</p></body></html>",
                                                  None))
        self.label_3.setToolTip(_translate("ImportFromHEDialogBase", "Version QKan Import", None))
        self.label_3.setText(_translate("ImportFromHEDialogBase", "Version 1.1.1", None))
