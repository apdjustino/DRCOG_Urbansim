# -*- coding: utf-8 -*-

# Form implementation generated from reading ui file '/Users/ckla/Documents/workspace/opus_trunk/opus_gui/results_manager/views/get_run_info.ui'
#
# Created: Thu May 14 17:03:05 2009
#      by: PyQt4 UI code generator 4.4.4
#
# WARNING! All changes made in this file will be lost!

from PyQt4 import QtCore, QtGui

class Ui_dlgGetRunInfo(object):
    def setupUi(self, dlgGetRunInfo):
        dlgGetRunInfo.setObjectName("dlgGetRunInfo")
        dlgGetRunInfo.resize(563, 243)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Preferred, QtGui.QSizePolicy.MinimumExpanding)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(dlgGetRunInfo.sizePolicy().hasHeightForWidth())
        dlgGetRunInfo.setSizePolicy(sizePolicy)
        self.verticalLayout = QtGui.QVBoxLayout(dlgGetRunInfo)
        self.verticalLayout.setObjectName("verticalLayout")
        self.horizontalLayout_7 = QtGui.QHBoxLayout()
        self.horizontalLayout_7.setMargin(0)
        self.horizontalLayout_7.setObjectName("horizontalLayout_7")
        self.frame = QtGui.QFrame(dlgGetRunInfo)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.MinimumExpanding, QtGui.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.frame.sizePolicy().hasHeightForWidth())
        self.frame.setSizePolicy(sizePolicy)
        self.frame.setMinimumSize(QtCore.QSize(450, 0))
        self.frame.setFrameShadow(QtGui.QFrame.Raised)
        self.frame.setObjectName("frame")
        self.gridLayout = QtGui.QGridLayout(self.frame)
        self.gridLayout.setMargin(0)
        self.gridLayout.setObjectName("gridLayout")
        self.label_5 = QtGui.QLabel(self.frame)
        self.label_5.setObjectName("label_5")
        self.gridLayout.addWidget(self.label_5, 0, 0, 1, 1)
        self.lblRunId = QtGui.QLabel(self.frame)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.MinimumExpanding, QtGui.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.lblRunId.sizePolicy().hasHeightForWidth())
        self.lblRunId.setSizePolicy(sizePolicy)
        self.lblRunId.setMinimumSize(QtCore.QSize(250, 25))
        self.lblRunId.setAutoFillBackground(False)
        self.lblRunId.setFrameShape(QtGui.QFrame.Panel)
        self.lblRunId.setFrameShadow(QtGui.QFrame.Sunken)
        self.lblRunId.setTextFormat(QtCore.Qt.PlainText)
        self.lblRunId.setTextInteractionFlags(QtCore.Qt.LinksAccessibleByMouse|QtCore.Qt.TextSelectableByMouse)
        self.lblRunId.setObjectName("lblRunId")
        self.gridLayout.addWidget(self.lblRunId, 0, 1, 1, 1)
        self.label_4 = QtGui.QLabel(self.frame)
        self.label_4.setObjectName("label_4")
        self.gridLayout.addWidget(self.label_4, 1, 0, 1, 1)
        self.lblScenario_name = QtGui.QLabel(self.frame)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.lblScenario_name.sizePolicy().hasHeightForWidth())
        self.lblScenario_name.setSizePolicy(sizePolicy)
        self.lblScenario_name.setMinimumSize(QtCore.QSize(0, 25))
        self.lblScenario_name.setAutoFillBackground(False)
        self.lblScenario_name.setFrameShape(QtGui.QFrame.Panel)
        self.lblScenario_name.setFrameShadow(QtGui.QFrame.Sunken)
        self.lblScenario_name.setTextFormat(QtCore.Qt.PlainText)
        self.lblScenario_name.setTextInteractionFlags(QtCore.Qt.LinksAccessibleByMouse|QtCore.Qt.TextSelectableByMouse)
        self.lblScenario_name.setObjectName("lblScenario_name")
        self.gridLayout.addWidget(self.lblScenario_name, 1, 1, 1, 1)
        self.label = QtGui.QLabel(self.frame)
        self.label.setObjectName("label")
        self.gridLayout.addWidget(self.label, 2, 0, 1, 1)
        self.lblRun_name = QtGui.QLabel(self.frame)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.lblRun_name.sizePolicy().hasHeightForWidth())
        self.lblRun_name.setSizePolicy(sizePolicy)
        self.lblRun_name.setMinimumSize(QtCore.QSize(0, 25))
        self.lblRun_name.setAutoFillBackground(False)
        self.lblRun_name.setFrameShape(QtGui.QFrame.Panel)
        self.lblRun_name.setFrameShadow(QtGui.QFrame.Sunken)
        self.lblRun_name.setTextFormat(QtCore.Qt.PlainText)
        self.lblRun_name.setWordWrap(True)
        self.lblRun_name.setTextInteractionFlags(QtCore.Qt.LinksAccessibleByMouse|QtCore.Qt.TextSelectableByMouse)
        self.lblRun_name.setObjectName("lblRun_name")
        self.gridLayout.addWidget(self.lblRun_name, 2, 1, 1, 1)
        self.label_2 = QtGui.QLabel(self.frame)
        self.label_2.setObjectName("label_2")
        self.gridLayout.addWidget(self.label_2, 3, 0, 1, 1)
        self.lblYears_run = QtGui.QLabel(self.frame)
        sizePolicy = QtGui.QSizePolicy(QtGui.QSizePolicy.Expanding, QtGui.QSizePolicy.Preferred)
        sizePolicy.setHorizontalStretch(0)
        sizePolicy.setVerticalStretch(0)
        sizePolicy.setHeightForWidth(self.lblYears_run.sizePolicy().hasHeightForWidth())
        self.lblYears_run.setSizePolicy(sizePolicy)
        self.lblYears_run.setMinimumSize(QtCore.QSize(0, 25))
        self.lblYears_run.setAutoFillBackground(False)
        self.lblYears_run.setFrameShape(QtGui.QFrame.Panel)
        self.lblYears_run.setFrameShadow(QtGui.QFrame.Sunken)
        self.lblYears_run.setTextFormat(QtCore.Qt.PlainText)
        self.lblYears_run.setTextInteractionFlags(QtCore.Qt.LinksAccessibleByMouse|QtCore.Qt.TextSelectableByMouse)
        self.lblYears_run.setObjectName("lblYears_run")
        self.gridLayout.addWidget(self.lblYears_run, 3, 1, 1, 1)
        self.label_3 = QtGui.QLabel(self.frame)
        self.label_3.setObjectName("label_3")
        self.gridLayout.addWidget(self.label_3, 4, 0, 1, 1)
        spacerItem = QtGui.QSpacerItem(20, 0, QtGui.QSizePolicy.Minimum, QtGui.QSizePolicy.Expanding)
        self.gridLayout.addItem(spacerItem, 5, 0, 1, 1)
        self.horizontalLayout_5 = QtGui.QHBoxLayout()
        self.horizontalLayout_5.setObjectName("horizontalLayout_5")
        self.lblCache_directory = QtGui.QLabel(self.frame)
        self.lblCache_directory.setMinimumSize(QtCore.QSize(0, 25))
        self.lblCache_directory.setAutoFillBackground(False)
        self.lblCache_directory.setFrameShape(QtGui.QFrame.Panel)
        self.lblCache_directory.setFrameShadow(QtGui.QFrame.Sunken)
        self.lblCache_directory.setWordWrap(True)
        self.lblCache_directory.setObjectName("lblCache_directory")
        self.horizontalLayout_5.addWidget(self.lblCache_directory)
        self.tb_select_cachedir = QtGui.QToolButton(self.frame)
        self.tb_select_cachedir.setMinimumSize(QtCore.QSize(25, 25))
        self.tb_select_cachedir.setObjectName("tb_select_cachedir")
        self.horizontalLayout_5.addWidget(self.tb_select_cachedir)
        self.gridLayout.addLayout(self.horizontalLayout_5, 4, 1, 1, 1)
        self.horizontalLayout_7.addWidget(self.frame)
        self.verticalLayout.addLayout(self.horizontalLayout_7)
        self.buttonBox = QtGui.QDialogButtonBox(dlgGetRunInfo)
        self.buttonBox.setStandardButtons(QtGui.QDialogButtonBox.Close)
        self.buttonBox.setObjectName("buttonBox")
        self.verticalLayout.addWidget(self.buttonBox)

        self.retranslateUi(dlgGetRunInfo)
        QtCore.QMetaObject.connectSlotsByName(dlgGetRunInfo)

    def retranslateUi(self, dlgGetRunInfo):
        dlgGetRunInfo.setWindowTitle(QtGui.QApplication.translate("dlgGetRunInfo", "Details for simulation run", None, QtGui.QApplication.UnicodeUTF8))
        self.label_5.setText(QtGui.QApplication.translate("dlgGetRunInfo", "Run id:", None, QtGui.QApplication.UnicodeUTF8))
        self.lblRunId.setText(QtGui.QApplication.translate("dlgGetRunInfo", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.label_4.setText(QtGui.QApplication.translate("dlgGetRunInfo", "Scenario:", None, QtGui.QApplication.UnicodeUTF8))
        self.lblScenario_name.setText(QtGui.QApplication.translate("dlgGetRunInfo", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.label.setText(QtGui.QApplication.translate("dlgGetRunInfo", "Run name:", None, QtGui.QApplication.UnicodeUTF8))
        self.lblRun_name.setText(QtGui.QApplication.translate("dlgGetRunInfo", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.label_2.setText(QtGui.QApplication.translate("dlgGetRunInfo", "Years run:", None, QtGui.QApplication.UnicodeUTF8))
        self.lblYears_run.setText(QtGui.QApplication.translate("dlgGetRunInfo", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.label_3.setText(QtGui.QApplication.translate("dlgGetRunInfo", "Cache directory:", None, QtGui.QApplication.UnicodeUTF8))
        self.lblCache_directory.setText(QtGui.QApplication.translate("dlgGetRunInfo", "...", None, QtGui.QApplication.UnicodeUTF8))
        self.tb_select_cachedir.setText(QtGui.QApplication.translate("dlgGetRunInfo", "...", None, QtGui.QApplication.UnicodeUTF8))

