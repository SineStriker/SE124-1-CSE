QT -= gui

CONFIG += c++11 console
CONFIG -= app_bundle

QMAKE_CXXFLAGS += -D_FILE_OFFSET_BITS=64 -DFUSE_USE_VERSION=25

# You can make your code fail to compile if it uses deprecated APIs.
# In order to do so, uncomment the following line.
#DEFINES += QT_DISABLE_DEPRECATED_BEFORE=0x060000    # disables all the APIs deprecated before Qt 6.0.0

SOURCES += \
        chfs_client.cc \
        extent_client.cc \
        extent_server.cc \
        gettime.cc \
        inode_manager.cc \
        my_test.cpp

# Default rules for deployment.
qnx: target.path = /tmp/$${TARGET}/bin
else: unix:!android: target.path = /opt/$${TARGET}/bin
!isEmpty(target.path): INSTALLS += target

DISTFILES += \
    grade.sh

HEADERS += \
    chfs_client.h \
    extent_client.h \
    extent_protocol.h \
    extent_server.h \
    gettime.h \
    inode_manager.h \
    lang/algorithm.h \
    lang/verify.h \
    rpc/connection.h \
    rpc/fifo.h \
    rpc/jsl_log.h \
    rpc/marshall.h \
    rpc/method_thread.h \
    rpc/pollmgr.h \
    rpc/rpc.h \
    rpc/slock.h \
    rpc/thr_pool.h

INCLUDEPATH += \
    rpc \
    lang \
    /usr/include/fuse

unix:!macx: LIBS += -lfuse
