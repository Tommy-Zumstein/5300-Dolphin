# Makefile, Wonseok Seo, Kevin Cushing, Seattle University, CPSC5300, Summer 2018.
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c -ggdb
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

OBJS       = shellparser.o heap_storage.o ParseTreeToString.o SQLExec.o schema_tables.o storage_engine.o

shellparser: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser

HEAP_STORAGE_H = heap_storage.h storage_engine.h
SCHEMA_TABLES_H = schema_tables.h $(HEAP_STORAGE_H)
SQLEXEC_H = SQLExec.h $(SCHEMA_TABLES_H)
ParseTreeToString.o : ParseTreeToString.h
SQLExec.o : $(SQLEXEC_H)
heap_storage.o : $(HEAP_STORAGE_H)
schema_tables.o : $(SCHEMA_TABLES_) ParseTreeToString.h
storage_engine.o : storage_engine.h
shellparser.o : $(SQLEXEC_H) ParseTreeToString.h

%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"

clean:
	 rm -f shellparser *.o
