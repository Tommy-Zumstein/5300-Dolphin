# Makefile, Natalia Manriquez, Siyao Xu, Seattle University, CPSC5300, Summer 2018.
CCFLAGS     = -std=c++11 -std=c++0x -Wall -Wno-c++11-compat -DHAVE_CXX_STDHEADERS -D_GNU_SOURCE -D_REENTRANT -O3 -c
COURSE      = /usr/local/db6
INCLUDE_DIR = $(COURSE)/include
LIB_DIR     = $(COURSE)/lib

OBJS       = shellparser.o heap_storage.o

shellparser: $(OBJS)
	g++ -L$(LIB_DIR) -o $@ $(OBJS) -ldb_cxx -lsqlparser
shellparser.o : heap_storage.h storage_engine.h
heap_storage.o : heap_storage.h storage_engine.h

%.o: %.cpp
	g++ -I$(INCLUDE_DIR) $(CCFLAGS) -o "$@" "$<"