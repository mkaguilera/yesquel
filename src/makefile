include makefile.defs

TARGET = storageserver shell callserver test-sql-simple yesquel.a 

CLIENTLIB_SRC = clientdir.cpp clientlib.cpp supervalue.cpp valbuf.cpp ccache.cpp

CLIENTLIBAUX_SRC = config.tab.cpp debug.cpp gaiarpcaux.cpp gaiatypes.cpp grpctcp.cpp ipmisc.cpp lex.yy.cpp newconfig.cpp os.cpp record.cpp scheduler.cpp pendingtx.cpp task.cpp tcpdatagram.cpp tmalloc.cpp util.cpp util-more.cpp

STORAGESERVER_SRC = storageserver.cpp storageserverstate.cpp storageserver-rpc.cpp diskstorage.cpp logmem.cpp main.cpp pendingtx.cpp disklog.cpp ccache-server.cpp

STORAGESERVERLOCALSTORAGE_SRC = clientlib-local.cpp

LOCALSTORAGE_SRC = clientlib-local.cpp disklog-nop.cpp diskstorage-nop.cpp logmem-local.cpp server-splitter-nop.cpp storageserver-nop.cpp storageserverstate-nop.cpp valbuf-local.cpp storageserver-local.cpp pendingtx-local.cpp storageserver-rpc-local.cpp ccache-server-nop.cpp

SPLITTER_SRC = dtreesplit.cpp splitter-client.cpp storageserver-splitter.cpp splitter-standalone.cpp loadstats.cpp

SPLITTERAUX_SRC = kvinterface.cpp dtreeaux.cpp coid.cpp

SPLITTERCLIENT_SRC = splitter-client.cpp

YSCLIENT_SRC = sqlite3.cpp yesql-init.cpp kvinterface.cpp dtreeaux.cpp coid.cpp

#------------------------ aux variables, derived from above

all: $(TARGET)

CLIENTLIB_OBJ = $(CLIENTLIB_SRC:.cpp=.o)
CLIENTLIBAUX_OBJ = $(CLIENTLIBAUX_SRC:.cpp=.o)
STORAGESERVER_OBJ = $(STORAGESERVER_SRC:.cpp=.o)
STORAGESERVERLOCALSTORAGE_OBJ = $(STORAGESERVERLOCALSTORAGE_SRC:.cpp=.o)
LOCALSTORAGE_OBJ = $(LOCALSTORAGE_SRC:.cpp=.o)
SPLITTER_OBJ = $(SPLITTER_SRC:.cpp=.o)
SPLITTERAUX_OBJ = $(SPLITTERAUX_SRC:.cpp=.o)
SPLITTERCLIENT_OBJ = $(SPLITTERCLIENT_SRC:.cpp=.o)
YSCLIENT_OBJ = $(YSCLIENT_SRC:.cpp=.o)
YESQUEL_OBJ = $(CLIENTLIB_OBJ) $(CLIENTLIBAUX_OBJ) $(SPLITTERCLIENT_OBJ) $(YSCLIENT_OBJ) localstorage.a
SERVER_OBJ = $(STORAGESERVER_OBJ) $(CLIENTLIB_OBJ) $(CLIENTLIBAUX_OBJ) $(SPLITTER_OBJ) $(SPLITTERAUX_OBJ) $(STORAGESERVERLOCALSTORAGE_OBJ)

ALL_SRC = $(shell find . -name "*.cpp")
SKIP_DEPS = clean

ifneq ($(MAKECMDGOALS),clean)
	include .depend
endif

#------------------ rules for actual targets

.depend: $(ALL_SRC)
	rm -rf ./.depend
	$(CXX) $(CXXFLAGS) -MM $^ >> ./.depend

config.tab.cpp config.tab.h: config.y
	bison --defines=config.tab.h --output=config.tab.cpp config.y

lex.yy.cpp: config.l config.tab.h
	flex --outfile=lex.yy.cpp config.l

sqlite3.o:
	$(CXX) $(CXXFLAGS) $(CXXUNUSEDFLAGS) -c -o $@ $<

yesquel.a: $(CLIENTLIB_OBJ) $(CLIENTLIBAUX_OBJ) $(SPLITTERCLIENT_OBJ) $(YSCLIENT_OBJ)
	ar rcs $@ $^

localstorage.a:  $(LOCALSTORAGE_OBJ)
	ar rcs $@ $^

storageserver: $(SERVER_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

callserver: callserver.o $(CLIENTLIB_OBJ) $(CLIENTLIBAUX_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

shell: shell.o $(YESQUEL_OBJ)
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

test-sql-simple: test-sql-simple.o yesquel.a localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)


clean:
	rm -f $(TARGET)
	find . -name "*.o" -exec rm {} \;
	rm -f .depend
	rm -f *.a
	rm -f config.tab.cpp config.tab.h lex.yy.cpp
