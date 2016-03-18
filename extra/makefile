include ../src/makefile.defs

TARGET = showdtree shelldt bench-redis bench-mysql bench-yesql bench-dtree bench-wiki-mysql bench-wiki-yesql getserver test-various test-gaia test-gaialocal test-tree  test-sql

BENCHLIB_SRC = bench-config.cpp bench-log.cpp bench-mysql-client.cpp bench-redis-client.cpp bench-runner.cpp bench-yesql-client.cpp bench-dtree-client.cpp bench-wiki-mysql-client.cpp bench-wiki-mysql.cpp bench-wiki-yesql-client.cpp bench-wiki-yesql.cpp bench-murmur-hash.cpp

BENCHLIB_OBJ = $(BENCHLIB_SRC:.cpp=.o)

all: $(TARGET)

ALL_SRC = $(shell find . -name "*.cpp")
SKIP_DEPS = clean

SRC_DIR = ../src

ifneq ($(MAKECMDGOALS),clean)
	include .depend
endif

.depend: $(ALL_SRC)
	rm -rf ./.depend
	$(CXX) $(CXXFLAGS) -MM $^ >> ./.depend


test-various: test-various.o $(SRC_DIR)/task.o $(SRC_DIR)/tmalloc.o $(SRC_DIR)/os.o $(SRC_DIR)/debug.o
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

test-gaia: test-gaia.o $(SRC_DIR)/yesquel.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

test-gaialocal: test-gaialocal.o $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

test-gaialocal.o: test-gaia.cpp test-gaialocal.cpp

test-sql: test-sql.o $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

showdtree: showdtree.o $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

shelldt: shelldt.o $(SRC_DIR)/treedirect.o $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

test-tree: test-tree.o $(SRC_DIR)/treedirect.o $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

bench.a: ${BENCHLIB_OBJ}
	ar rcs $@ $^

bench-redis: bench-redis.o bench.a
	$(CXX) $(CXXFLAGS) -o $@ $^ -lyajl -lhiredis -lpthread

bench-mysql: bench-mysql.o bench.a
	$(CXX) $(CXXFLAGS) -o $@ $^ -lyajl -lmysqlclient_r -lpthread

bench-mysql.o:
	$(CXX) $(CXXFLAGS) -I/usr/include/mysql -c -o $@ $<

bench-mysql-client.o:
	$(CXX) $(CXXFLAGS) -I/usr/include/mysql -c -o $@ $<

bench-yesql: bench-yesql.o bench.a $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ -lyajl $(LDLIBS)

bench-dtree: bench-dtree.o bench.a $(SRC_DIR)/treedirect.o $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ -lyajl $(LDLIBS)

bench-wiki-mysql: bench-wiki-mysql.o bench.a
	$(CXX) $(CXXFLAGS) -o $@ $^ -lyajl -lmysqlclient_r -lpthread

bench-wiki-mysql.o:
	$(CXX) $(CXXFLAGS) -I/usr/include/mysql -c -o $@ $<

bench-wiki-mysql-client.o:
	$(CXX) $(CXXFLAGS) -I/usr/include/mysql -c -o $@ $<


bench-wiki-yesql: bench-wiki-yesql.o bench.a $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ -lyajl $(LDLIBS)

getserver: getserver.o $(SRC_DIR)/yesquel.a $(SRC_DIR)/localstorage.a
	$(CXX) $(CXXFLAGS) -o $@ $^ $(LDLIBS)

clean:
	rm -f $(TARGET)
	find . -name "*.o" -exec rm {} \;
	rm -f .depend
	rm -f *.a
