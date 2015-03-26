all: Bitset.cpp Database.cpp DatabaseNode.cpp DiskPageReadWriter.cpp GlobalConfiguration.cpp mydb.cpp
	g++ -O2 --std=c++11 -fPIC -shared Bitset.cpp Database.cpp DatabaseNode.cpp DiskPageReadWriter.cpp GlobalConfiguration.cpp Page.cpp mydb.cpp -o libmydb.so

sophia:
	make -C sophia/
