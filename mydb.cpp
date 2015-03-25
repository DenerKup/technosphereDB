#include "mydb.h"

#include <iostream>
#include <string>

DB *dbcreate(char *file, DBC *conf)
{
    try {
	std::cerr << "1";
	DB *res = new DB;

	std::cerr << "2";

	Database::Configuration newConf;
	newConf.pageSize = conf->page_size;
	newConf.cacheSize = conf->cache_size;
	newConf.size = conf->db_size;

	std::cerr << "page size " << conf->page_size << std::endl;
	std::cerr << "base size " << conf->db_size << std::endl;

	std::cerr << "3";

	res->base = new Database(file, newConf);

	std::cerr << "4";

	return res;
    } catch (std::string err) {
	std::cerr << "Error: " << err << std::endl;
	exit(0);
	return 0;
    }
}

int db_close(DB *db) {
    try {
	db->base->close();
	return 0;
    } catch (...) {
	exit(0);
	return 1;
    }
}

int db_delete(DB *db, void *key, size_t key_len) {
    try {
	db->base->remove(DatabaseNode::Record(key_len, static_cast<char *>(key)));
	return 0;
    } catch (...) {
	exit(0);
	return 1;
    }
}

int db_select(
    DB *db,
    void *key,
    size_t key_len,
    void **val,
    size_t *val_len
)
{
    DatabaseNode::Record keyRec(key_len, static_cast<char *>(key));
    DatabaseNode::Record valueRec(0, 0);

    try {
	int res = db->base->select(keyRec, valueRec);

	if (!res) {
	    std::cerr << "Not found\n";
	}

	*val_len = valueRec.size;
	*val = valueRec.data;
	return 0;
    } catch (...) {
	exit(0);
	return 1;
    }
}

int db_insert(
    DB *db,
    void *key,
    size_t key_len,
    void *val,
    size_t val_len
)
{
    try {
	db->base->insert(
	    DatabaseNode::Record(key_len, static_cast<char *>(key)),
	    DatabaseNode::Record(val_len, static_cast<char *>(val))
	);
	return 0;
    } catch (std::string s) {
	std::cerr << s << std::endl;
	exit(0);
	return 1;
    }
}

// TODO: implement
int db_sync(const DB *db)
{
}

// TODO: implement
int db_flush(const DB *db)
{
}
