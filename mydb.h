#include <stddef.h>

#include "Database.h"

struct DB
{
    Database *base;

    ~DB()
    {
	delete base;
    }
};

struct DBC
{
    /* Maximum on-disk file size
     * 512MB by default
     * */
    size_t db_size;

    /* Page (node/data) size
     * 4KB by default
     * */
    size_t page_size;

    /* Maximum cached memory size
     * 16MB by default
     * */
    size_t cache_size;
};

/* Open DB if it exists, otherwise create DB */
extern "C" DB *dbcreate(char *file, DBC *conf);

extern "C" int db_close(DB *db);
extern "C" int db_delete(DB *, void *, size_t);
extern "C" int db_select(DB *, void *, size_t, void **, size_t *);
extern "C" int db_insert(DB *, void *, size_t, void * , size_t  );

/* Sync cached pages with disk */
extern "C" int db_flush(const DB *db);
extern "C" int db_sync(const DB *db);
