#pragma once

#include "DiskPageReadWriter.h"
#include "DatabaseNode.h"

class Database
{
public:
    struct Configuration
    {
	size_t size;
	size_t pageSize;
	size_t cacheSize;
    };

    Database(const char *databaseFile, const Database::Configuration &configuration);
    ~Database();

    void close();
    void remove(const DatabaseNode::Record &key);
    void insert(const DatabaseNode::Record &key, const DatabaseNode::Record &value);
    bool select(const DatabaseNode::Record &key, DatabaseNode::Record &toWrite);

    //To be implemented
    void sync();

private:
    GlobalConfiguration m_globConfiguration;
    DiskPageReadWriter m_pageReadWriter;
    DatabaseNode *m_rootNode;

    size_t effectivePageSize() const;

    bool selectFromNode(
	DatabaseNode *node,
	const DatabaseNode::Record &key,
	DatabaseNode::Record &toWrite
    );

    void insertNonFull(
	DatabaseNode *node,
	const DatabaseNode::Record &key,
	const DatabaseNode::Record &value
    );

    void splitChild(
	DatabaseNode *x,
	size_t i,
	DatabaseNode *y
    );

    void removeFromNode(
	DatabaseNode *node,
	const DatabaseNode::Record &key
    );

    DatabaseNode *loadNode(size_t pageNum);
    DatabaseNode *createNode();

    void findRightmostKey(
	DatabaseNode *node,
	DatabaseNode::Record &key,
	DatabaseNode::Record &value
    );

    void findLeftmostKey(
	DatabaseNode *node,
	DatabaseNode::Record &key,
	DatabaseNode::Record &value
    );

    void merge(
	DatabaseNode *y,
	DatabaseNode *x,
	size_t i,
	DatabaseNode *z
    );
};
