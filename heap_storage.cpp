/**
 * @File    heap_storage.cpp - Implemnetation of:
                                SlottedPage, HeapFile, and HeapTable
 * @Group:  Dolphin - Sprint2
 * @Author: Wonseok Seo, Kevin Cushing - advised from Kevin Lundeen @SU
 * @see "Seattle University, CPSC5300, Summer 2018"
 */

#include <stdio.h>
#include <stdlib.h>
#include <memory.h>
#include <cstring>
#include <sstream>
#include "heap_storage.h"
using namespace std;

typedef uint16_t u16;

/************************************************
 *  Implementation of SlottedPage class
 ***********************************************/

/**
 *  New empty block ready for new records to be added or existing block is added
 */
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) :
                         DbBlock(block, block_id, is_new) {
    if (is_new) {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    } else {
        get_header(this->num_records, this->end_free);
    }
}

/**
 * Add a new record to the block.
 * @param   Dbt *data           data to store
 * @return  RecordID            new record id added to slottedpage block
 * @throw   DbBlockNoRoomError  no room exception error
 */
RecordID SlottedPage::add(const Dbt *data) throw(DbBlockNoRoomError){
    if (!has_room((u16)data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1U;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

/**
 * Get a record from this block with the corresponding id
 * @param   RecordID record_id  target record id
 * @return  Dbt*                pointer to data in memory
 */
Dbt *SlottedPage::get(RecordID record_id) const {
    u16 size = 0, loc = 0;
    this->get_header(size, loc, record_id);
    if (loc == 0)
        return nullptr;
    return new Dbt(this->address(loc), size);
}

/**
 * Replace the record with the given data
 * @param   RecordID record_id  target record to replace
 * @param   Dbt &data           data to be stored in the target record
 * @throw   DbBlockNoRoomError  no room exception error
 * @throw   DbBlockError        no record error
 *
 * Note: this function implemenation logic is different from sample solution
 *       (from sprint1), we made slight modification
 */
void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError, DbBlockError) {
    // check record id
    if (!have_record(record_id))
        throw DbBlockError("Record not found");
    // to hold original record location and size
    u16 old_size, old_loc;
    get_header(old_size, old_loc, record_id);
    // to hold new record size
    u16 new_size = (u16)data.get_size();
    // in case of new data size is bigger than before and there is no room
    if (new_size > old_size && !has_room(new_size - old_size))
        throw DbBlockNoRoomError("Not enough room");
    // in case of new record size is bigger than before and there is room
    if (new_size > old_size) {
        // calculate shift size
        u16 shift_loc = new_size - old_size;
        // calculate new location
        u16 new_loc = old_loc - shift_loc;
        slide(record_id + 1, shift_loc);
        memcpy(this->address(new_loc), data.get_data(), new_size);
        // update header
        put_header(record_id, new_size, new_loc);
        // in case of the target record is the last one
        if (record_id == this->num_records) {
            this->end_free -= shift_loc;
        }
    // in case of new data size is not bigger than before
    } else {
        // calculate shift size
        u16 shift_loc = old_size - new_size;
        // calculate new location
        u16 new_loc = old_loc + shift_loc;
        slide(record_id + 1, shift_loc, false);
        memcpy(this->address(new_loc), data.get_data(), new_size);
        // update header
        put_header(record_id, new_size, new_loc);
        // in case of the target record is the last one
        if (record_id == this->num_records){
            this->end_free += shift_loc;
        }
    }
    put_header();
}

/**
 * Mark the given id as deleted by changing its size to zero and tis location to
 * 0. Compact the rest of the data in the block. But keep the record ids the
 * same for everyone
 * @param   RecordID record_id  target record id to delete
 */
void SlottedPage::del(RecordID record_id) {
    // check record id
    if (!have_record(record_id))
       throw DbBlockError("Record not found");
    // to hold size and location of the record
    u16 size, loc;
    get_header(size, loc, record_id);
    slide(record_id + 1, size, false);
    put_header(record_id, 0U, 0U);
    // in case of the record is the last one
    if (record_id == this->num_records){
       this->end_free += size;
    }
    put_header();
}

/**
 * Obtain all record ids from this block
 * @return  RecordIDs*  vector of record ids
 */
RecordIDs *SlottedPage::ids(void) const {
    RecordIDs *record_ids = new RecordIDs();
    for (RecordID i = 1; i <= this->num_records; i++) {
        if (have_record(i)){
            record_ids->push_back(i);
        }
    }
    return record_ids;
}

// Check if the record exists based on the record id
bool SlottedPage::have_record(RecordID record_id) const {
    if (record_id == 0 || record_id > this->num_records)
        return false;
    // to hold a record size and location
    u16 size, loc;
    get_header(size, loc, record_id);
    // check the record id that has been deleted
    if (loc == 0)
        return false;
    return true;
}

// Set the header values from a record
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id) const {
    size = get_n((u16)4 * id);
    loc = get_n((u16)(4 * id + 2));
}

// Store the size and offset for given id. For id of zero, store block header
void SlottedPage::put_header(RecordID id, u16 size, u16 loc){
    if (id == 0){
        size = this->num_records;
        loc = this->end_free;
    }
    put_n((u16)4 * id, size);
    put_n((u16)(4 * id + 2), loc);
}

// Check if there is enough room
bool SlottedPage::has_room(u16 size) const {
    return 4 * this->num_records + 4 <= this->end_free - size;
}

// Slide all record in the blokc left or right direction to compact it
// Note: this function implementation logic is different from sample solution
//       it is based on record_id instead of location (From sprint1), we made
//       slight modificaiton
void SlottedPage::slide(RecordID start_record_id, u16 offset, bool left) {
    while (start_record_id <= this->num_records &&
           !have_record(start_record_id)) {
        start_record_id++;
    }
    if (start_record_id > this->num_records) {
        return;
    }
    u16 begin_offset, begin_size;
    get_header(begin_size, begin_offset, start_record_id);
    u16 shift_size = begin_offset + begin_size - 1 - this->end_free;
    char temp[shift_size];
    memcpy(temp, this->address(this->end_free + 1), shift_size);
    if (left) {
        memcpy(this->address(this->end_free + 1 - offset), temp, shift_size);
    } else {
        memcpy(this->address(this->end_free + 1 + offset), temp, shift_size);
    }
    for (RecordID i = start_record_id; i <= this->num_records; i++) {
        if (have_record(i)) {
            u16 temp_offset, temp_size;
            get_header(temp_size, temp_offset, i);
            if (left) {
                put_header(i, temp_size, temp_offset - offset);
            } else {
                put_header(i, temp_size, temp_offset + offset);
            }
        }
    }
    this->end_free = left ? this->end_free - offset : this->end_free + offset;
    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset) const {
    return *(u16 *)this->address(offset);
}

// Put 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n) {
    *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset) const {
    return (void *)((char *)this->block.get_data() + offset);
}

/************************************************
 *  Implementation of HeapFile class
 ***********************************************/

/**
 * Set name of the relation, and other parameters
 * @param   string name   File name
 */
HeapFile::HeapFile(std::string name) : DbFile(name), last(0), db(_DB_ENV, 0) {
    this->dbfilename = this->name + ".db";
    this->closed = true;
}

/**
 * Create the database file
 */
void HeapFile::create(void) {
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *block = get_new();
    delete block;
}

/**
 * Delete the file, and the physical file
 */
void HeapFile::drop(void) {
    close();
    Db db(_DB_ENV, 0);
    db.remove(NULL, this->dbfilename.c_str(), NULL , 0);
}

/**
 * Open the database file
 */
void HeapFile::open(void) {
    db_open();
}

/**
 * Close the database file
 */
void HeapFile::close(void) {
    if (!this->closed) {
        this->db.close(0);
        this->closed = true;
    }
}

/**
 * Allocate a new block for the database file
 * @return  SlottedPage*  new empty DbBlock that is managing the records in this
 *                        block and tis block id
 */
SlottedPage *HeapFile::get_new(void) {
    char block[DbBlock::BLOCK_SZ];
    memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));
    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));
    // write out an empty block and read it back in so Berkeley DB is managing
    // the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    // write it out with initialization applied
    this->db.put(nullptr, &key, &data, 0);
    delete page;
    this->db.get(nullptr, &key, &data, 0);
    return new SlottedPage(data, this->last);
}

/**
 * Get a block from the database file for a given block id
 * @param   BlockID block_id  target block id
 * @return  SlottedPage*      slottedpage block pointer
 */
SlottedPage *HeapFile::get(BlockID block_id) {
    Dbt data;
    Dbt key(&block_id, sizeof(block_id));
    this->db.get(NULL, &key, &data, 0);
    SlottedPage *page = new SlottedPage(data, block_id, false);
    return page;
}

/**
 * Write a block to the file
 * @param   DbBlock *block  target block id
 */
void HeapFile::put(DbBlock *block) {
    BlockID id = block->get_block_id();
    Dbt key(&id, sizeof(id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

/**
 * Obtain all blocks from this file
 * @return  BlockIDs*   vector of block ids
 */
BlockIDs *HeapFile::block_ids() const {
    BlockIDs *ids = new BlockIDs();
    for (BlockID i = 1; i <= this->last; i++) {
        ids->push_back(i);
    }
    return ids;
}

// Get the number of blocks in the file
uint32_t HeapFile::get_block_count() {
  DB_BTREE_STAT* stat;
  this->db.stat(nullptr, &stat, DB_FAST_STAT);
  return stat->bt_ndata;
}

// Open the database file, and set dbenv parameters
void HeapFile::db_open(uint flags) {
    if (!this->closed){
        return;
    }
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0644);
    this->last = flags ? 0 : get_block_count();
    this->closed = false;
}

/************************************************
 *  Implementation of HeapTable class
 ***********************************************/

/**
 * Takes the name of the relation, the columns, and all the column attributes
 * @param           table_name        relation name
 * ColumnNames      column_names      column name list
 * ColumnAttributes column_attributes column attribute list
 */
HeapTable::HeapTable(Identifier table_name, ColumnNames column_names,
                     ColumnAttributes column_attributes) :
                     DbRelation(table_name, column_names, column_attributes),
                     file(table_name) {
}

/**
 * Execute CREATE TABLE <table_name> ( <columns> )
 */
void HeapTable::create() {
    this->file.create();
}

/**
 * Exectue CREATE TABLE IF NOT EXITST <table_name> ( <columns> )
 */
void HeapTable::create_if_not_exists() {
    try {
        open();
    }
    catch (DbException& e) {
        create();
    }
}

/**
 * Exectue DROP TABLE <table_name>
 */
void HeapTable::drop(){
    this->file.drop();
}

/**
 * Open existing table. Enables: insert, delete, select, project
 */
void HeapTable::open(){
    this->file.open();
}

/**
 * Close the table. Disables: insert, delete, select, project
 */
void HeapTable::close(){
    this->file.close();
}

/**
 * Expect row to be a dictionary with column name keys
 * Execute INSERT INTO <table_name> ( <row_keys> ) VALUES ( <row_values> )
 * @param   row     the key and value pair to insert
 * @return  handle  the handle of the inserted row
 */
Handle HeapTable::insert(const ValueDict *row) {
    open();
    ValueDict* full_row = validate(row);
    Handle handle = append(full_row);
    delete full_row;
    return handle;
}

// Not implemented, next sprint
void HeapTable::update(const Handle handle, const ValueDict *new_values) {
  throw DbRelationError("Not implemented");
}

/**
 * Execute DELETE FROM <table_name> WHERE <handle>
 * @param   handles   the handle of the row to be deleted
 */
void HeapTable::del(const Handle handle) {
    open();
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage* block = this->file.get(block_id);
    block->del(record_id);
    this->file.put(block);
    delete block;
}

/**
 * Returns handles to the all rows of the Select statement
 * Execute SELECT <handle> FROM <table_name>
 * @return  handles   a list of handles for all rows
 */
Handles *HeapTable::select() {
    open();
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id : *record_ids)
            handles->push_back(Handle(block_id, record_id));
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**
 * Exectue SELECT <handle> FROM <table_name> WHERE <where>
 * @param where     key and value pair for condition
 * @return handles  a list of handles for qualifying rows
 */
Handles *HeapTable::select(const ValueDict *where) {
    open();
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id: *block_ids) {
        SlottedPage *block = file.get(block_id);
        RecordIDs *record_ids = block->ids();
        for (auto const &record_id: *record_ids) {
            Handle handle(block_id, record_id);
            if (selected(handle, where)) {
                handles->push_back(Handle(block_id, record_id));
            }
        }
        delete record_ids;
        delete block;
    }
    delete block_ids;
    return handles;
}

/**
 * Get a sequence of all values for handle
 * @param   handle  handle for rows
 * @return  row     values
 */
ValueDict *HeapTable::project(Handle handle){
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    delete data;
    delete block;
    return row;
}

/**
 * Extracts fields from a row handle based on a set of given column names
 * @param   handle          handle for rows
 * @param   column_names    column names to project
 * @return  result          sequence of vlaues
 * @throw   DbRelationError error if there is no matching column name/s
 */
ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names) {
    ValueDict *result = new ValueDict();
    ValueDict *row = project(handle);
    if (column_names == NULL || column_names->empty()) {
        delete result;
        return row;
    } else {
        for (auto const &column_name : *column_names) {
            if (row->find(column_name) != row->end()) {
                result->insert(std::pair<Identifier, Value>(column_name,
                               row->at(column_name)));
            } else {
                throw DbRelationError("table does not have column named ''" +
                                      column_name + "'");
            }
        }
    }
    delete row;
    return result;
}

// Validate the row before insert it
ValueDict *HeapTable::validate(const ValueDict *row) const {
    ValueDict *validated = new ValueDict();
    for (auto const &column_name : this->column_names) {
        if (row->find(column_name) == row->end()) {
            string m = "don't know how to handle NULLs, defaults, etc, yet";
            throw DbRelationError(m);
        } else {
            validated->insert(std::pair<Identifier, Value>(column_name,
                              row->at(column_name)));
        }
    }
    return validated;
}

// Appends a record to the file
Handle HeapTable::append(const ValueDict *row) {
    RecordID id;
    Dbt *data = marshal(row);
    BlockID last_block_id = this->file.get_last_block_id();
    SlottedPage *block = this->file.get(last_block_id);
    try {
        id = block->add(data);
    } catch(DbBlockNoRoomError& error) {
        // need a new block
        block = this->file.get_new();
        id = block->add(data);
    }
    this->file.put(block);
    Handle handle = std::make_pair(this->file.get_last_block_id(), id);
	  delete block;
	  free(data->get_data());
	  delete data;
    return handle;
}

// Return the bits to go into the file
Dbt *HeapTable::marshal(const ValueDict *row) const {
    char *bytes = new char[DbBlock::BLOCK_SZ];
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names){
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT) {
            if (offset + 4 > DbBlock::BLOCK_SZ - 4)
                throw DbRelationError("row too big to marshal");
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        } else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT) {
            uint size = value.s.length();
            if (size > UINT16_MAX)
                throw DbRelationError("text field too long to marshal");
            if (offset + 2 + size > DbBlock::BLOCK_SZ)
                throw DbRelationError("row too big to marshal");
            *(u16*) (bytes + offset) = size;
            offset += sizeof(u16);
            // Assume ascii
            memcpy(bytes + offset, value.s.c_str(), size);
            offset += size;
        } else {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

// Transform the bit data from a Dbt object into a ValueDict row
ValueDict *HeapTable::unmarshal(Dbt *data) const {
    ValueDict *row = new ValueDict();
    char *bytes = (char *)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names){
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT){
            int32_t n = *(int32_t *)(bytes + offset);
            row->insert(std::pair<Identifier, Value>(column_name, Value(n)));
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT){
            u16 size = *(u16 *)(bytes + offset);
            offset += sizeof(u16);
            char buffer[DbBlock::BLOCK_SZ];
            // Assume ascii for now
            memcpy(buffer, bytes + offset, size);
            buffer[size] = '\0';
            row->insert(std::pair<Identifier, Value>(column_name, Value(buffer)));
            offset += size;
        }
        else{
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    return row;
}

// See if the row at the given handle satisfies the given where clause
bool HeapTable::selected(Handle handle, const ValueDict* where) {
    if (where == nullptr)
        return true;
    ValueDict* row = this->project(handle, where);
	  return *row == *where;
}

void test_set_row(ValueDict &row, int a, string b) {
	  row["a"] = Value(a);
	  row["b"] = Value(b);
}

bool test_compare(DbRelation &table, Handle handle, int a, string b) {
	  ValueDict *result = table.project(handle);
	  Value value = (*result)["a"];
	  if (value.n != a) {
		    delete result;
		    return false;
	  }
	  value = (*result)["b"];
	  delete result;
	  return !(value.s != b);
}

// test function -- returns true if all tests pass
bool test_heap_storage() {
	  ColumnNames column_names;
	  column_names.push_back("a");
	  column_names.push_back("b");
	  ColumnAttributes column_attributes;
	  ColumnAttribute ca(ColumnAttribute::INT);
	  column_attributes.push_back(ca);
	  ca.set_data_type(ColumnAttribute::TEXT);
	  column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
	  cout << "test_heap_storage: " << endl;
    table1.create();
    cout << "create ok" << endl;
    // drop makes the object unusable because of BerkeleyDB restriction
    table1.drop();
    cout << "drop ok" << endl;
	  HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    cout << "create_if_not_exsts ok" << endl;
    ValueDict row;
	  string b = "alkjsl;kj; as;lkj;alskjf;laalsdfkjads;lfkj a;sldfkj a;sdlfjk a";
	  test_set_row(row, -1, b);
    table.insert(&row);
    cout << "insert ok" << endl;
    Handles* handles = table.select();
    if (!test_compare(table, (*handles)[0], -1, b))
        return false;
    cout << "select/project ok " << handles->size() << endl;
	  delete handles;
    Handle last_handle;
    for (int i = 0; i < 1000; i++) {
        test_set_row(row, i, b);
        last_handle = table.insert(&row);
    }
    handles = table.select();
    if (handles->size() != 1001)
        return false;
    int i = -1;
    for (auto const& handle: *handles)
        if (!test_compare(table, handle, i++, b))
            return false;
    cout << "many inserts/select/projects ok" << endl;
	  delete handles;
    table.del(last_handle);
    handles = table.select();
    if (handles->size() != 1000)
        return false;
    i = -1;
    for (auto const& handle: *handles)
        if (!test_compare(table, handle, i++, b))
            return false;
    cout << "del ok" << endl;
    table.drop();
	  delete handles;
    return true;
}
