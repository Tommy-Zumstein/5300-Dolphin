#include <cstring>
#include <sstream>

#include "heap_storage.h"
#include "storage_engine.h"

using namespace std;

typedef u_int16_t u16;
// CONSTRUCTOR 1: new empty block ready for new records to be added
SlottedPage::SlottedPage(Dbt &block, BlockID block_id, bool is_new) : DbBlock(block, block_id, is_new)
{
    if (is_new)
    {
        this->num_records = 0;
        this->end_free = DbBlock::BLOCK_SZ - 1;
        put_header();
    }
    else
    {
        get_header(this->num_records, this->end_free);
    }
}

// add(Dbt* data): adds a new record to the block, assumes that the record itself
// has been marshaled into the memory at data. Returns an id suitable for fetching
// it back later with get().
// Add a new record to the block. Return its id.
RecordID SlottedPage::add(const Dbt *data) throw(DbBlockNoRoomError)
{
    if (!has_room(data->get_size()))
        throw DbBlockNoRoomError("not enough room for new record");
    u16 id = ++this->num_records;
    u16 size = (u16)data->get_size();
    this->end_free -= size;
    u16 loc = this->end_free + 1;
    put_header();
    put_header(id, size, loc);
    memcpy(this->address(loc), data->get_data(), size);
    return id;
}

// new code
Dbt *SlottedPage::get(RecordID record_id) throw(DbBlockError)
{
    if (!have_record(record_id))
        throw DbBlockError("Record not found");

    u16 size = 0;
    u16 loc = 0;
    this->get_header(size, loc, record_id);

    return new Dbt(this->address(loc), size);
}

void SlottedPage::put(RecordID record_id, const Dbt &data) throw(DbBlockNoRoomError, DbBlockError)
{
    if (!have_record(record_id))
        throw DbBlockError("Record not found");

    u16 old_size;
    u16 old_loc;

    get_header(old_size, old_loc, record_id);

    u16 new_size = (u16)data.get_size();

    if (new_size > old_size && !has_room(new_size - old_size))
        throw DbBlockNoRoomError("Not enough room");

    if (new_size > old_size)
    {
        u16 shift_loc = new_size - old_size;
        u16 new_loc = old_loc - shift_loc;

        slide(record_id + 1, shift_loc);
        memcpy(this->address(new_loc), data.get_data(), new_size);
        put_header(record_id, new_size, new_loc);

        if (record_id == this->num_records)
        {
            this->end_free -= shift_loc;
        }
    }
    else
    {
        u16 shift_loc = old_size - new_size;
        u16 new_loc = old_loc + shift_loc;

        slide(record_id + 1, shift_loc, false);
        memcpy(this->address(new_loc), data.get_data(), new_size);
        put_header(record_id, new_size, new_loc);

        if (record_id == this->num_records)
        {
            this->end_free += shift_loc;
        }
    }

    put_header();
}

void SlottedPage::del(RecordID record_id)
{
    if (!have_record(record_id))
        throw DbBlockError("Record not found");

    u16 size, loc;
    get_header(size, loc, record_id);

    slide(record_id + 1, size, false);
    put_header(record_id, 0U, 0U);

    if (record_id == this->num_records)
    {
        this->end_free += size;
    }

    put_header();
}

RecordIDs *SlottedPage::ids(void)
{
    RecordIDs *record_ids = new RecordIDs();

    for (RecordID i = 1; i <= this->num_records; i++)
    {
        if (have_record(i))
        {
            record_ids->push_back(i);
        }
    }

    return record_ids;
}

bool SlottedPage::have_record(RecordID record_id)
{
    if (record_id == 0 || record_id > this->num_records)
        return false;

    u16 size;
    u16 loc;

    get_header(size, loc, record_id);

    if (loc == 0)
        return false;

    return true;
}

// new code
void SlottedPage::get_header(u16 &size, u16 &loc, RecordID id)
{
    size = get_n(4 * id);
    loc = get_n(4 * id + 2);
}

// Store the size and offset for given id. For id of zero, store the block header.
void SlottedPage::put_header(RecordID id, u16 size, u16 loc)
{
    if (id == 0)
    { // called the put_header() version and using the default params
        size = this->num_records;
        loc = this->end_free;
    }
    put_n(4 * id, size);
    put_n(4 * id + 2, loc);
}

bool SlottedPage::has_room(u16 size)
{
    return 4 * this->num_records + 3 < this->end_free - size + 1;
}

//undone
void SlottedPage::slide(RecordID start_record_id, u16 offset, bool left)
{
    while (start_record_id <= this->num_records && !have_record(start_record_id))
    {
        start_record_id++;
    }

    if (start_record_id > this->num_records)
    {
        return;
    }

    u16 begin_offset, begin_size;
    get_header(begin_size, begin_offset, start_record_id);

    u16 shift_block_size = begin_offset + begin_size - 1 - this->end_free;

    char temperary[shift_block_size];
    memcpy(temperary, this->address(this->end_free + 1), shift_block_size);

    if (left)
    {
        memcpy(this->address(this->end_free + 1 - offset), temperary, shift_block_size);
    }
    else
    {
        memcpy(this->address(this->end_free + 1 + offset), temperary, shift_block_size);
    }

    for (RecordID i = start_record_id; i <= this->num_records; i++)
    {
        if (have_record(i))
        {
            u16 temp_offset, temp_size;
            get_header(temp_size, temp_offset, i);

            if (left)
            {
                put_header(i, temp_size, temp_offset - offset);
            }
            else
            {
                put_header(i, temp_size, temp_offset + offset);
            }
        }
    }

    this->end_free = left ? this->end_free - offset : this->end_free + offset;

    put_header();
}

// Get 2-byte integer at given offset in block.
u16 SlottedPage::get_n(u16 offset)
{
    return *(u16 *)this->address(offset);
}

// Put a 2-byte integer at given offset in block.
void SlottedPage::put_n(u16 offset, u16 n)
{
    *(u16 *)this->address(offset) = n;
}

// Make a void* pointer for a given offset into the data block.
void *SlottedPage::address(u16 offset)
{
    return (void *)((char *)this->block.get_data() + offset);
}

void SlottedPage::ensure_record_exist(RecordID record_id) throw (DbBlockError)
{
	if (!have_record(record_id))
	{
		std::ostringstream string_stream;
		string_stream << "Record not found with id: " << record_id;
		
		throw DbBlockError(string_stream.str());
	}
}

/**
* @class HeapFile - heap file implementation of DbFile
*/

HeapFile::HeapFile(std::string name) : DbFile(name), last(0), db(_DB_ENV, 0)
{
    this->dbfilename = this->name + ".db";
    this->closed = true;
}

void HeapFile::create()
{
    db_open(DB_CREATE | DB_EXCL);
    SlottedPage *block = get_new(); // first block of the file
    put(block);
}

void HeapFile::drop()
{
    open();
    close();
    _DB_ENV->dbremove(NULL, this->dbfilename.c_str(), NULL , 0);
   // remove(this->dbfilename.c_str()); //remove the file
}

void HeapFile::open()
{
    db_open();
}

void HeapFile::close()
{
    if (!this->closed)
    {
        this->db.close(0);
        this->closed = true;
    }
}

// Allocate a new block for the database file.
// Returns the new empty DbBlock that is managing the records in this block and its block id.
SlottedPage *HeapFile::get_new()
{
    char block[DbBlock::BLOCK_SZ];
    std::memset(block, 0, sizeof(block));
    Dbt data(block, sizeof(block));
    int block_id = ++this->last;
    Dbt key(&block_id, sizeof(block_id));

    // write out an empty block and read it back in so Berkeley DB is managing the memory
    SlottedPage *page = new SlottedPage(data, this->last, true);
    this->db.put(nullptr, &key, &data, 0); // write it out with initialization applied
    this->db.get(nullptr, &key, &data, 0);
    return page;
}

SlottedPage *HeapFile::get(BlockID block_id)
{
    Dbt data;
    Dbt key(&block_id, sizeof(block_id));
    this->db.get(NULL, &key, &data, 0);
    SlottedPage *page = new SlottedPage(data, block_id, false);
    return page;
}

void HeapFile::put(DbBlock *block)
{
    BlockID id = block->get_block_id();
    Dbt key(&id, sizeof(id));
    this->db.put(nullptr, &key, block->get_block(), 0);
}

BlockIDs *HeapFile::block_ids()
{
    BlockIDs *ids = new BlockIDs();
    for (BlockID i = 1; i <= this->last; i++)
    {
        ids->push_back(i);
    }
    return ids;
}

void HeapFile::db_open(uint flags)
{
    if (!this->closed){
        return;
    }
    this->db.set_message_stream(_DB_ENV->get_message_stream());
    this->db.set_error_stream(_DB_ENV->get_error_stream());
    this->db.set_re_len(DbBlock::BLOCK_SZ);
    this->db.open(NULL, this->dbfilename.c_str(), NULL, DB_RECNO, flags, 0); 
    DB_BTREE_STAT *sp;
    this->db.stat(NULL, &sp, DB_FAST_STAT);
    this->last = sp->bt_ndata;
    this->closed = false;
}

/**
* @class HeapTable - Heap storage engine (implementation of DbRelation)
*/

HeapTable::HeapTable(Identifier table_name, ColumnNames column_names, ColumnAttributes column_attributes) : DbRelation(table_name, column_names, column_attributes),
                                                                                                            file(table_name)
{
}

void HeapTable::create()
{
    this->file.create();
}

void HeapTable::create_if_not_exists()
{
    try
    {
        this->file.open();
    }
    catch (DbException e)
    {
        this->file.create();
    }
}

void HeapTable::drop()
{
    this->file.drop();
}

void HeapTable::open()
{
    this->file.open();
}

void HeapTable::close()
{
    this->file.close();
}

Handle HeapTable::insert(const ValueDict *row)
{
    this->file.open();
    return append(validate(row));
}

// Not implemented
void HeapTable::update(const Handle handle, const ValueDict *new_values)
{
}
// Not implemented
void HeapTable::del(const Handle handle)
{
}

Handles *HeapTable::select()
{
    Handles *handles = new Handles();
    BlockIDs *block_ids = file.block_ids();
    for (auto const &block_id : *block_ids)
    {
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
//Not implemented
Handles *HeapTable::select(const ValueDict *where)
{
    Handles *handles = new Handles();
    return handles;
}

ValueDict *HeapTable::project(Handle handle)
{
    BlockID block_id = handle.first;
    RecordID record_id = handle.second;
    SlottedPage *block = this->file.get(block_id);
    Dbt *data = block->get(record_id);
    ValueDict *row = unmarshal(data);
    return row;
}

ValueDict *HeapTable::project(Handle handle, const ColumnNames *column_names)
{
    ValueDict *result = new ValueDict();
    ValueDict *row = project(handle);
    if (column_names == NULL)
    {
        return row;
    }
    else
    {
        for (auto const &column_name : *column_names)
        {
            if (row->find(column_name) != row->end())
                result->insert(std::pair<Identifier, Value>(column_name, row->at(column_name)));
        }
    }
    return result;
}

ValueDict *HeapTable::validate(const ValueDict *row)
{
    ValueDict *validated = new ValueDict();
    for (auto const &column_name : this->column_names)
    {
        if (row->find(column_name) == row->end())
        {
            return NULL;
        }
        else
        {
            validated->insert(std::pair<Identifier, Value>(column_name, row->at(column_name)));
        }
    }
    return validated;
}

Handle HeapTable::append(const ValueDict *row)
{
    Dbt *data = marshal(row);
    RecordID id;
    BlockID last_block_id = this->file.get_last_block_id();
    SlottedPage *block = this->file.get(last_block_id);
    try{
        id = block->add(data);
    }
    catch(DbBlockNoRoomError error){
        delete block;
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

// return the bits to go into the file
// caller responsible for freeing the returned Dbt and its enclosed ret->get_data().
Dbt *HeapTable::marshal(const ValueDict *row)
{
    char *bytes = new char[DbBlock::BLOCK_SZ]; // more than we need (we insist that one row fits into DbBlock::BLOCK_SZ)
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        ValueDict::const_iterator column = row->find(column_name);
        Value value = column->second;
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            *(int32_t *)(bytes + offset) = value.n;
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            uint size = value.s.length();
            *(u16 *)(bytes + offset) = size;
            offset += sizeof(u16);
            memcpy(bytes + offset, value.s.c_str(), size); // assume ascii for now
            offset += size;
        }
        else
        {
            throw DbRelationError("Only know how to marshal INT and TEXT");
        }
    }
    char *right_size_bytes = new char[offset];
    memcpy(right_size_bytes, bytes, offset);
    delete[] bytes;
    Dbt *data = new Dbt(right_size_bytes, offset);
    return data;
}

ValueDict *HeapTable::unmarshal(Dbt *data)
{
    ValueDict *row = new ValueDict();
    char *bytes = (char *)data->get_data();
    uint offset = 0;
    uint col_num = 0;
    for (auto const &column_name : this->column_names)
    {
        ColumnAttribute ca = this->column_attributes[col_num++];
        if (ca.get_data_type() == ColumnAttribute::DataType::INT)
        {
            int32_t n = *(int32_t *)(bytes + offset);
            row->insert(std::pair<Identifier, Value>(column_name, Value(n)));
            offset += sizeof(int32_t);
        }
        else if (ca.get_data_type() == ColumnAttribute::DataType::TEXT)
        {
            uint size = *(u16 *)(bytes + offset);
            offset += sizeof(u16);
            char *s = new char[size];
            memcpy(s, bytes + offset, size); // assume ascii for now
            row->insert(std::pair<Identifier, Value>(column_name, Value(s)));
            offset += size;
        }
        else{
            throw DbRelationError("Only know how to marshal INT and TEXT"); // Not implemented
        }
    }
    return row;
}

// test function -- returns true if all tests pass
bool test_heap_storage()
{
    ColumnNames column_names;
    column_names.push_back("a");
    column_names.push_back("b");
    ColumnAttributes column_attributes;
    ColumnAttribute ca(ColumnAttribute::INT);
    column_attributes.push_back(ca);
    ca.set_data_type(ColumnAttribute::TEXT);
    column_attributes.push_back(ca);
    HeapTable table1("_test_create_drop_cpp", column_names, column_attributes);
    table1.create();
    std::cout << "create ok" << std::endl;
    table1.drop(); // drop makes the object unusable because of BerkeleyDB restriction -- maybe want to fix this some day
    std::cout << "drop ok" << std::endl;

    HeapTable table("_test_data_cpp", column_names, column_attributes);
    table.create_if_not_exists();
    std::cout << "create_if_not_exsts ok" << std::endl;

    ValueDict row;
    row["a"] = Value(12);
    row["b"] = Value("Hello!");
    std::cout << "try insert" << std::endl;
    table.insert(&row);
    std::cout << "insert ok" << std::endl;
    Handles *handles = table.select();
    std::cout << "select ok " << handles->size() << std::endl;
    ValueDict *result = table.project((*handles)[0]);
    std::cout << "project ok" << std::endl;
    Value value = (*result)["a"];
    if (value.n != 12)
        return false;
    value = (*result)["b"];
    if (value.s != "Hello!")
        return false;
    table.drop();

    return true;
}