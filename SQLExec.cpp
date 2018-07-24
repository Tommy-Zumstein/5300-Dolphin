/**
 * @file SQLExec.cpp - implementation of SQLExec class
 * @author Wonseok Seo, Kevin Cushing - advised from Kevin Lundeen @SU
 * @see "Seattle University, CPSC5300, Summer 2018"
 */
#include "SQLExec.h"
using namespace std;
using namespace hsql;

// define static data
Tables* SQLExec::tables = nullptr;

// make query result be printable
ostream &operator<<(ostream &out, const QueryResult &qres) {
    if (qres.column_names != nullptr) {
        for (auto const &column_name: *qres.column_names)
            out << column_name << " ";
        out << endl << "+";
        for (unsigned int i = 0; i < qres.column_names->size(); i++)
            out << "----------+";
        out << endl;
        for (auto const &row: *qres.rows) {
            for (auto const &column_name: *qres.column_names) {
                Value value = row->at(column_name);
                switch (value.data_type) {
                    case ColumnAttribute::INT:
                        out << value.n;
                        break;
                    case ColumnAttribute::TEXT:
                        out << "\"" << value.s << "\"";
                        break;
                    default:
                        out << "???";
                }
                out << " ";
            }
            out << endl;
        }
    }
    out << qres.message;
    return out;
}

// checks pointer variables to prevent memory leak
QueryResult::~QueryResult() {
    if (column_names != nullptr)
        delete column_names;
    if (column_attributes != nullptr)
        delete column_attributes;
    if (rows != nullptr) {
        for (auto row: *rows)
            delete row;
        delete rows;
    }
}

/**
 *  Execute the given SQL DropStatement
 *  @param statement  the Hyrise AST of the SQL statement to exectue
 *  @returns          the query result (freed by caller)
 *  @throw            display what exception happend
 */
QueryResult *SQLExec::execute(const SQLStatement *statement)
                              throw(SQLExecError) {
    if (SQLExec::tables == nullptr)
        SQLExec::tables = new Tables();

    try {
        // for now, we only have three cases, create, drop, and show
        switch (statement->type()) {
            case kStmtCreate:
                return create((const CreateStatement *) statement);
            case kStmtDrop:
                return drop((const DropStatement *) statement);
            case kStmtShow:
                return show((const ShowStatement *) statement);
            default:
                return new QueryResult("not implemented");
        }
    } catch (DbRelationError& e) {
        throw SQLExecError(string("DbRelationError: ") + e.what());
    }
}

// Pull out column name and attrivutes from AST's column definition clause
void SQLExec::column_definition(const ColumnDefinition *col,
                                Identifier& column_name,
                                ColumnAttribute& column_attribute) {
    column_name = col->name;
    // for now, we only have two data types, INT and TEXT
    switch (col->type) {
        case ColumnDefinition::INT:
            column_attribute.set_data_type(ColumnAttribute::INT);
            break;
        case ColumnDefinition::TEXT:
            column_attribute.set_data_type(ColumnAttribute::TEXT);
            break;
        default:
            throw SQLExecError("not implemented");
    }
}

// Execute create SQL statement
QueryResult *SQLExec::create(const CreateStatement *statement) {
    // check statement type
    if (statement->type != CreateStatement::kTable)
        return new QueryResult("Only CREATE TABLE is implemented");
    // to hold table name
    Identifier table_name = statement->tableName;
    // to hold column names
    ColumnNames column_names;
    // to hold column attributes
    ColumnAttributes column_attributes;
    // to hold each column name
    Identifier column_name;
    // to hold each column attributes
    ColumnAttribute column_attribute;
    // insert column name and column attribute definitions into each vectors
    for (ColumnDefinition *columnDef : *statement->columns) {
        column_definition(columnDef, column_name, column_attribute);
        column_names.push_back(column_name);
        column_attributes.push_back(column_attribute);
    }

    // to hold new row of the table value in table schema
    ValueDict row;
    // insert pair values (key: "table_name", value: table_name)
    row["table_name"] = table_name;
    // insert the row in tables and create temporally handle for table for
    // deletion in case of exception
    Handle temp_table_handle = SQLExec::tables->insert(&row);
    try {
        // temporally Handles variable for deletion in case of exception
        Handles temp_columns_handles;
        // to hold columns schema
        DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);
        try {
            // insert each column name and type into columns schema
            for (uint i = 0; i < column_names.size(); i++) {
                row["column_name"] = column_names[i];
                if (column_attributes[i].get_data_type() == ColumnAttribute::INT) {
                    row["data_type"] = Value("INT");
                } else if (column_attributes[i].get_data_type() == ColumnAttribute::TEXT){
                    row["data_type"] = Value("TEXT");
                }
                // insert each handle to temporally column handles vector
                temp_columns_handles.push_back(columns.insert(&row));
            }
            // create actual relation
            DbRelation& table = SQLExec::tables->get_table(table_name);
            // validate ifNotExists statement query
            if (statement->ifNotExists)
                table.create_if_not_exists();
            else
                table.create();
        } catch (exception& e) {
            // delete all column values in column schema
            try {
                for (auto const &handle: temp_columns_handles)
                    columns.del(handle);
            } catch (...) {}
            throw;
        }
    } catch (exception& e) {
        // delete table value in table schema
        try {
            SQLExec::tables->del(temp_table_handle);
        } catch (...) {}
        throw;
    }
    return new QueryResult("created " + table_name);
}

// Execute drop SQL statement
QueryResult *SQLExec::drop(const DropStatement *statement) {
    // check statement type
    if (statement->type != DropStatement::kTable)
        throw SQLExecError("unrecognized DROP type");

    // to hold table name from statment
    Identifier table_name = statement->name;
    // validate attempt to drop schema tables
    if (table_name == Tables::TABLE_NAME || table_name == Columns::TABLE_NAME)
        throw SQLExecError("cannot drop a schema table");

    // to hold target location
    ValueDict target;
    target["table_name"] = Value(table_name);

    // get the table
    DbRelation& table = SQLExec::tables->get_table(table_name);

    // remove from columns schema
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    // temporally vector to hold each column handles
    Handles* handles = columns.select(&target);
    for (auto const& handle: *handles)
        columns.del(handle);

    // handle memory leak
    delete handles;

    // remove table
    table.drop();

    // remove from table schema
    SQLExec::tables->del(*SQLExec::tables->select(&target)->begin());
    return new QueryResult("dropped " + table_name);
}

// Execute show SQL statement
QueryResult *SQLExec::show(const ShowStatement *statement) {
    // For now, we only have two show types, show tables and show columns
    switch (statement->type) {
        case ShowStatement::kTables:
            return show_tables();
        case ShowStatement::kColumns:
            return show_columns(statement);
        default:
            throw SQLExecError("unrecognized SHOW type");
    }
}

// Exectue show_tables SQL statement
QueryResult *SQLExec::show_tables() {
    // to hold column name "table_name"
    ColumnNames* name_key = new ColumnNames;
    name_key->push_back("table_name");

    // to hold attribute TEXT
    ColumnAttributes* attribute_key = new ColumnAttributes;
    attribute_key->push_back(ColumnAttribute(ColumnAttribtue::TEXT));

    // to hold all tables handles
    Handles* handles = SQLExec::tables->select();

    // table schema and column schema tables are not counted (-2)
    u_long row_size = handles->size() - 2;

    // to hold all table names
    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = SQLExec::tables->project(handle, name_key);
        Identifier table_name = row->at("table_name").s;
        // validation to exclude schema tables
        if (table_name != Tables::TABLE_NAME &&
            table_name != Columns::TABLE_NAME)
            rows->push_back(row);
    }

    // handle memory leak
    delete handles;
    return new QueryResult(name_key, attribute_key, rows,
                           "successfully returned " + to_string(row_size) +
                           " rows");
}

// Exectue show_columns SQL statement
QueryResult *SQLExec::show_columns(const ShowStatement *statement) {
    // to hold column schema table
    DbRelation& columns = SQLExec::tables->get_table(Columns::TABLE_NAME);

    // to hold column names for schema table, table_name, column_name, data_type
    ColumnNames* name_keys = new ColumnNames;
    name_keys->push_back("table_name");
    name_keys->push_back("column_name");
    name_keys->push_back("data_type");

    // to hold attribute TEXT
    ColumnAttributes* attribute_key = new ColumnAttributes;
    attribute_key->push_back(ColumnAttribute(ColumnAttribute::TEXT));

    // to hold target location
    ValueDict target;
    target["table_name"] = Value(statement->tableName);

    // to hold handles for all columns
    Handles* handles = columns.select(&target);

    // the number of columns
    u_long row_size = handles->size();

    // to hold all columns
    ValueDicts* rows = new ValueDicts;
    for (auto const& handle: *handles) {
        ValueDict* row = columns.project(handle, name_keys);
        rows->push_back(row);
    }

    // handle memory leak
    delete handles;
	  return new QueryResult(name_keys, attribute_key, rows,
                           "successfully returned " + to_string(row_size) +
                           " rows");
}
