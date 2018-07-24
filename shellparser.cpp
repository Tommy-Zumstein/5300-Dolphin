/**
* @CPSC5300 Milestone 1: Skeleton
*           Milestone 2: Rudimentary Storage Engine
*           Milestone 3: Schema Storage
* @File: shellparser.cpp - main entry for the relation manager's SQL shell
* @Group: Dolphin
* @Author: Sprint 1 team , Wonseok Seo, Kevin Cushing - advised from Kevin Lundeen
* @see "Seattle University, cpsc5300, Summer 2018"
*/

#include <stdio.h>
#include <stdlib.h>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "ParseTreeToString.h"
#include "SQLExec.h"
using namespace std;
using namespace hsql;

DbEnv* _DB_ENV;

// The Parser class itself
class DBParser{
public:
	/**
	* Parse TABLE REF INFO
	* @param	TableRef *table
	* @return	string
	*/
	string printTableRefInfo(TableRef *table){
		string tableref;
		switch (table->type){
		case kTableSelect:
			tableref += "kTableSelect TODO"; // TODO
			break;
		case kTableName:
			tableref += table->name;
			if (table->alias != NULL)
				tableref += string(" AS ") + table->alias;
			break;
		case kTableJoin:
			tableref += printTableRefInfo(table->join->left);
			switch (table->join->type){
			case kJoinCross:
			case kJoinInner:
				tableref += " JOIN ";
				break;
			case kJoinOuter:
			case kJoinLeftOuter:
			case kJoinLeft:
				tableref += " LEFT JOIN ";
				break;
			case kJoinRightOuter:
			case kJoinRight:
				tableref += " RIGHT JOIN ";
				break;
			case kJoinNatural:
				tableref += " NATURAL JOIN ";
				break;
			}
			tableref += printTableRefInfo(table->join->right);
			if (table->join->condition != NULL)
				tableref += " ON " + printExpression(table->join->condition);
			break;
		case kTableCrossProduct:
			int comma = 0;
			for (TableRef *tbl : *table->list){
				if (comma == 1)
					tableref += ", ";
				tableref += printTableRefInfo(tbl);
				comma = 1;
			}
			break;
		}
		return tableref;
	}

	/**
	* Parse OPERATOR Expression
	* @param	Expr *expr
	* @return	string
	*/
	string printOperatorExpression(Expr *expr){
		string operatorExpression;
		if (expr == nullptr){
			operatorExpression = "null";
		}
		// Left-hand side of expression
		operatorExpression += printExpression(expr->expr) + " ";
		switch (expr->opType){
		case Expr::SIMPLE_OP:
			operatorExpression += expr->opChar;
			break;
		case Expr::AND:
			operatorExpression = "AND";
			break;
		case Expr::OR:
			operatorExpression = "OR";
			break;
		case Expr::NOT:
			operatorExpression = "NOT";
			break;
		default:
			break;
		}
		// Right-hand side of expression
		if (expr->expr2 != nullptr){
			operatorExpression += " " + printExpression(expr->expr2);
		}
		return operatorExpression;
	}

	/**
	* Parse expression
	* @param	Expr *expr
	* @return	string
	*/
	string printExpression(Expr *expr){
		string expression;
		switch (expr->type){
		case kExprStar:
			expression += "*";
			break;
		case kExprColumnRef:
			if (expr->table){
				expression += string(expr->table) + ".";
			}
			expression += expr->name;
			break;
		case kExprLiteralFloat:
			expression += to_string(expr->fval);
			break;
		case kExprLiteralInt:
			expression += to_string(expr->ival);
			break;
		case kExprLiteralString:
			expression += expr->name;
			break;
		case kExprFunctionRef:
			expression += expr->name;
			for (Expr *e : *expr->exprList)
				expression += printExpression(e);
			break;
		case kExprOperator:
			expression += printOperatorExpression(expr);
			break;
		default:
			std::cerr << "Unrecognized expression type " << expr->type << std::endl;
			break;
		}
		if (expr->alias != nullptr){
			expression += "AS ";
			expression += expr->alias;
		}
		return expression;
	}

	/**
	* Parse the COLUMN DEFINITION
	* @param	const ColumnDefinition *col
	* @return	string
	*/
	string columnDefinitionToString(const ColumnDefinition *col){
		string columnDef(col->name);
		switch (col->type){
		case ColumnDefinition::DOUBLE:
			columnDef += " DOUBLE";
			break;
		case ColumnDefinition::INT:
			columnDef += " INT";
			break;
		case ColumnDefinition::TEXT:
			columnDef += " TEXT";
			break;
		default:
			columnDef += "Not Implemented";
			break;
		}
		return columnDef;
	}

	/**
	* Parse the CREATE statement
	* @param	Selectstatement *stmt
	* @return	string
	*/
	string executeCreateStatement(const CreateStatement *stmt){
		string statement = "CREATE TABLE ";
		statement += string(stmt->tableName) + " (";
		int comma = 0;
		for (ColumnDefinition *col : *stmt->columns){
			if (comma == 1)
				statement += ", ";
			statement += columnDefinitionToString(col);
			comma = 1;
		}
		statement += ")";
		return statement;
	}

	/**
	* Parse the SELECT statement
	* @param	Selectstatement *stmt
	* @return	string
	*/
	string executeSelectStatement(const SelectStatement *stmt){
		string statement = "SELECT ";
		int comma = 0;
		for (Expr *expr : *stmt->selectList){
			if (comma == 1)
				statement += ", ";
			statement += printExpression(expr);
			comma = 1;
		}
		if (stmt->fromTable != nullptr){
			statement += " FROM ";
			statement += printTableRefInfo(stmt->fromTable);
		}
		if (stmt->whereClause != nullptr){
			statement += " WHERE ";
			statement += printExpression(stmt->whereClause);
		}
		// Not required for Milestone1
		if (stmt->groupBy != nullptr){
			statement += " GROUP BY ";
			for (Expr *expr : *stmt->groupBy->columns)
				statement += printExpression(expr);
			if (stmt->groupBy->having != nullptr){
				statement += " HAVING ";
				statement += printExpression(stmt->groupBy->having);
			}
		}
		return statement;
	}

	/**
	* (temp) Parse an SQL statement
	* @param	stmt, Hyrise AST for the statement
	* @return	string, the parsed SQL
	*/
	string executeStatement(const SQLStatement *stmt){
		switch (stmt->type()){
		case kStmtSelect:
			return executeSelectStatement((const SelectStatement *)stmt);
		case kStmtCreate:
			return executeCreateStatement((const CreateStatement *)stmt);
		default:
			return "Not implemented";
		}
	}

	/**
	* (temp) receives an SQL statement
	* @param	SQLStatement, String with the input
	* @return	string, the parsed SQL
	*/
	string executeSQL(string SQLStatement){
		string output;
		SQLParserResult *result = SQLParser::parseSQLString(SQLStatement);
		if (result->isValid()){
			for (uint i = 0; i < result->size(); ++i){
				output += executeStatement(result->getStatement(0));
			}
		}
		else
			return "Invalid SQL : " + SQLStatement;
		return output;
	}
};

/**
* Main function, the entry point of the program
* @param	null
* @return	int
*/
int main(int argc, char *argv[]) {
    if (argc != 2) {
		    cerr << "Usage: cpsc5300: dbenvpath" << endl;
				return 1;
		}
	  char* envHome = argv[1];
		cout << "(sql5300: running with database environment at " << envHome << ")" << endl;
		DBParser dbParser;
		// Initialize dbenv
		DbEnv env(0U);
		env.set_message_stream(&cout);
		env.set_error_stream(&cerr);
		try {
				env.open(envHome, DB_CREATE | DB_INIT_MPOOL, 0);
		} catch (DbException &exc) {
				cerr << "(sql5300: " << exc.what() << ")" << endl;
				exit(1);
		}
		_DB_ENV = &env;
		initialize_schema_tables();
		//Db db(&env, 0);
		while(true){
				// Receive SQLstatement by shell
				string query;
				cout << "SQL> ";
				getline(cin, query);
				if (query == "quit")
						break;
				if (query == "test") {
						cout << "test_heap_storage: " << (test_heap_storage() ? "ok" : "failed") << endl;
						continue;
				}
				SQLPArserResult* parse = SQLParser::parseSQLString(query);
				if (!parse->isValie()) {
						cout << "invalid SQL: " << query << endl;
						cout << parse->errorMsg() << endl;
				} else {
						for (uint i = 0; i < parse->size(); i++) {
								const SQLStatement *statement = parse->getStatement(i);
								try {
										cout << dbParser.executeSQL(query) << endl;
										QueryResult *result - SQLExec::execute(statement);
										cout << *result << endl;
										delete result;
								} catch (SQLExecError& e) {
										cout << "Error: " << e.what() << endl;
								}
						}
				}
				delete parse;
		}
		return 0;
}
