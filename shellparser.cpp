/**
* @CPSC5300 Milestone 1: Skeleton
*           Milestone 2: Data block
* @File: shellparser.cpp - main entry for the relation manager's SQL shell
* @Group: Dolphin
* @Author: Natalia Manriquez, Siyao Xu
*/

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include "db_cxx.h"
#include "SQLParser.h"
#include "sqlhelper.h"
using namespace std;
using namespace hsql;

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
int main(){
	const string HOME = "cpsc5300/data";
	DBParser dbParser;
	// Initialize dbenv
	string home = getenv("HOME");
	string envdir = string(home) + "/" + HOME;
	DbEnv env(0U);
	env.set_message_stream(&cout);
	env.set_error_stream(&cerr);
	env.open(envdir.c_str(), DB_CREATE | DB_INIT_MPOOL, 0);
	Db db(&env, 0);
	while(true){
		// Receive SQLstatement by shell
		string SQLStatement;
		cout << "SQL> ";
		getline(cin, SQLStatement);
		if (SQLStatement == "quit")
			break;
		// Print the parsed sql
		cout << dbParser.executeSQL(SQLStatement) << endl;
	}
	return 0;
}