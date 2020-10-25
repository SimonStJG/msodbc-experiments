#include <stdio.h>

#include <stdlib.h>

#include <sql.h>

#include <sqlext.h>

#include <msodbcsql.h>

//sudo ln -s /opt/microsoft/msodbcsql17/lib64/libmsodbcsql-17.6.so.1.1 /usr/lib/libmsodbcsql.so

void extract_error(
  char * fn,
  SQLHANDLE handle,
  SQLSMALLINT type) {
  SQLINTEGER i = 0;
  SQLINTEGER native;
  SQLCHAR state[7];
  SQLCHAR text[256];
  SQLSMALLINT len;
  SQLRETURN ret;

  fprintf(stderr,
    "\n"
    "The driver reported the following diagnostics whilst running "
    "%s\n\n",
    fn);

  do {
    ret = SQLGetDiagRec(type, handle, ++i, state, & native, text,
      sizeof(text), & len);
    if (SQL_SUCCEEDED(ret))
      printf("%s:%d:%d:%s\n", state, i, native, text);
  }
  while (ret == SQL_SUCCESS);
}

int main() {
  SQLHENV env;
  SQLHDBC dbc;
  SQLHSTMT stmt;
  char driver[256] = "ODBC Driver 17 for SQL Server";
  SQLRETURN ret;
  SQLCHAR outstr[1024];
  SQLSMALLINT outstrlen;
  SQLINTEGER col1 = 123;

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, & env))) {
    printf("Failed to SQLAllocHandle env\n");
    exit(1);
  }

  if (!SQL_SUCCEEDED(SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void * ) SQL_OV_ODBC3, 0))) {
    printf("Failed to SQLSetEnvAttr\n");
    exit(1);
  }

  if (!SQL_SUCCEEDED(SQLAllocHandle(SQL_HANDLE_DBC, env, & dbc))) {
    printf("Failed to SQLAllocHandle dbc \n");
    exit(1);
  }

  if (!SQL_SUCCEEDED(SQLSetConnectAttr(dbc, SQL_COPT_SS_BCP, (void * ) SQL_BCP_ON, SQL_IS_INTEGER))) {
    printf("Failed to SQLSetConnectAttr dbc (BCP) \n");
    exit(1);
  }

  ret = SQLDriverConnect(dbc, NULL, "DSN=MSSQLTest;UID=SA;PWD=PASSword1;", SQL_NTS,
    outstr, sizeof(outstr), & outstrlen,
    SQL_DRIVER_COMPLETE);

  if (!SQL_SUCCEEDED(ret)) {
    printf("Failed to SQLDriverConnect dbc \n");
    extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
    exit(1);
  }

  if (ret == SQL_SUCCESS_WITH_INFO) {
    printf("Driver reported the following diagnostics\n");
    extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
  }

  if ((bcp_init(dbc, "testtable", NULL, NULL, DB_IN)) == FAIL) {
    printf("bcp_init failed\n");
    extract_error("bcp_init", dbc, SQL_HANDLE_DBC);
  }

  if ((bcp_control(dbc, BCPMAXERRS, (void * ) 1)) == FAIL) {
    printf("Failed in bcp_control\n");
    exit(1);
  }

  SQLTCHAR hints[16] = "TABLOCK";
  if ((bcp_control(dbc, BCPHINTS, (void * ) hints)) == FAIL) {
    printf("Failed in bcp_control\n");
    exit(1);
  }

  if (bcp_bind(dbc, (LPCBYTE) & col1, 0, sizeof(SQLINTEGER), NULL, 0,
      SQLINT4, 1) == FAIL) {
    printf("Failed to bcp_bind \n");
    exit(1);
  }

  int c = 0;
  while (c < 1000000) {
    if (bcp_sendrow(dbc) == FAIL) {
      printf("Failed during bcp sendrow");
      exit(1);
    }
    c++;

    if ((c % 10000) == 0 && c) {
      bcp_batch(dbc);
    }
  }

  if (bcp_sendrow(dbc) == FAIL) {
    printf("Failed during bcp sendrow");
    exit(1);
  }

  if (bcp_done(dbc) == FAIL) {
    printf("Failed during bcp_done");
    exit(1);
  }

  if (!SQL_SUCCEEDED(SQLDisconnect(dbc))) {
    printf("Failed to SQLDisconnect dbc \n");
    exit(1);
  }

  if (!SQL_SUCCEEDED(SQLFreeHandle(SQL_HANDLE_DBC, dbc))) {
    printf("Failed to SQLFreeHandle dbc\n");
    extract_error("SQLFreeHandle", dbc, SQL_HANDLE_DBC);
    exit(1);
  }

  if (!SQL_SUCCEEDED(SQLFreeHandle(SQL_HANDLE_ENV, env))) {
    printf("Failed to SQLFreeHandle env\n");
    exit(1);
  }
}