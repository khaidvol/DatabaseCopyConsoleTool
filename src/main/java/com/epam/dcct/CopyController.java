package com.epam.dcct;

import java.util.List;

public class CopyController {

    private String dbOriginal;
    private String dbCopy;

    public CopyController(String dbOriginal, String dbCopy) {
        this.dbOriginal = dbOriginal;
        this.dbCopy = dbCopy;
    }

    public void executeCopy(){

        DatabaseOperator operator = new DatabaseOperator();

        //test connection to original DB
        if(!operator.testConnection(dbOriginal)){
            System.out.println("DATABASE COPY FAILED");
            System.exit(0);
        }

        //drop copy DB via original DB connection
        operator.dropCopyDatabaseIfExist(dbOriginal, dbCopy);

        //create copy DB via original DB connection
        operator.createCopyDatabase(dbOriginal, dbCopy);

        //create and copy tables
        List<String> tablesName = operator.getTables(dbOriginal);
        for(String tableName: tablesName) {
            String tableSQL = operator.getTableDefinitionSQL(dbOriginal, tableName);
            operator.createTable(dbCopy, tableSQL);
            operator.copyTable(dbCopy, dbOriginal, tableName);
        }
        System.out.println("DATABASE COPIED SUCCESSFULLY");
    }

}
