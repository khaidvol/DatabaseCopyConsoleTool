package com.epam.dcct;

import java.net.URI;
import java.net.URISyntaxException;

public class Main {

    public static void main(String[] args) {

        //first argument = dbUrl, second argument = dbCopyName
        //args example->  mysql://localhost:3306/sndb sndbcopy

        String[] dbs = parseArgs(args[0], args[1]);

        CopyController copyController = new CopyController(dbs[0], dbs[1]);
        copyController.executeCopy();
    }


    public static String[] parseArgs(String dbUrl, String dbCopyName) {
        String[] dbs = new String[2];
        try{
            URI uri = new URI(dbUrl);
            dbs[0] = uri.getPath();
            dbs[1] = dbCopyName;

            if (dbs[0].isEmpty()) {
                System.out.println("URL incorrect: " + uri.toString());
                System.exit(0);
            } else if(dbs[0].startsWith("/")) {
                dbs[0] = dbs[0].substring(1);
            }

            if (!dbs[1].matches("[a-zA-Z0-9]*")) {
                System.out.println("Incorrect DB name for copy, only letters and digits allowed: " + dbs[1]);
                System.exit(0);
            } else if (dbs[1].equals(dbs[0])) {
                System.out.println("Databases names should be different. original db name: "
                        + dbs[0] + ", entered copy db name: " + dbs[1]);
                System.exit(0);
            }
        } catch (URISyntaxException e) {
            e.getMessage();
            System.exit(0);
        }
        return dbs;
    }
}
