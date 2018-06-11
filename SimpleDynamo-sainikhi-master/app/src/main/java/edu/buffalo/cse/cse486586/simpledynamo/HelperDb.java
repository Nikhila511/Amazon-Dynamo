package edu.buffalo.cse.cse486586.simpledynamo;

import android.content.Context;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;

public class HelperDb extends SQLiteOpenHelper{
    private static final String SQL_CREATE_ENTRIES;
    private static final String SQL_DELETE_ENTRIES;

    static {
        SQL_CREATE_ENTRIES = "CREATE TABLE msgTable (key TEXT PRIMARY KEY, value TEXT)";
        SQL_DELETE_ENTRIES = "DROP TABLE IF EXISTS simpleDynamo";
    }
    public static final int DATABASE_VERSION = 1;
    public static final String DATABASE_NAME = "MsgReader.db";

    public HelperDb(Context context) {

        super(context, DATABASE_NAME, null, DATABASE_VERSION);
    }
    public void onCreate(SQLiteDatabase db) {

        db.execSQL(SQL_CREATE_ENTRIES);
    }
    public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        // This database is only a cache for online data, so its upgrade policy is
        // to simply to discard the data and start over
        db.execSQL(SQL_DELETE_ENTRIES);
        onCreate(db);
    }
    public void onDowngrade(SQLiteDatabase db, int oldVersion, int newVersion) {
        onUpgrade(db, oldVersion, newVersion);
    }
}